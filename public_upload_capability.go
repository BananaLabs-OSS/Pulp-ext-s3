package s3ext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// PublicUploadCapability is a capability separate from storage.s3. It grants
// only two exact-object operations: issue a constrained PUT URL and validate
// that same object through HEAD. There is intentionally no list, delete,
// prefix, bucket, or raw-client operation on this surface.
const PublicUploadCapability = effect.StoragePublicUploadCapability

const (
	publicUploadPresignExport  = effect.StorageExactObjectPresignPutImport
	publicUploadValidateExport = effect.StorageExactObjectValidatePutImport
	publicUploadDeleteExport   = effect.StorageExactObjectDeleteImport

	publicUploadCodeOK          uint32 = 0
	publicUploadCodeEmpty       uint32 = 1
	publicUploadCodeMemory      uint32 = 2
	publicUploadCodeDecode      uint32 = 3
	publicUploadCodeInvalid     uint32 = 4
	publicUploadCodeProvider    uint32 = 5
	publicUploadCodeNotFound    uint32 = 6
	publicUploadCodeEncode      uint32 = 7
	publicUploadCodeAllocate    uint32 = 8
	publicUploadCodeWrite       uint32 = 9
	publicUploadCodeUnavailable uint32 = 10
	publicUploadCodeGeneration  uint32 = 11
	publicUploadCodeStub        uint32 = 99
)

type publicUploadPlan struct {
	exactKey           string
	expectedGeneration string
	contentLength      int64
	contentType        string
}

// publicUploadState is application-instance owned. The captured scope is
// never supplied by guest bytes, so one application cannot sign or validate
// an object through another application's configured client.
type publicUploadState struct {
	client       *s3ClientState
	mu           sync.Mutex
	plans        map[string]publicUploadPlan
	presignPut   func(context.Context, *s3ClientState, *s3.PutObjectInput, time.Duration) (string, error)
	headObject   func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	deleteObject func(context.Context, *s3ClientState, *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
}

type publicUploadManager struct {
	mu     sync.Mutex
	states map[s3ApplicationKey]*publicUploadState
}

func newPublicUploadManager() *publicUploadManager {
	return &publicUploadManager{states: map[s3ApplicationKey]*publicUploadState{}}
}

func (m *publicUploadManager) setup(scope ext.Scope) (*publicUploadState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.states[key]; state != nil {
		return state, nil
	}
	state := &publicUploadState{client: &s3ClientState{}, plans: map[string]publicUploadPlan{}}
	m.states[key] = state
	return state, nil
}

func (m *publicUploadManager) forScope(scope ext.Scope) (*publicUploadState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[key]
	if state == nil {
		return nil, fmt.Errorf("storage public upload: setup not called for application %s/%s", key.applicationID, key.instanceID)
	}
	return state, nil
}

func (m *publicUploadManager) teardown(scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	state := m.states[key]
	delete(m.states, key)
	m.mu.Unlock()
	if state != nil {
		state.client.close()
	}
	return nil
}

var publicUploads = newPublicUploadManager()

func init() {
	ext.Register(newPublicUploadCapability(publicUploads, time.Now))
}

func newPublicUploadCapability(manager *publicUploadManager, now func() time.Time) ext.Capability {
	if now == nil {
		now = time.Now
	}
	return ext.Capability{
		Name:     PublicUploadCapability,
		Provider: "github.com/BananaLabs-OSS/Pulp-ext-s3",
		Setup: func(env ext.SetupEnv) error {
			_, err := manager.setup(env.EffectiveScope())
			return err
		},
		Register: func(builder wazero.HostModuleBuilder, cell ext.Cell) error {
			scope, err := ext.ValidatedScopeOf(cell)
			if err != nil {
				return fmt.Errorf("storage public upload: resolve cell scope: %w", err)
			}
			state, err := manager.forScope(scope)
			if err != nil {
				return err
			}
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return publicUploadPresignHost(ctx, module, state, now, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(publicUploadPresignExport)
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return publicUploadValidateHost(ctx, module, state, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(publicUploadValidateExport)
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return publicUploadDeleteHost(ctx, module, state, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(publicUploadDeleteExport)
			return nil
		},
		Stub: func(builder wazero.HostModuleBuilder, _ ext.Cell) error {
			stub := func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 { return publicUploadCodeStub }
			builder.NewFunctionBuilder().WithFunc(stub).Export(publicUploadPresignExport)
			builder.NewFunctionBuilder().WithFunc(stub).Export(publicUploadValidateExport)
			builder.NewFunctionBuilder().WithFunc(stub).Export(publicUploadDeleteExport)
			return nil
		},
		TeardownScope: func(_ context.Context, scope ext.Scope) error { return manager.teardown(scope) },
	}
}

// publicUploadPresignHost and publicUploadValidateHost keep host-memory
// plumbing separate from the typed operation. The typed functions are also
// directly testable without a wasm runtime or network.
func publicUploadPresignHost(ctx context.Context, module api.Module, state *publicUploadState, now func() time.Time, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	request, code := readPublicUploadRequest(module, requestPtr, requestLen)
	if code != publicUploadCodeOK {
		return code
	}
	result, code := publicUploadPresignWire(ctx, state, now, request)
	if code != publicUploadCodeOK {
		return code
	}
	return writePublicUploadResponse(ctx, module, result, responsePtrPtr, responseLenPtr)
}

func publicUploadValidateHost(ctx context.Context, module api.Module, state *publicUploadState, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	request, code := readPublicUploadRequest(module, requestPtr, requestLen)
	if code != publicUploadCodeOK {
		return code
	}
	result, code := publicUploadValidateWire(ctx, state, request)
	if code != publicUploadCodeOK {
		return code
	}
	return writePublicUploadResponse(ctx, module, result, responsePtrPtr, responseLenPtr)
}

func publicUploadDeleteHost(ctx context.Context, module api.Module, state *publicUploadState, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	request, code := readPublicUploadRequest(module, requestPtr, requestLen)
	if code != publicUploadCodeOK {
		return code
	}
	result, code := publicUploadDeleteWire(ctx, state, request)
	if code != publicUploadCodeOK {
		return code
	}
	return writePublicUploadResponse(ctx, module, result, responsePtrPtr, responseLenPtr)
}

func readPublicUploadRequest(module api.Module, requestPtr, requestLen uint32) ([]byte, uint32) {
	if requestLen == 0 {
		return nil, publicUploadCodeEmpty
	}
	if module == nil || module.Memory() == nil {
		return nil, publicUploadCodeMemory
	}
	request, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return nil, publicUploadCodeMemory
	}
	return request, publicUploadCodeOK
}

func publicUploadPresignWire(ctx context.Context, state *publicUploadState, now func() time.Time, wire []byte) ([]byte, uint32) {
	command, err := decodePublicUploadPresignCommand(wire)
	if err != nil {
		return nil, publicUploadCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, publicUploadCodeInvalid
	}
	result, err := state.presign(ctx, now, command)
	if err != nil {
		return nil, publicUploadErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, publicUploadCodeEncode
	}
	return encoded, publicUploadCodeOK
}

func publicUploadValidateWire(ctx context.Context, state *publicUploadState, wire []byte) ([]byte, uint32) {
	command, err := decodePublicUploadValidateCommand(wire)
	if err != nil {
		return nil, publicUploadCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, publicUploadCodeInvalid
	}
	result, err := state.validate(ctx, command)
	if err != nil {
		return nil, publicUploadErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, publicUploadCodeEncode
	}
	return encoded, publicUploadCodeOK
}

func publicUploadDeleteWire(ctx context.Context, state *publicUploadState, wire []byte) ([]byte, uint32) {
	var command effect.StorageExactObjectDeleteCommand
	decoder := msgpack.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil {
		return nil, publicUploadCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, publicUploadCodeInvalid
	}
	result, err := state.delete(ctx, command)
	if err != nil {
		return nil, publicUploadErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, publicUploadCodeEncode
	}
	return encoded, publicUploadCodeOK
}

func (s *publicUploadState) presign(ctx context.Context, now func() time.Time, command effect.StorageExactObjectPresignPutCommand) (effect.StorageExactObjectPresignPutResult, error) {
	if s == nil || s.client == nil {
		return effect.StorageExactObjectPresignPutResult{}, errors.New("storage public upload: provider unavailable")
	}
	plan := publicUploadPlan{exactKey: command.ExactKey, expectedGeneration: command.ExpectedGeneration, contentLength: command.ContentLength, contentType: command.ContentType}
	s.mu.Lock()
	if existing, ok := s.plans[command.UploadID]; ok && existing != plan {
		s.mu.Unlock()
		return effect.StorageExactObjectPresignPutResult{}, errors.New("storage public upload: upload id replay has different exact-object command")
	}
	s.plans[command.UploadID] = plan
	s.mu.Unlock()
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageExactObjectPresignPutResult{}, fmt.Errorf("storage public upload: client: %w", err)
	}
	input := &s3.PutObjectInput{
		Bucket: &s.client.bucket, Key: &command.ExactKey,
		ContentLength: &command.ContentLength, ContentType: &command.ContentType,
		Metadata: map[string]string{"pulp-upload-id": command.UploadID},
	}
	applyWriteFence(command.ExpectedGeneration, &input.IfMatch, &input.IfNoneMatch)
	presign := s.presignPut
	if presign == nil {
		presign = func(ctx context.Context, state *s3ClientState, input *s3.PutObjectInput, ttl time.Duration) (string, error) {
			out, err := state.presig.PresignPutObject(ctx, input, s3.WithPresignExpires(ttl))
			if err != nil {
				return "", err
			}
			return out.URL, nil
		}
	}
	presigned, err := presign(ctx, s.client, input, time.Duration(command.TTLSec)*time.Second)
	if err != nil {
		return effect.StorageExactObjectPresignPutResult{}, fmt.Errorf("storage public upload: presign %q: %w", command.ExactKey, err)
	}
	if err := validatePublicUploadURL(presigned); err != nil {
		return effect.StorageExactObjectPresignPutResult{}, err
	}
	return effect.StorageExactObjectPresignPutResult{
		ExactKey: command.ExactKey, UploadID: command.UploadID, ExpectedGeneration: command.ExpectedGeneration,
		ContentLength: command.ContentLength, ContentType: command.ContentType,
		URL: presigned, ExpiresAtUnix: now().Add(time.Duration(command.TTLSec) * time.Second).Unix(),
	}, nil
}

func (s *publicUploadState) validate(ctx context.Context, command effect.StorageExactObjectValidatePutCommand) (effect.StorageExactObjectValidatePutResult, error) {
	if s == nil || s.client == nil {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: provider unavailable")
	}
	s.mu.Lock()
	plan, ok := s.plans[command.UploadID]
	s.mu.Unlock()
	if !ok || plan.exactKey != command.ExactKey || plan.contentLength != command.ContentLength || plan.contentType != command.ContentType {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: upload validation is not bound to a presign command")
	}
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageExactObjectValidatePutResult{}, fmt.Errorf("storage public upload: client: %w", err)
	}
	head := s.headObject
	if head == nil {
		head = func(ctx context.Context, state *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return state.client.HeadObject(ctx, input)
		}
	}
	out, err := head(ctx, s.client, &s3.HeadObjectInput{Bucket: &s.client.bucket, Key: &command.ExactKey})
	if err != nil {
		return effect.StorageExactObjectValidatePutResult{}, fmt.Errorf("storage public upload: head %q: %w", command.ExactKey, err)
	}
	if out == nil {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: head returned no result")
	}
	if out.ContentLength == nil || *out.ContentLength != command.ContentLength {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: uploaded object content length does not match signed policy")
	}
	if out.ContentType == nil || *out.ContentType != command.ContentType {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: uploaded object content type does not match signed policy")
	}
	if !metadataEquals(out.Metadata, "pulp-upload-id", command.UploadID) {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: uploaded object is not bound to the signed upload id")
	}
	generation := normalizeETag(out.ETag)
	if generation == "" {
		return effect.StorageExactObjectValidatePutResult{}, errors.New("storage public upload: uploaded object has no generation")
	}
	return effect.StorageExactObjectValidatePutResult{
		ExactKey: command.ExactKey, UploadID: command.UploadID, Generation: generation,
		ContentLength: command.ContentLength, ContentType: command.ContentType,
	}, nil
}

func (s *publicUploadState) delete(ctx context.Context, command effect.StorageExactObjectDeleteCommand) (effect.StorageExactObjectDeleteResult, error) {
	if s == nil || s.client == nil {
		return effect.StorageExactObjectDeleteResult{}, errors.New("storage public upload: provider unavailable")
	}
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageExactObjectDeleteResult{}, fmt.Errorf("storage public upload: client: %w", err)
	}
	head := s.headObject
	if head == nil {
		head = func(ctx context.Context, state *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return state.client.HeadObject(ctx, input)
		}
	}
	current, err := head(ctx, s.client, &s3.HeadObjectInput{Bucket: &s.client.bucket, Key: &command.ExactKey})
	if err != nil {
		if errors.Is(archiveObjectError("head", command.ExactKey, err), ErrObjectNotFound) {
			return effect.StorageExactObjectDeleteResult{ExactKey: command.ExactKey, ExpectedGeneration: command.ExpectedGeneration}, nil
		}
		return effect.StorageExactObjectDeleteResult{}, archiveObjectError("head", command.ExactKey, err)
	}
	if current == nil || normalizeETag(current.ETag) != normalizeETagValue(command.ExpectedGeneration) {
		return effect.StorageExactObjectDeleteResult{}, ErrObjectGenerationMismatch
	}
	deleteObject := s.deleteObject
	if deleteObject == nil {
		deleteObject = func(ctx context.Context, state *s3ClientState, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			return state.client.DeleteObject(ctx, input)
		}
	}
	if _, err := deleteObject(ctx, s.client, &s3.DeleteObjectInput{Bucket: &s.client.bucket, Key: &command.ExactKey, IfMatch: quotedETag(command.ExpectedGeneration)}); err != nil {
		mapped := archiveObjectError("delete", command.ExactKey, err)
		if errors.Is(mapped, ErrObjectNotFound) {
			return effect.StorageExactObjectDeleteResult{ExactKey: command.ExactKey, ExpectedGeneration: command.ExpectedGeneration}, nil
		}
		return effect.StorageExactObjectDeleteResult{}, mapped
	}
	return effect.StorageExactObjectDeleteResult{ExactKey: command.ExactKey, ExpectedGeneration: command.ExpectedGeneration}, nil
}

func decodePublicUploadPresignCommand(wire []byte) (effect.StorageExactObjectPresignPutCommand, error) {
	var command effect.StorageExactObjectPresignPutCommand
	decoder := msgpack.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil {
		return command, err
	}
	return command, nil
}

func decodePublicUploadValidateCommand(wire []byte) (effect.StorageExactObjectValidatePutCommand, error) {
	var command effect.StorageExactObjectValidatePutCommand
	decoder := msgpack.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil {
		return command, err
	}
	return command, nil
}

func writePublicUploadResponse(ctx context.Context, module api.Module, value []byte, responsePtrPtr, responseLenPtr uint32) uint32 {
	if module == nil || module.Memory() == nil {
		return publicUploadCodeMemory
	}
	alloc := module.ExportedFunction("pulp_alloc")
	if alloc == nil {
		return publicUploadCodeAllocate
	}
	result, err := alloc.Call(ctx, uint64(len(value)))
	if err != nil || len(result) == 0 || result[0] == 0 {
		return publicUploadCodeAllocate
	}
	ptr := uint32(result[0])
	if !module.Memory().Write(ptr, value) || !module.Memory().WriteUint32Le(responsePtrPtr, ptr) || !module.Memory().WriteUint32Le(responseLenPtr, uint32(len(value))) {
		return publicUploadCodeWrite
	}
	return publicUploadCodeOK
}

func publicUploadErrorCode(err error) uint32 {
	if err == nil {
		return publicUploadCodeOK
	}
	if errors.Is(err, ErrObjectNotFound) || classifyS3Error(err) == scopedObjectStoreNotFoundCode {
		return publicUploadCodeNotFound
	}
	if errors.Is(err, ErrObjectGenerationMismatch) {
		return publicUploadCodeGeneration
	}
	if strings.Contains(err.Error(), "client:") || strings.Contains(err.Error(), "provider unavailable") {
		return publicUploadCodeUnavailable
	}
	return publicUploadCodeProvider
}

func validatePublicUploadURL(value string) error {
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return errors.New("storage public upload: presigner returned an invalid URL")
	}
	return nil
}

func metadataEquals(values map[string]string, key, want string) bool {
	for candidate, value := range values {
		if strings.EqualFold(candidate, key) && value == want {
			return true
		}
	}
	return false
}
