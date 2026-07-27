package s3ext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"net/url"
	"os"
	"strings"
	"sync"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// DownloadReferenceCapability returns a stable configured public reference for
// one exact current object. It intentionally does not fall back to a generic
// presigner: applications that need a public URL must configure a provider
// capable of serving that stable reference.
const DownloadReferenceCapability = effect.StorageExactObjectDownloadReferenceCapability

const (
	downloadReferenceExport = effect.StorageExactObjectDownloadReferenceImport

	downloadReferenceCodeOK          uint32 = 0
	downloadReferenceCodeEmpty       uint32 = 1
	downloadReferenceCodeMemory      uint32 = 2
	downloadReferenceCodeDecode      uint32 = 3
	downloadReferenceCodeInvalid     uint32 = 4
	downloadReferenceCodeProvider    uint32 = 5
	downloadReferenceCodeNotFound    uint32 = 6
	downloadReferenceCodeGeneration  uint32 = 7
	downloadReferenceCodeEncode      uint32 = 8
	downloadReferenceCodeAllocate    uint32 = 9
	downloadReferenceCodeWrite       uint32 = 10
	downloadReferenceCodeUnavailable uint32 = 11
	downloadReferenceCodeStub        uint32 = 99
)

type downloadReferenceState struct {
	client     *s3ClientState
	headObject func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
}

type downloadReferenceManager struct {
	mu     sync.Mutex
	states map[s3ApplicationKey]*downloadReferenceState
}

func newDownloadReferenceManager() *downloadReferenceManager {
	return &downloadReferenceManager{states: map[s3ApplicationKey]*downloadReferenceState{}}
}

func (m *downloadReferenceManager) setup(scope ext.Scope) (*downloadReferenceState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.states[key]; state != nil {
		return state, nil
	}
	state := &downloadReferenceState{client: &s3ClientState{}}
	m.states[key] = state
	return state, nil
}

func (m *downloadReferenceManager) forScope(scope ext.Scope) (*downloadReferenceState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[key]
	if state == nil {
		return nil, fmt.Errorf("storage download reference: setup not called for application %s/%s", key.applicationID, key.instanceID)
	}
	return state, nil
}

func (m *downloadReferenceManager) teardown(scope ext.Scope) error {
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

var downloadReferences = newDownloadReferenceManager()

func init() {
	ext.Register(newDownloadReferenceCapability(downloadReferences))
}

func newDownloadReferenceCapability(manager *downloadReferenceManager) ext.Capability {
	return ext.Capability{
		Name:     DownloadReferenceCapability,
		Provider: "github.com/BananaLabs-OSS/Pulp-ext-s3",
		Setup: func(env ext.SetupEnv) error {
			_, err := manager.setup(env.EffectiveScope())
			return err
		},
		Register: func(builder wazero.HostModuleBuilder, cell ext.Cell) error {
			scope, err := ext.ValidatedScopeOf(cell)
			if err != nil {
				return fmt.Errorf("storage download reference: resolve cell scope: %w", err)
			}
			state, err := manager.forScope(scope)
			if err != nil {
				return err
			}
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return downloadReferenceHost(ctx, module, state, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(downloadReferenceExport)
			return nil
		},
		Stub: func(builder wazero.HostModuleBuilder, _ ext.Cell) error {
			builder.NewFunctionBuilder().WithFunc(func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 {
				return downloadReferenceCodeStub
			}).Export(downloadReferenceExport)
			return nil
		},
		TeardownScope: func(_ context.Context, scope ext.Scope) error { return manager.teardown(scope) },
	}
}

func downloadReferenceHost(ctx context.Context, module api.Module, state *downloadReferenceState, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	if requestLen == 0 {
		return downloadReferenceCodeEmpty
	}
	if module == nil || module.Memory() == nil {
		return downloadReferenceCodeMemory
	}
	wire, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return downloadReferenceCodeMemory
	}
	result, code := downloadReferenceWire(ctx, state, wire)
	if code != downloadReferenceCodeOK {
		return code
	}
	return writeDownloadReferenceResponse(ctx, module, result, responsePtrPtr, responseLenPtr)
}

func downloadReferenceWire(ctx context.Context, state *downloadReferenceState, wire []byte) ([]byte, uint32) {
	var command effect.StorageExactObjectDownloadReferenceCommand
	decoder := msgpack.NewDecoder(bytes.NewReader(wire))
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil {
		return nil, downloadReferenceCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, downloadReferenceCodeInvalid
	}
	result, err := state.downloadReference(ctx, command)
	if err != nil {
		return nil, downloadReferenceErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, downloadReferenceCodeEncode
	}
	return encoded, downloadReferenceCodeOK
}

func (s *downloadReferenceState) downloadReference(ctx context.Context, command effect.StorageExactObjectDownloadReferenceCommand) (effect.StorageExactObjectDownloadReferenceResult, error) {
	if s == nil || s.client == nil {
		return effect.StorageExactObjectDownloadReferenceResult{}, errors.New("storage download reference: provider unavailable")
	}
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageExactObjectDownloadReferenceResult{}, fmt.Errorf("storage download reference: client: %w", err)
	}
	head := s.headObject
	if head == nil {
		head = func(ctx context.Context, state *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return state.client.HeadObject(ctx, input)
		}
	}
	object, err := head(ctx, s.client, &s3.HeadObjectInput{Bucket: &s.client.bucket, Key: &command.ObjectKey, IfMatch: quotedETag(command.ExpectedETag)})
	if err != nil {
		return effect.StorageExactObjectDownloadReferenceResult{}, archiveObjectError("head", command.ObjectKey, err)
	}
	if object == nil || normalizeETag(object.ETag) != normalizeETagValue(command.ExpectedETag) {
		return effect.StorageExactObjectDownloadReferenceResult{}, ErrObjectGenerationMismatch
	}
	publicURL, err := configuredStablePublicURL(command.ObjectKey)
	if err != nil {
		return effect.StorageExactObjectDownloadReferenceResult{}, err
	}
	return effect.StorageExactObjectDownloadReferenceResult{
		ContractVersion: command.ContractVersion, ObjectKey: command.ObjectKey, ExpectedETag: command.ExpectedETag,
		PublicURL: publicURL, ExpiresAtUnix: command.ExpiresAtUnix,
	}, nil
}

// configuredStablePublicURL reads only host configuration. S3_PUBLIC_BASE_URL
// identifies a provider's stable public origin/path and never comes from guest
// input. A missing or non-canonical value fails closed rather than issuing an
// unconstrained provider URL.
func configuredStablePublicURL(objectKey string) (string, error) {
	configured := os.Getenv("S3_PUBLIC_BASE_URL")
	if configured == "" || strings.TrimSpace(configured) != configured {
		return "", errors.New("storage download reference: stable public URL is not configured")
	}
	base, err := url.Parse(configured)
	if err != nil || base.Scheme != "https" || base.Host == "" || base.User != nil || base.RawQuery != "" || base.Fragment != "" {
		return "", errors.New("storage download reference: stable public URL configuration is invalid")
	}
	escapedParts := make([]string, 0, strings.Count(objectKey, "/")+1)
	parts := strings.Split(objectKey, "/")
	for _, part := range parts {
		if part == "" || part == "." || part == ".." {
			return "", errors.New("storage download reference: exact object key is invalid")
		}
		escapedParts = append(escapedParts, url.PathEscape(part))
	}
	base.Path = strings.TrimRight(base.Path, "/") + "/" + strings.Join(parts, "/")
	base.RawPath = strings.TrimRight(base.EscapedPath(), "/") + "/" + strings.Join(escapedParts, "/")
	value := base.String()
	if len(value) == 0 || len(value) > 8192 {
		return "", errors.New("storage download reference: stable public URL is invalid")
	}
	return value, nil
}

func writeDownloadReferenceResponse(ctx context.Context, module api.Module, value []byte, responsePtrPtr, responseLenPtr uint32) uint32 {
	if module == nil || module.Memory() == nil {
		return downloadReferenceCodeMemory
	}
	alloc := module.ExportedFunction("pulp_alloc")
	if alloc == nil {
		return downloadReferenceCodeAllocate
	}
	allocated, err := alloc.Call(ctx, uint64(len(value)))
	if err != nil || len(allocated) == 0 || allocated[0] == 0 {
		return downloadReferenceCodeAllocate
	}
	ptr := uint32(allocated[0])
	if !module.Memory().Write(ptr, value) || !module.Memory().WriteUint32Le(responsePtrPtr, ptr) || !module.Memory().WriteUint32Le(responseLenPtr, uint32(len(value))) {
		return downloadReferenceCodeWrite
	}
	return downloadReferenceCodeOK
}

func downloadReferenceErrorCode(err error) uint32 {
	if errors.Is(err, ErrObjectNotFound) {
		return downloadReferenceCodeNotFound
	}
	if errors.Is(err, ErrObjectGenerationMismatch) {
		return downloadReferenceCodeGeneration
	}
	if strings.Contains(err.Error(), "not configured") || strings.Contains(err.Error(), "configuration is invalid") || strings.Contains(err.Error(), "client:") || strings.Contains(err.Error(), "provider unavailable") {
		return downloadReferenceCodeUnavailable
	}
	return downloadReferenceCodeProvider
}
