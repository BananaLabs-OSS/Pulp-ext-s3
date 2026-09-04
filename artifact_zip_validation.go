package s3ext

// This file implements the intentionally narrow artifact-validation host
// boundary. It reads one owner-derived S3 object under an exact ETag fence and
// validates ZIP metadata in memory. It never extracts an archive to disk and
// never accepts an endpoint, bucket, path, header, or provider from a guest.

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"path"
	"regexp"
	"strings"
	"sync"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

const ArtifactValidationCapability = effect.StorageArtifactValidationCapability

const (
	artifactValidationExport = effect.StorageExactObjectArtifactZIPValidateImport

	artifactValidationCodeOK                 uint32 = 0
	artifactValidationCodeEmpty              uint32 = 1
	artifactValidationCodeMemory             uint32 = 2
	artifactValidationCodeDecode             uint32 = 3
	artifactValidationCodeInvalid            uint32 = 4
	artifactValidationCodeProvider           uint32 = 5
	artifactValidationCodeNotFound           uint32 = 6
	artifactValidationCodeGenerationMismatch uint32 = 7
	artifactValidationCodeEncode             uint32 = 8
	artifactValidationCodeAllocate           uint32 = 9
	artifactValidationCodeWrite              uint32 = 10
	artifactValidationCodeUnavailable        uint32 = 11
	artifactValidationCodeStub               uint32 = 99

	artifactValidationMaxCompressedBytes int64 = 50 << 20
)

var (
	errArtifactGenerationMismatch = errors.New("storage artifact validation: object generation mismatch")
	errArtifactInvalid            = errors.New("storage artifact validation: invalid artifact")
	canonicalUUID                 = regexp.MustCompile(`^[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}$`)
)

type artifactValidationState struct {
	client     *s3ClientState
	headObject func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	getObject  func(context.Context, *s3ClientState, *s3.GetObjectInput) (*s3.GetObjectOutput, error)
}

type artifactValidationManager struct {
	mu     sync.Mutex
	states map[s3ApplicationKey]*artifactValidationState
}

func newArtifactValidationManager() *artifactValidationManager {
	return &artifactValidationManager{states: map[s3ApplicationKey]*artifactValidationState{}}
}

func (m *artifactValidationManager) setup(scope ext.Scope) (*artifactValidationState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.states[key]; state != nil {
		return state, nil
	}
	state := &artifactValidationState{client: &s3ClientState{}}
	m.states[key] = state
	return state, nil
}

func (m *artifactValidationManager) forScope(scope ext.Scope) (*artifactValidationState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	state := m.states[key]
	if state == nil {
		return nil, fmt.Errorf("storage artifact validation: setup not called for application %s/%s", key.applicationID, key.instanceID)
	}
	return state, nil
}

func (m *artifactValidationManager) teardown(scope ext.Scope) error {
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

var artifactValidations = newArtifactValidationManager()

func init() {
	ext.Register(newArtifactValidationCapability(artifactValidations))
}

func newArtifactValidationCapability(manager *artifactValidationManager) ext.Capability {
	return ext.Capability{
		Name:     ArtifactValidationCapability,
		Provider: "github.com/BananaLabs-OSS/Pulp-ext-s3",
		Setup: func(env ext.SetupEnv) error {
			_, err := manager.setup(env.EffectiveScope())
			return err
		},
		Register: func(builder wazero.HostModuleBuilder, cell ext.Cell) error {
			scope, err := ext.ValidatedScopeOf(cell)
			if err != nil {
				return fmt.Errorf("storage artifact validation: resolve cell scope: %w", err)
			}
			state, err := manager.forScope(scope)
			if err != nil {
				return err
			}
			builder.NewFunctionBuilder().WithFunc(func(ctx context.Context, module api.Module, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
				return artifactValidationHost(ctx, module, state, requestPtr, requestLen, responsePtrPtr, responseLenPtr)
			}).Export(artifactValidationExport)
			return nil
		},
		Stub: func(builder wazero.HostModuleBuilder, _ ext.Cell) error {
			builder.NewFunctionBuilder().WithFunc(func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 {
				return artifactValidationCodeStub
			}).Export(artifactValidationExport)
			return nil
		},
		TeardownScope: func(_ context.Context, scope ext.Scope) error { return manager.teardown(scope) },
	}
}

func artifactValidationHost(ctx context.Context, module api.Module, state *artifactValidationState, requestPtr, requestLen, responsePtrPtr, responseLenPtr uint32) uint32 {
	if requestLen == 0 {
		return artifactValidationCodeEmpty
	}
	if module == nil || module.Memory() == nil {
		return artifactValidationCodeMemory
	}
	wire, ok := module.Memory().Read(requestPtr, requestLen)
	if !ok {
		return artifactValidationCodeMemory
	}
	result, code := artifactValidationWire(ctx, state, wire)
	if code != artifactValidationCodeOK {
		return code
	}
	return writeArtifactValidationResponse(ctx, module, result, responsePtrPtr, responseLenPtr)
}

func artifactValidationWire(ctx context.Context, state *artifactValidationState, wire []byte) ([]byte, uint32) {
	var header struct {
		ContractVersion string `msgpack:"contract_version"`
	}
	reader := bytes.NewReader(wire)
	if err := msgpack.NewDecoder(reader).Decode(&header); err != nil || reader.Len() != 0 {
		return nil, artifactValidationCodeDecode
	}
	if header.ContractVersion == effect.StorageLargeExactObjectValidationContractV1 {
		return largeExactObjectValidationWire(ctx, state, wire)
	}
	return artifactValidationWireV1(ctx, state, wire)
}

func artifactValidationWireV1(ctx context.Context, state *artifactValidationState, wire []byte) ([]byte, uint32) {
	var command effect.StorageArtifactZIPValidationCommandV1
	reader := bytes.NewReader(wire)
	decoder := msgpack.NewDecoder(reader)
	decoder.DisallowUnknownFields(true)
	if err := decoder.Decode(&command); err != nil || reader.Len() != 0 {
		return nil, artifactValidationCodeDecode
	}
	if err := command.Validate(); err != nil {
		return nil, artifactValidationCodeInvalid
	}
	result, err := state.validate(ctx, command)
	if err != nil {
		return nil, artifactValidationErrorCode(err)
	}
	encoded, err := msgpack.Marshal(result)
	if err != nil {
		return nil, artifactValidationCodeEncode
	}
	return encoded, artifactValidationCodeOK
}

func (s *artifactValidationState) validate(ctx context.Context, command effect.StorageArtifactZIPValidationCommandV1) (effect.StorageArtifactZIPValidationResultV1, error) {
	if s == nil || s.client == nil {
		return effect.StorageArtifactZIPValidationResultV1{}, errors.New("storage artifact validation: provider unavailable")
	}
	if command.ContentLength <= 0 || command.ContentLength > artifactValidationMaxCompressedBytes {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("%w: declared content length exceeds 50MiB", errArtifactInvalid)
	}
	if err := s.client.ensureClient(); err != nil {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("storage artifact validation: client: %w", err)
	}
	get := s.getObject
	if get == nil {
		get = func(ctx context.Context, state *s3ClientState, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return state.client.GetObject(ctx, input)
		}
	}
	output, err := get(ctx, s.client, &s3.GetObjectInput{Bucket: aws.String(s.client.bucket), Key: aws.String(command.ObjectKey), IfMatch: quotedETag(command.ExpectedGeneration)})
	if err != nil {
		if classifyS3Error(err) == scopedObjectStoreNotFoundCode {
			return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("%w: %s", ErrObjectNotFound, command.ObjectKey)
		}
		if isArtifactPreconditionFailure(err) {
			return effect.StorageArtifactZIPValidationResultV1{}, errArtifactGenerationMismatch
		}
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("storage artifact validation: get %q: %w", command.ObjectKey, err)
	}
	if output == nil || output.Body == nil {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("%w: object read returned no body", errArtifactInvalid)
	}
	defer output.Body.Close()
	if output.ContentLength == nil || *output.ContentLength != command.ContentLength || *output.ContentLength > artifactValidationMaxCompressedBytes {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("%w: observed content length does not match declared length", errArtifactInvalid)
	}
	generation := normalizeETag(output.ETag)
	if generation == "" || generation != normalizeETagValue(command.ExpectedGeneration) {
		return effect.StorageArtifactZIPValidationResultV1{}, errArtifactGenerationMismatch
	}
	body, err := io.ReadAll(io.LimitReader(output.Body, artifactValidationMaxCompressedBytes+1))
	if err != nil {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("storage artifact validation: read object: %w", err)
	}
	if int64(len(body)) != command.ContentLength || int64(len(body)) > artifactValidationMaxCompressedBytes {
		return effect.StorageArtifactZIPValidationResultV1{}, fmt.Errorf("%w: object body violates declared length cap", errArtifactInvalid)
	}
	sum := sha256.Sum256(body)
	result := effect.StorageArtifactZIPValidationResultV1{
		ContractVersion: command.ContractVersion,
		UploadID:        command.UploadID,
		ObjectKey:       command.ObjectKey,
		// Preserve the owner-recorded generation representation after the
		// conditional read has compared its normalized ETag. The Fiber wrapper
		// deliberately binds this field byte-for-byte to the durable command.
		Generation:            command.ExpectedGeneration,
		Purpose:               command.Purpose,
		ObservedContentLength: int64(len(body)),
		SHA256:                hex.EncodeToString(sum[:]),
		ValidatorVersion:      command.ValidatorVersion,
	}
	if err := validateArtifactZip(body, command, &result); err != nil {
		result.Valid = false
		result.ErrorCode = artifactValidationErrorName(err)
		return result, nil
	}
	result.Valid = true
	return result, nil
}

func writeArtifactValidationResponse(ctx context.Context, module api.Module, value []byte, responsePtrPtr, responseLenPtr uint32) uint32 {
	if module == nil || module.Memory() == nil {
		return artifactValidationCodeMemory
	}
	alloc := module.ExportedFunction("pulp_alloc")
	if alloc == nil {
		return artifactValidationCodeAllocate
	}
	allocated, err := alloc.Call(ctx, uint64(len(value)))
	if err != nil || len(allocated) == 0 || allocated[0] == 0 {
		return artifactValidationCodeAllocate
	}
	ptr := uint32(allocated[0])
	if !module.Memory().Write(ptr, value) || !module.Memory().WriteUint32Le(responsePtrPtr, ptr) || !module.Memory().WriteUint32Le(responseLenPtr, uint32(len(value))) {
		return artifactValidationCodeWrite
	}
	return artifactValidationCodeOK
}

func artifactValidationErrorCode(err error) uint32 {
	if errors.Is(err, ErrObjectNotFound) {
		return artifactValidationCodeNotFound
	}
	if errors.Is(err, errArtifactGenerationMismatch) {
		return artifactValidationCodeGenerationMismatch
	}
	if strings.Contains(err.Error(), "client:") || strings.Contains(err.Error(), "provider unavailable") {
		return artifactValidationCodeUnavailable
	}
	return artifactValidationCodeProvider
}

func isArtifactPreconditionFailure(err error) bool {
	message := strings.ToLower(err.Error())
	return strings.Contains(message, "preconditionfailed") || strings.Contains(message, "precondition failed") || strings.Contains(message, "412")
}

func validateArtifactZip(body []byte, command effect.StorageArtifactZIPValidationCommandV1, result *effect.StorageArtifactZIPValidationResultV1) error {
	reader, err := zip.NewReader(bytes.NewReader(body), int64(len(body)))
	if err != nil {
		return fmt.Errorf("zip_invalid")
	}
	limits := command.Limits
	if len(reader.File) == 0 || len(reader.File) > int(limits.MaxEntries) {
		return fmt.Errorf("zip_entry_count")
	}
	var total uint64
	var rootManifest *zip.File
	for _, entry := range reader.File {
		if err := validateArtifactZipEntry(entry, limits); err != nil {
			return err
		}
		if entry.UncompressedSize64 > uint64(limits.MaxEntryUncompressedBytes) {
			return fmt.Errorf("zip_entry_size")
		}
		if total > uint64(limits.MaxTotalUncompressedBytes)-entry.UncompressedSize64 {
			return fmt.Errorf("zip_total_size")
		}
		total += entry.UncompressedSize64
		if entry.UncompressedSize64 > 0 && (entry.CompressedSize64 == 0 || entry.UncompressedSize64 > entry.CompressedSize64*uint64(limits.MaxCompressionRatio)) {
			return fmt.Errorf("zip_compression_ratio")
		}
		if entry.Name == requiredArtifactManifest(command.Purpose) {
			if rootManifest != nil {
				return fmt.Errorf("manifest_duplicate")
			}
			rootManifest = entry
		}
	}
	if rootManifest == nil {
		return fmt.Errorf("manifest_missing")
	}
	manifest, err := readArtifactManifest(rootManifest, limits.MaxManifestBytes)
	if err != nil {
		return err
	}
	if string(command.Purpose) == "datapack" {
		return validateDatapackManifest(manifest)
	}
	uuid, version, err := validateBedrockManifest(manifest)
	if err != nil {
		return err
	}
	result.ManifestUUID, result.ManifestVersion = uuid, version
	return nil
}

func validateArtifactZipEntry(entry *zip.File, limits effect.StorageArtifactZIPValidationLimitsV1) error {
	if entry == nil || entry.Name == "" || strings.Contains(entry.Name, "\\") || strings.Contains(entry.Name, ":") || strings.HasPrefix(entry.Name, "/") || strings.HasPrefix(entry.Name, "\\") {
		return fmt.Errorf("zip_path")
	}
	name := strings.TrimSuffix(entry.Name, "/")
	if name == "" || path.Clean(name) != name || name == "." || strings.HasPrefix(name, "../") {
		return fmt.Errorf("zip_path")
	}
	if entry.FileInfo().Mode()&fs.ModeSymlink != 0 {
		return fmt.Errorf("zip_symlink")
	}
	if entry.UncompressedSize64 > uint64(limits.MaxEntryUncompressedBytes) {
		return fmt.Errorf("zip_entry_size")
	}
	return nil
}

func requiredArtifactManifest(purpose effect.StorageArtifactPurposeV1) string {
	if string(purpose) == "datapack" {
		return "pack.mcmeta"
	}
	return "manifest.json"
}

func readArtifactManifest(entry *zip.File, max int64) ([]byte, error) {
	if entry.UncompressedSize64 > uint64(max) {
		return nil, fmt.Errorf("manifest_size")
	}
	stream, err := entry.Open()
	if err != nil {
		return nil, fmt.Errorf("manifest_read")
	}
	defer stream.Close()
	value, err := io.ReadAll(io.LimitReader(stream, max+1))
	if err != nil || int64(len(value)) > max {
		return nil, fmt.Errorf("manifest_size")
	}
	return value, nil
}

func validateDatapackManifest(value []byte) error {
	var document struct {
		Pack struct {
			PackFormat  json.Number     `json:"pack_format"`
			Description json.RawMessage `json:"description"`
		} `json:"pack"`
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil || document.Pack.PackFormat == "" || len(document.Pack.Description) == 0 || string(document.Pack.Description) == "null" {
		return fmt.Errorf("datapack_manifest")
	}
	format, err := document.Pack.PackFormat.Int64()
	if err != nil || format < 1 {
		return fmt.Errorf("datapack_manifest")
	}
	return nil
}

func validateBedrockManifest(value []byte) (string, string, error) {
	var document struct {
		Header struct {
			UUID    string        `json:"uuid"`
			Version []json.Number `json:"version"`
		} `json:"header"`
	}
	if !json.Valid(value) {
		return "", "", fmt.Errorf("bedrock_manifest")
	}
	decoder := json.NewDecoder(bytes.NewReader(value))
	decoder.UseNumber()
	if err := decoder.Decode(&document); err != nil || canonicalUUID.FindString(document.Header.UUID) != document.Header.UUID || len(document.Header.Version) != 4 {
		return "", "", fmt.Errorf("bedrock_manifest")
	}
	parts := make([]string, 4)
	for index, number := range document.Header.Version {
		integer, err := number.Int64()
		if err != nil || integer < 0 || integer > 65535 {
			return "", "", fmt.Errorf("bedrock_manifest")
		}
		parts[index] = number.String()
	}
	version := strings.Join(parts, ".")
	if len(version) == 0 || len(version) > 64 {
		return "", "", fmt.Errorf("bedrock_manifest")
	}
	return document.Header.UUID, version, nil
}

func artifactValidationErrorName(err error) string {
	if err == nil {
		return ""
	}
	value := err.Error()
	if len(value) > 64 || !regexp.MustCompile(`^[a-z0-9][a-z0-9_.-]{0,63}$`).MatchString(value) {
		return "zip_invalid"
	}
	return value
}
