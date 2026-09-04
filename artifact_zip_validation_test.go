package s3ext

import (
	"archive/zip"
	"bytes"
	"context"
	"io"
	"io/fs"
	"reflect"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

func TestArtifactValidationCapabilityExportsOnlyExactZIPABI(t *testing.T) {
	manager := newArtifactValidationManager()
	scope := scopedS3(t, "evolution", "blue", "artifact-validator")
	if _, err := manager.setup(scope); err != nil {
		t.Fatal(err)
	}
	capability := newArtifactValidationCapability(manager)
	if capability.Name != effect.StorageArtifactValidationCapability || capability.Provider != "github.com/BananaLabs-OSS/Pulp-ext-s3" {
		t.Fatalf("capability identity = %#v", capability)
	}
	runtime := wazero.NewRuntime(context.Background())
	defer runtime.Close(context.Background())
	builder := runtime.NewHostModuleBuilder("artifact_validation_active")
	if err := capability.Register(builder, publicUploadTestCell{scope: scope}); err != nil {
		t.Fatalf("register: %v", err)
	}
	module, err := builder.Instantiate(context.Background())
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	if got, want := len(module.ExportedFunctionDefinitions()), 1; got != want {
		t.Fatalf("unexpected exported host functions = %d", got)
	}
	definition, ok := module.ExportedFunctionDefinitions()[artifactValidationExport]
	if !ok {
		t.Fatalf("missing %q", artifactValidationExport)
	}
	if want := []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}; !reflect.DeepEqual(definition.ParamTypes(), want) || !reflect.DeepEqual(definition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
		t.Fatalf("ABI = %#v -> %#v", definition.ParamTypes(), definition.ResultTypes())
	}
}

func TestArtifactValidationReadsOneExactGenerationAndReturnsDatapackEvidence(t *testing.T) {
	body := testArtifactZip(t, map[string]string{"pack.mcmeta": `{"pack":{"pack_format":15,"description":"test"}}`, "data/example/functions/load.mcfunction": "say ready"})
	state := newArtifactValidationTestState(t)
	command := testArtifactCommand("uploads/a.zip", "etag-1", int64(len(body)), effect.StorageArtifactPurposeDatapackV1)
	state.getObject = func(_ context.Context, client *s3ClientState, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		if got, want := aws.ToString(input.Bucket), "test-bucket"; got != want {
			t.Fatalf("bucket = %q, want %q", got, want)
		}
		if got, want := aws.ToString(input.Key), command.ObjectKey; got != want {
			t.Fatalf("key = %q, want %q", got, want)
		}
		if got, want := aws.ToString(input.IfMatch), `"etag-1"`; got != want {
			t.Fatalf("IfMatch = %q, want %q", got, want)
		}
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body)), ContentLength: aws.Int64(int64(len(body))), ETag: aws.String(`"etag-1"`)}, nil
	}
	result, err := state.validate(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if !result.Valid || result.ErrorCode != "" || result.Generation != command.ExpectedGeneration || result.SHA256 == "" || result.ManifestUUID != "" {
		t.Fatalf("result = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("result contract invalid: %v", err)
	}
	replayed, err := state.validate(context.Background(), command)
	if err != nil || !reflect.DeepEqual(replayed, result) {
		t.Fatalf("idempotent replay = %#v, %v; initial %#v", replayed, err, result)
	}
}

func TestArtifactValidationReturnsInvalidEvidenceForUnsafeZIPAndKeepsIdentity(t *testing.T) {
	body := testArtifactZip(t, map[string]string{"pack.mcmeta": `{"pack":{}}`, `data\\unsafe.txt`: "no"})
	state := newArtifactValidationTestState(t)
	command := testArtifactCommand("uploads/a.zip", "etag-1", int64(len(body)), effect.StorageArtifactPurposeDatapackV1)
	state.getObject = artifactObjectResult(body, command.ExpectedGeneration)
	result, err := state.validate(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || result.ErrorCode != "zip_path" || result.UploadID != command.UploadID || result.ObjectKey != command.ObjectKey || result.ObservedContentLength != command.ContentLength {
		t.Fatalf("invalid evidence = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("invalid evidence violates contract: %v", err)
	}
}

func TestArtifactValidationRejectsZIPSymlinkWithoutExtracting(t *testing.T) {
	var raw bytes.Buffer
	writer := zip.NewWriter(&raw)
	pack, err := writer.Create("pack.mcmeta")
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(pack, `{"pack":{"pack_format":15,"description":"test"}}`); err != nil {
		t.Fatal(err)
	}
	link := &zip.FileHeader{Name: "data/link"}
	link.SetMode(fs.ModeSymlink | 0o777)
	entry, err := writer.CreateHeader(link)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.WriteString(entry, "target"); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	body := raw.Bytes()
	state := newArtifactValidationTestState(t)
	command := testArtifactCommand("uploads/symlink.zip", "etag-1", int64(len(body)), effect.StorageArtifactPurposeDatapackV1)
	state.getObject = artifactObjectResult(body, command.ExpectedGeneration)
	result, err := state.validate(context.Background(), command)
	if err != nil || result.Valid || result.ErrorCode != "zip_symlink" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

func TestArtifactValidationValidatesBedrockRootManifestOnly(t *testing.T) {
	body := testArtifactZip(t, map[string]string{
		"manifest.json":        `{"header":{"uuid":"01234567-89ab-4cde-8fab-0123456789ab","version":[1,2,3,4]}}`,
		"nested/manifest.json": `{"header":{"uuid":"not-used","version":[1]}}`,
	})
	state := newArtifactValidationTestState(t)
	command := testArtifactCommand("uploads/bedrock.zip", "etag-2", int64(len(body)), effect.StorageArtifactPurposeBedrockResourceV1)
	state.getObject = artifactObjectResult(body, command.ExpectedGeneration)
	result, err := state.validate(context.Background(), command)
	if err != nil || !result.Valid || result.ManifestUUID != "01234567-89ab-4cde-8fab-0123456789ab" || result.ManifestVersion != "1.2.3.4" {
		t.Fatalf("result = %#v, err = %v", result, err)
	}
}

func TestArtifactValidationRejectsMalformedWireAndAppCrossScope(t *testing.T) {
	manager := newArtifactValidationManager()
	leftScope := scopedS3(t, "sessions", "blue", "artifacts")
	rightScope := scopedS3(t, "evolution", "blue", "artifacts")
	left, err := manager.setup(leftScope)
	if err != nil {
		t.Fatal(err)
	}
	right, err := manager.setup(rightScope)
	if err != nil {
		t.Fatal(err)
	}
	if left == right || left.client == right.client {
		t.Fatal("applications share artifact-validation state")
	}
	if _, code := artifactValidationWire(context.Background(), left, mustMsgpack(t, map[string]any{"unknown": true})); code != artifactValidationCodeDecode {
		t.Fatalf("unknown wire code = %d", code)
	}
	if err := manager.teardown(leftScope); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.forScope(leftScope); err == nil {
		t.Fatal("teardown retained application state")
	}
	if _, err := manager.forScope(rightScope); err != nil {
		t.Fatalf("teardown crossed application boundary: %v", err)
	}
}

func newArtifactValidationTestState(t *testing.T) *artifactValidationState {
	t.Helper()
	setPublicUploadEnv(t)
	return &artifactValidationState{client: &s3ClientState{}}
}

func testArtifactCommand(key, generation string, length int64, purpose effect.StorageArtifactPurposeV1) effect.StorageArtifactZIPValidationCommandV1 {
	return effect.StorageArtifactZIPValidationCommandV1{
		ContractVersion: effect.StorageArtifactValidationContractV1,
		UploadID:        "upload-1", ObjectKey: key, ExpectedGeneration: generation, ContentLength: length, Purpose: purpose,
		ValidatorVersion: effect.StorageArtifactZIPValidatorVersionV1, Limits: effect.StorageArtifactZIPValidationFixedLimitsV1(),
	}
}

func artifactObjectResult(body []byte, generation string) func(context.Context, *s3ClientState, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
	return func(_ context.Context, _ *s3ClientState, _ *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		return &s3.GetObjectOutput{Body: io.NopCloser(bytes.NewReader(body)), ContentLength: aws.Int64(int64(len(body))), ETag: aws.String(`"` + generation + `"`)}, nil
	}
}

func testArtifactZip(t *testing.T, files map[string]string) []byte {
	t.Helper()
	var output bytes.Buffer
	writer := zip.NewWriter(&output)
	for name, contents := range files {
		entry, err := writer.Create(name)
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, contents); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	return output.Bytes()
}

func mustMsgpack(t *testing.T, value any) []byte {
	t.Helper()
	encoded, err := msgpack.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
