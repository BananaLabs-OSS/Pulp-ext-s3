package s3ext

import (
	"context"
	"reflect"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

func TestDownloadReferenceCapabilityBindsOnlyExactObjectABI(t *testing.T) {
	manager := newDownloadReferenceManager()
	scope := scopedS3(t, "evolution", "primary", "downloads")
	if _, err := manager.setup(scope); err != nil {
		t.Fatal(err)
	}
	capability := newDownloadReferenceCapability(manager)
	if capability.Name != effect.StorageExactObjectDownloadReferenceCapability || capability.Provider != "github.com/BananaLabs-OSS/Pulp-ext-s3" {
		t.Fatalf("capability identity = %#v", capability)
	}
	runtime := wazero.NewRuntime(context.Background())
	defer runtime.Close(context.Background())
	builder := runtime.NewHostModuleBuilder("download_reference_active")
	if err := capability.Register(builder, publicUploadTestCell{scope: scope}); err != nil {
		t.Fatal(err)
	}
	module, err := builder.Instantiate(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	definition, ok := module.ExportedFunctionDefinitions()[downloadReferenceExport]
	if !ok {
		t.Fatalf("missing %q", downloadReferenceExport)
	}
	if want := []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}; !reflect.DeepEqual(definition.ParamTypes(), want) || !reflect.DeepEqual(definition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
		t.Fatalf("ABI = %#v -> %#v", definition.ParamTypes(), definition.ResultTypes())
	}
}

func TestDownloadReferenceIsGenerationFencedStableAndScoped(t *testing.T) {
	setPublicUploadEnv(t)
	t.Setenv("S3_PUBLIC_BASE_URL", "https://downloads.example.test/stable")
	state := &downloadReferenceState{client: &s3ClientState{}}
	command := effect.StorageExactObjectDownloadReferenceCommand{
		ContractVersion: effect.StorageExactObjectDownloadReferenceContractV1,
		ObjectKey:       "uploads/order 1/world.zip", ExpectedETag: "etag-1", ExpiresAtUnix: 123,
	}
	state.headObject = func(_ context.Context, client *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		if got, want := aws.ToString(input.Bucket), "test-bucket"; got != want {
			t.Fatalf("bucket = %q", got)
		}
		if got, want := aws.ToString(input.Key), command.ObjectKey; got != want {
			t.Fatalf("key = %q", got)
		}
		if got, want := aws.ToString(input.IfMatch), `"etag-1"`; got != want {
			t.Fatalf("IfMatch = %q", got)
		}
		return &s3.HeadObjectOutput{ETag: aws.String(`"etag-1"`)}, nil
	}
	wire, err := msgpack.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	first, code := downloadReferenceWire(context.Background(), state, wire)
	if code != downloadReferenceCodeOK {
		t.Fatalf("first code = %d", code)
	}
	second, code := downloadReferenceWire(context.Background(), state, wire)
	if code != downloadReferenceCodeOK || !reflect.DeepEqual(first, second) {
		t.Fatalf("replay code/value = %d / %q / %q", code, first, second)
	}
	var result effect.StorageExactObjectDownloadReferenceResult
	if err := msgpack.Unmarshal(first, &result); err != nil {
		t.Fatal(err)
	}
	if result.PublicURL != "https://downloads.example.test/stable/uploads/order%201/world.zip" || result.ExpiresAtUnix != command.ExpiresAtUnix || result.ExpectedETag != command.ExpectedETag {
		t.Fatalf("result = %#v", result)
	}
}

func TestDownloadReferenceFailsClosedForMissingPublicProviderOrReplacement(t *testing.T) {
	setPublicUploadEnv(t)
	state := &downloadReferenceState{client: &s3ClientState{}}
	command := effect.StorageExactObjectDownloadReferenceCommand{ContractVersion: effect.StorageExactObjectDownloadReferenceContractV1, ObjectKey: "uploads/a.zip", ExpectedETag: "etag-1", ExpiresAtUnix: 1}
	state.headObject = func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{ETag: aws.String(`"etag-1"`)}, nil
	}
	wire, _ := msgpack.Marshal(command)
	if _, code := downloadReferenceWire(context.Background(), state, wire); code != downloadReferenceCodeUnavailable {
		t.Fatalf("unconfigured provider code = %d", code)
	}
	t.Setenv("S3_PUBLIC_BASE_URL", "https://downloads.example.test")
	state.headObject = func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{ETag: aws.String(`"etag-new"`)}, nil
	}
	if _, code := downloadReferenceWire(context.Background(), state, wire); code != downloadReferenceCodeGeneration {
		t.Fatalf("replacement code = %d", code)
	}
}

func TestDownloadReferenceTeardownIsApplicationScoped(t *testing.T) {
	manager := newDownloadReferenceManager()
	leftScope := scopedS3(t, "sessions", "primary", "downloads")
	rightScope := scopedS3(t, "evolution", "primary", "downloads")
	left, err := manager.setup(leftScope)
	if err != nil {
		t.Fatal(err)
	}
	right, err := manager.setup(rightScope)
	if err != nil {
		t.Fatal(err)
	}
	if left == right || left.client == right.client {
		t.Fatal("download state crossed application boundary")
	}
	if err := manager.teardown(leftScope); err != nil {
		t.Fatal(err)
	}
	if _, err := manager.forScope(leftScope); err == nil {
		t.Fatal("torn-down application retained state")
	}
	if _, err := manager.forScope(rightScope); err != nil {
		t.Fatalf("teardown crossed scope: %v", err)
	}
}
