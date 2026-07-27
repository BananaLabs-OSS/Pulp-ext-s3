package s3ext

import (
	"context"
	"reflect"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

func TestPublicUploadCapabilityBindsOnlyExactObjectABI(t *testing.T) {
	manager := newPublicUploadManager()
	scope := scopedS3(t, "sessions", "blue", "uploads")
	if _, err := manager.setup(scope); err != nil {
		t.Fatal(err)
	}
	capability := newPublicUploadCapability(manager, func() time.Time { return time.Unix(100, 0) })
	runtime := wazero.NewRuntime(context.Background())
	defer runtime.Close(context.Background())
	builder := runtime.NewHostModuleBuilder("public_upload_active")
	if err := capability.Register(builder, publicUploadTestCell{scope: scope}); err != nil {
		t.Fatalf("register: %v", err)
	}
	module, err := builder.Instantiate(context.Background())
	if err != nil {
		t.Fatalf("instantiate: %v", err)
	}
	for _, name := range []string{publicUploadPresignExport, publicUploadValidateExport, publicUploadDeleteExport} {
		definition, ok := module.ExportedFunctionDefinitions()[name]
		if !ok {
			t.Fatalf("missing %q", name)
		}
		if want := []api.ValueType{api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32, api.ValueTypeI32}; !reflect.DeepEqual(definition.ParamTypes(), want) || !reflect.DeepEqual(definition.ResultTypes(), []api.ValueType{api.ValueTypeI32}) {
			t.Fatalf("%s ABI = %#v -> %#v", name, definition.ParamTypes(), definition.ResultTypes())
		}
	}
	stub := runtime.NewHostModuleBuilder("public_upload_stub")
	if err := capability.Stub(stub, publicUploadTestCell{scope: scope}); err != nil {
		t.Fatalf("stub: %v", err)
	}
	if got := publicUploadCodeStub; got != 99 {
		t.Fatalf("stub code = %d", got)
	}
}

func TestPublicUploadDeleteUsesExactGenerationFenceAndIsReplaySafe(t *testing.T) {
	state := newPublicUploadTestState(t)
	command := effect.StorageExactObjectDeleteCommand{ExactKey: "uploads/order-1/world.zip", ExpectedGeneration: "etag-1"}
	headCalls, deleteCalls := 0, 0
	state.headObject = func(_ context.Context, _ *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		headCalls++
		if got := aws.ToString(input.Key); got != command.ExactKey {
			t.Fatalf("head key = %q", got)
		}
		if headCalls == 2 {
			return nil, &s3types.NoSuchKey{}
		}
		return &s3.HeadObjectOutput{ETag: aws.String(`"etag-1"`)}, nil
	}
	state.deleteObject = func(_ context.Context, _ *s3ClientState, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
		deleteCalls++
		if got := aws.ToString(input.IfMatch); got != `"etag-1"` || aws.ToString(input.Key) != command.ExactKey {
			t.Fatalf("delete fence/key = %#v", input)
		}
		return &s3.DeleteObjectOutput{}, nil
	}
	wire, err := msgpack.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	for attempt := 0; attempt < 2; attempt++ {
		encoded, code := publicUploadDeleteWire(context.Background(), state, wire)
		if code != publicUploadCodeOK {
			t.Fatalf("delete attempt %d code = %d", attempt, code)
		}
		var result effect.StorageExactObjectDeleteResult
		if err := msgpack.Unmarshal(encoded, &result); err != nil || result != (effect.StorageExactObjectDeleteResult{ExactKey: command.ExactKey, ExpectedGeneration: command.ExpectedGeneration}) {
			t.Fatalf("delete result = %#v, %v", result, err)
		}
	}
	if deleteCalls != 1 {
		t.Fatalf("delete calls = %d", deleteCalls)
	}
}

func TestPublicUploadDeleteRejectsGenerationReplacement(t *testing.T) {
	state := newPublicUploadTestState(t)
	command := effect.StorageExactObjectDeleteCommand{ExactKey: "uploads/a.zip", ExpectedGeneration: "etag-before"}
	state.headObject = func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{ETag: aws.String(`"etag-after"`)}, nil
	}
	wire, _ := msgpack.Marshal(command)
	if _, code := publicUploadDeleteWire(context.Background(), state, wire); code != publicUploadCodeGeneration {
		t.Fatalf("replacement delete code = %d", code)
	}
}

func TestPublicUploadDeleteStateDoesNotSurviveApplicationTeardown(t *testing.T) {
	manager := newPublicUploadManager()
	scope := scopedS3(t, "sessions", "primary", "uploads")
	first, err := manager.setup(scope)
	if err != nil {
		t.Fatal(err)
	}
	if err := manager.teardown(scope); err != nil {
		t.Fatal(err)
	}
	second, err := manager.setup(scope)
	if err != nil {
		t.Fatal(err)
	}
	if first == second || first.client == second.client {
		t.Fatal("delete-capable application state survived teardown")
	}
}

func TestPublicUploadPresignAndValidationAreBoundToOneExactObject(t *testing.T) {
	state := newPublicUploadTestState(t)
	command := effect.StorageExactObjectPresignPutCommand{
		ExactKey: "uploads/order-1/world.zip", UploadID: "upload-1", ExpectedGeneration: effect.StorageObjectGenerationAbsent,
		ContentLength: 12, ContentType: "application/zip", TTLSec: 300,
	}
	state.presignPut = func(_ context.Context, _ *s3ClientState, input *s3.PutObjectInput, ttl time.Duration) (string, error) {
		if got := aws.ToString(input.Key); got != command.ExactKey || aws.ToString(input.Bucket) != "test-bucket" {
			t.Fatalf("presign exact scope/key = %q/%q", aws.ToString(input.Bucket), got)
		}
		if got := aws.ToInt64(input.ContentLength); got != command.ContentLength || aws.ToString(input.ContentType) != command.ContentType || !metadataEquals(input.Metadata, "pulp-upload-id", command.UploadID) {
			t.Fatalf("presign policy = %#v", input)
		}
		if aws.ToString(input.IfNoneMatch) != "*" || input.IfMatch != nil || ttl != 5*time.Minute {
			t.Fatalf("presign generation/ttl fence = %#v / %s", input, ttl)
		}
		return "https://object.test/test-bucket/uploads/order-1/world.zip?signature=ok", nil
	}
	wire, err := msgpack.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	encoded, code := publicUploadPresignWire(context.Background(), state, func() time.Time { return time.Unix(100, 0) }, wire)
	if code != publicUploadCodeOK {
		t.Fatalf("presign code = %d", code)
	}
	var result effect.StorageExactObjectPresignPutResult
	if err := msgpack.Unmarshal(encoded, &result); err != nil || result.ExpiresAtUnix != 400 || result.ExactKey != command.ExactKey {
		t.Fatalf("presign result = %#v, %v", result, err)
	}

	validate := effect.StorageExactObjectValidatePutCommand{ExactKey: command.ExactKey, UploadID: command.UploadID, ContentLength: command.ContentLength, ContentType: command.ContentType}
	state.headObject = func(_ context.Context, _ *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		if aws.ToString(input.Key) != command.ExactKey || aws.ToString(input.Bucket) != "test-bucket" {
			t.Fatalf("head exact scope/key = %#v", input)
		}
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(command.ContentLength), ContentType: aws.String(command.ContentType), ETag: aws.String("\"etag-1\""),
			Metadata: map[string]string{"pulp-upload-id": command.UploadID},
		}, nil
	}
	wire, err = msgpack.Marshal(validate)
	if err != nil {
		t.Fatal(err)
	}
	encoded, code = publicUploadValidateWire(context.Background(), state, wire)
	if code != publicUploadCodeOK {
		t.Fatalf("validate code = %d", code)
	}
	var validated effect.StorageExactObjectValidatePutResult
	if err := msgpack.Unmarshal(encoded, &validated); err != nil || validated.Generation != "etag-1" || validated.ExactKey != command.ExactKey {
		t.Fatalf("validation result = %#v, %v", validated, err)
	}
}

func TestPublicUploadRejectsCrossScopeAndReplayMutation(t *testing.T) {
	manager := newPublicUploadManager()
	leftScope := scopedS3(t, "sessions", "blue", "uploads")
	rightScope := scopedS3(t, "evolution", "blue", "uploads")
	left, err := manager.setup(leftScope)
	if err != nil {
		t.Fatal(err)
	}
	right, err := manager.setup(rightScope)
	if err != nil {
		t.Fatal(err)
	}
	if left == right || left.client == right.client {
		t.Fatal("applications share public-upload provider state")
	}
	setPublicUploadEnv(t)
	command := effect.StorageExactObjectPresignPutCommand{ExactKey: "uploads/a.zip", UploadID: "upload-a", ExpectedGeneration: effect.StorageObjectGenerationAbsent, ContentLength: 1, ContentType: "application/zip", TTLSec: 1}
	left.presignPut = func(context.Context, *s3ClientState, *s3.PutObjectInput, time.Duration) (string, error) {
		return "https://object.test/a", nil
	}
	wire, _ := msgpack.Marshal(command)
	if _, code := publicUploadPresignWire(context.Background(), left, time.Now, wire); code != publicUploadCodeOK {
		t.Fatalf("left presign code = %d", code)
	}
	validation := effect.StorageExactObjectValidatePutCommand{ExactKey: command.ExactKey, UploadID: command.UploadID, ContentLength: command.ContentLength, ContentType: command.ContentType}
	validationWire, _ := msgpack.Marshal(validation)
	if _, code := publicUploadValidateWire(context.Background(), right, validationWire); code == publicUploadCodeOK {
		t.Fatal("cross-application validation was accepted")
	}
	command.ContentLength = 2
	wire, _ = msgpack.Marshal(command)
	if _, code := publicUploadPresignWire(context.Background(), left, time.Now, wire); code == publicUploadCodeOK {
		t.Fatal("replayed upload id with changed policy was accepted")
	}
}

func TestPublicUploadSignsExactExistingGenerationFence(t *testing.T) {
	state := newPublicUploadTestState(t)
	command := effect.StorageExactObjectPresignPutCommand{
		ExactKey: "uploads/a.zip", UploadID: "upload-update", ExpectedGeneration: "etag-before",
		ContentLength: 4, ContentType: "application/zip", TTLSec: 1,
	}
	state.presignPut = func(_ context.Context, _ *s3ClientState, input *s3.PutObjectInput, _ time.Duration) (string, error) {
		if got := aws.ToString(input.IfMatch); got != "\"etag-before\"" || input.IfNoneMatch != nil {
			t.Fatalf("exact generation fence = IfMatch %q IfNoneMatch %#v", got, input.IfNoneMatch)
		}
		return "https://object.test/a", nil
	}
	wire, _ := msgpack.Marshal(command)
	if _, code := publicUploadPresignWire(context.Background(), state, time.Now, wire); code != publicUploadCodeOK {
		t.Fatalf("presign code = %d", code)
	}
}

func TestPublicUploadValidationRejectsObjectThatBreaksSignedPolicy(t *testing.T) {
	state := newPublicUploadTestState(t)
	command := effect.StorageExactObjectPresignPutCommand{ExactKey: "uploads/a.zip", UploadID: "upload-a", ExpectedGeneration: effect.StorageObjectGenerationAbsent, ContentLength: 4, ContentType: "application/zip", TTLSec: 1}
	state.presignPut = func(context.Context, *s3ClientState, *s3.PutObjectInput, time.Duration) (string, error) {
		return "https://object.test/a", nil
	}
	wire, _ := msgpack.Marshal(command)
	if _, code := publicUploadPresignWire(context.Background(), state, time.Now, wire); code != publicUploadCodeOK {
		t.Fatalf("presign = %d", code)
	}
	validate := effect.StorageExactObjectValidatePutCommand{ExactKey: command.ExactKey, UploadID: command.UploadID, ContentLength: command.ContentLength, ContentType: command.ContentType}
	state.headObject = func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{ContentLength: aws.Int64(5), ContentType: aws.String(command.ContentType), ETag: aws.String("etag"), Metadata: map[string]string{"pulp-upload-id": "other-upload"}}, nil
	}
	wire, _ = msgpack.Marshal(validate)
	if _, code := publicUploadValidateWire(context.Background(), state, wire); code == publicUploadCodeOK {
		t.Fatal("wrong content length was accepted")
	}
	state.headObject = func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		return &s3.HeadObjectOutput{ContentLength: aws.Int64(command.ContentLength), ContentType: aws.String(command.ContentType), ETag: aws.String("etag"), Metadata: map[string]string{"pulp-upload-id": "other-upload"}}, nil
	}
	if _, code := publicUploadValidateWire(context.Background(), state, wire); code == publicUploadCodeOK {
		t.Fatal("wrong upload-id metadata was accepted")
	}
}

func newPublicUploadTestState(t *testing.T) *publicUploadState {
	t.Helper()
	setPublicUploadEnv(t)
	return &publicUploadState{client: &s3ClientState{}, plans: map[string]publicUploadPlan{}}
}

func setPublicUploadEnv(t *testing.T) {
	t.Helper()
	t.Setenv("S3_ACCESS_KEY_ID", "test-access")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test-secret")
	t.Setenv("S3_BUCKET", "test-bucket")
	t.Setenv("S3_ENDPOINT", "https://object.test")
}

type publicUploadTestCell struct{ scope ext.Scope }

func (c publicUploadTestCell) Name() string     { return c.scope.CellID() }
func (c publicUploadTestCell) Scope() ext.Scope { return c.scope }
