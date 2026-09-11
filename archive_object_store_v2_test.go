package s3ext

import (
	"bytes"
	"context"
	"errors"
	"io"
	"testing"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

func archiveStoreScope(t *testing.T) ext.Scope {
	t.Helper()
	resetClient(t)
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", "archive-test")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1")
	scope, err := ext.NewScope("sessions", "primary", "fleet", "primary")
	if err != nil {
		t.Fatal(err)
	}
	return scope
}

func TestArchiveObjectStoreV2WritesWithExactGenerationFences(t *testing.T) {
	scope := archiveStoreScope(t)
	var inputs []*s3.PutObjectInput
	store := &ScopedArchiveObjectStoreV2{
		putObject: func(_ context.Context, _ *s3ClientState, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			inputs = append(inputs, input)
			return &s3.PutObjectOutput{ETag: stringPointer(`"written-etag"`)}, nil
		},
	}
	for _, expected := range []string{"current-etag", ObjectGenerationAbsent} {
		result, err := store.WriteExactObject(context.Background(), scope, ExactObjectWrite{
			Key: "internal/result.msgpack", Body: bytes.NewReader([]byte("wire")),
			ContentLength: 4, ContentType: "application/msgpack",
			ExpectedGeneration: expected, Metadata: map[string]string{"Pulp-Intent": "abc"},
		})
		if err != nil {
			t.Fatalf("WriteExactObject(%q): %v", expected, err)
		}
		if result.Generation != "written-etag" {
			t.Fatalf("generation = %q", result.Generation)
		}
	}
	if inputs[0].IfMatch == nil || *inputs[0].IfMatch != `"current-etag"` || inputs[0].IfNoneMatch != nil {
		t.Fatalf("exact generation fence = %#v/%#v", inputs[0].IfMatch, inputs[0].IfNoneMatch)
	}
	if inputs[1].IfNoneMatch == nil || *inputs[1].IfNoneMatch != "*" || inputs[1].IfMatch != nil {
		t.Fatalf("absent generation fence = %#v/%#v", inputs[1].IfMatch, inputs[1].IfNoneMatch)
	}
	if inputs[0].Metadata["pulp-intent"] != "abc" {
		t.Fatalf("metadata = %#v", inputs[0].Metadata)
	}
}

func TestArchiveObjectStoreV2CopyFencesSourceAndDestination(t *testing.T) {
	scope := archiveStoreScope(t)
	var input *s3.CopyObjectInput
	store := &ScopedArchiveObjectStoreV2{
		copyObject: func(_ context.Context, _ *s3ClientState, got *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
			input = got
			return &s3.CopyObjectOutput{
				CopyObjectResult: &s3types.CopyObjectResult{ETag: stringPointer(`"copy-etag"`)},
			}, nil
		},
	}
	result, err := store.CopyExactObject(context.Background(), scope, ExactObjectCopy{
		SourceKey: "backups/current.zip", SourceGeneration: "source-etag",
		DestinationKey: "backups/previous.zip", DestinationGeneration: "destination-etag",
		ContentType: "application/zip", Metadata: map[string]string{"pulp-generation": "g-1"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if result.Generation != "copy-etag" {
		t.Fatalf("generation = %q", result.Generation)
	}
	if input.CopySourceIfMatch == nil || *input.CopySourceIfMatch != `"source-etag"` {
		t.Fatalf("source fence = %#v", input.CopySourceIfMatch)
	}
	if input.IfMatch == nil || *input.IfMatch != `"destination-etag"` || input.IfNoneMatch != nil {
		t.Fatalf("destination fence = %#v/%#v", input.IfMatch, input.IfNoneMatch)
	}
}

func TestArchiveObjectStoreV2DeleteIsGenerationFencedAndReplaySafe(t *testing.T) {
	scope := archiveStoreScope(t)
	headCalls := 0
	deleteCalls := 0
	store := &ScopedArchiveObjectStoreV2{
		headObject: func(_ context.Context, _ *s3ClientState, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			headCalls++
			if headCalls == 2 {
				return nil, archiveStoreAPIError{code: "NoSuchKey"}
			}
			return &s3.HeadObjectOutput{ETag: stringPointer(`"etag-1"`)}, nil
		},
		deleteObject: func(_ context.Context, _ *s3ClientState, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			deleteCalls++
			if input.IfMatch == nil || *input.IfMatch != `"etag-1"` {
				t.Fatalf("delete fence = %#v", input.IfMatch)
			}
			return &s3.DeleteObjectOutput{}, nil
		},
	}
	result, err := store.DeleteExactObject(context.Background(), scope, "worlds/order.zip", "etag-1")
	if err != nil || result.AlreadyAbsent {
		t.Fatalf("first delete = %#v, %v", result, err)
	}
	result, err = store.DeleteExactObject(context.Background(), scope, "worlds/order.zip", "etag-1")
	if err != nil || !result.AlreadyAbsent {
		t.Fatalf("replayed delete = %#v, %v", result, err)
	}
	if deleteCalls != 1 {
		t.Fatalf("delete calls = %d", deleteCalls)
	}
}

func TestArchiveObjectStoreV2RejectsGenerationConflict(t *testing.T) {
	scope := archiveStoreScope(t)
	store := &ScopedArchiveObjectStoreV2{
		putObject: func(context.Context, *s3ClientState, *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			return nil, archiveStoreAPIError{code: "PreconditionFailed"}
		},
	}
	_, err := store.WriteExactObject(context.Background(), scope, ExactObjectWrite{
		Key: "worlds/order.zip", Body: bytes.NewReader([]byte("world")),
		ContentLength: 5, ExpectedGeneration: "etag-old",
	})
	if !errors.Is(err, ErrObjectGenerationMismatch) {
		t.Fatalf("generation conflict = %v", err)
	}
}

func TestArchiveObjectStoreV2BoundsReceiptReads(t *testing.T) {
	scope := archiveStoreScope(t)
	store := &ScopedArchiveObjectStoreV2{
		getObject: func(context.Context, *s3ClientState, *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			size := int64(5)
			now := time.Now().UTC()
			return &s3.GetObjectOutput{
				Body: io.NopCloser(bytes.NewReader([]byte("12345"))), ContentLength: &size,
				ETag: stringPointer(`"receipt-etag"`), LastModified: &now,
			}, nil
		},
	}
	if _, err := store.ReadExactObject(context.Background(), scope, "internal/pulp-receipts/receipt.msgpack", 4); err == nil {
		t.Fatal("oversized receipt read succeeded")
	}
	result, err := store.ReadExactObject(context.Background(), scope, "internal/pulp-receipts/receipt.msgpack", 5)
	if err != nil {
		t.Fatal(err)
	}
	if string(result.Body) != "12345" || result.Generation != "receipt-etag" {
		t.Fatalf("receipt = %#v", result)
	}
}

type archiveStoreAPIError struct{ code string }

func (e archiveStoreAPIError) Error() string                 { return e.code }
func (e archiveStoreAPIError) ErrorCode() string             { return e.code }
func (e archiveStoreAPIError) ErrorMessage() string          { return e.code }
func (e archiveStoreAPIError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }
