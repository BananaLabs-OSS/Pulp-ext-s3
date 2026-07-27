package s3ext

import (
	"archive/zip"
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"io"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"

	"github.com/BananaLabs-OSS/Fiber/pulp/effect"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/vmihailenco/msgpack/v5"
)

func largeExactObjectCommand(body []byte) effect.StorageLargeExactObjectValidationCommandV1 {
	sum := sha256.Sum256(body)
	return effect.StorageLargeExactObjectValidationCommandV1{
		ContractVersion: effect.StorageLargeExactObjectValidationContractV1,
		Claim: effect.StorageLargeExactObjectValidationClaimV1{
			Version: effect.StorageLargeExactObjectValidationClaimVersionV1,
			ID:      "claim-1",
			Digest:  strings.Repeat("a", 64),
		},
		ObjectNamespace:    "application-uploads.v1",
		ObjectKey:          "uploads/claim-1/archive.zip",
		ExpectedGeneration: "etag-1",
		ContentLength:      int64(len(body)),
		ExpectedSHA256:     hex.EncodeToString(sum[:]),
		ValidatorPolicy:    "application.archive.v1",
		Limits:             effect.StorageLargeExactObjectValidationFixedLimitsV1(),
	}
}

func largeExactObjectState(t *testing.T, body []byte, command effect.StorageLargeExactObjectValidationCommandV1) *artifactValidationState {
	t.Helper()
	setPublicUploadEnv(t)
	state := &artifactValidationState{client: &s3ClientState{}}
	state.headObject = func(_ context.Context, _ *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		if aws.ToString(input.IfMatch) != `"etag-1"` || aws.ToString(input.Key) != command.ObjectKey {
			t.Fatalf("HEAD fence = %#v", input)
		}
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(int64(len(body))),
			ETag:          aws.String(`"etag-1"`),
		}, nil
	}
	state.getObject = func(_ context.Context, _ *s3ClientState, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
		if aws.ToString(input.IfMatch) != `"etag-1"` || aws.ToString(input.Key) != command.ObjectKey {
			t.Fatalf("GET fence = %#v", input)
		}
		return &s3.GetObjectOutput{
			Body:          io.NopCloser(bytes.NewReader(body)),
			ContentLength: aws.Int64(int64(len(body))),
			ETag:          aws.String(`"etag-1"`),
		}, nil
	}
	return state
}

func TestLargeExactObjectValidationStreamsAndReturnsTerminalObjectRef(t *testing.T) {
	body := testArtifactZip(t, map[string]string{
		"level.dat":        "bounded content",
		"region/r.0.0.mca": "region",
	})
	command := largeExactObjectCommand(body)
	state := largeExactObjectState(t, body, command)
	var heads atomic.Int32
	head := state.headObject
	state.headObject = func(ctx context.Context, client *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		heads.Add(1)
		return head(ctx, client, input)
	}
	result, err := state.validateLargeExactObject(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if heads.Load() != 2 {
		t.Fatalf("HEAD calls = %d, want 2", heads.Load())
	}
	if !result.Valid || result.ErrorCode != "" || result.Object == nil ||
		result.Object.Key != command.ObjectKey || result.Object.SHA256 != command.ExpectedSHA256 ||
		result.Object.SizeBytes != command.ContentLength {
		t.Fatalf("terminal result = %#v", result)
	}
	if err := result.Validate(); err != nil {
		t.Fatalf("terminal result contract: %v", err)
	}
}

func TestLargeExactObjectValidationFencesDigestAndGeneration(t *testing.T) {
	body := testArtifactZip(t, map[string]string{"file": "content"})
	command := largeExactObjectCommand(body)
	state := largeExactObjectState(t, body, command)
	command.ExpectedSHA256 = strings.Repeat("f", 64)
	result, err := state.validateLargeExactObject(context.Background(), command)
	if err != nil {
		t.Fatal(err)
	}
	if result.Valid || result.ErrorCode != "sha256_mismatch" || result.Object != nil {
		t.Fatalf("digest mismatch result = %#v", result)
	}

	command = largeExactObjectCommand(body)
	state = largeExactObjectState(t, body, command)
	var calls atomic.Int32
	state.headObject = func(_ context.Context, _ *s3ClientState, _ *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
		generation := `"etag-1"`
		if calls.Add(1) == 2 {
			generation = `"etag-replaced"`
		}
		return &s3.HeadObjectOutput{
			ContentLength: aws.Int64(command.ContentLength),
			ETag:          aws.String(generation),
		}, nil
	}
	if _, err := state.validateLargeExactObject(context.Background(), command); !errorsIsGenerationMismatch(err) {
		t.Fatalf("generation drift error = %v", err)
	}
}

func TestLargeExactObjectValidationRejectsUnsafeAndBombLikeZIPs(t *testing.T) {
	unsafe := testArtifactZip(t, map[string]string{`..\escape`: "x"})
	if err := validateLargeZIPBytes(t, unsafe); err == nil || err.Error() != "zip_path" {
		t.Fatalf("unsafe path error = %v", err)
	}

	var duplicate bytes.Buffer
	writer := zip.NewWriter(&duplicate)
	for index := 0; index < 2; index++ {
		entry, err := writer.Create("same")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := io.WriteString(entry, "x"); err != nil {
			t.Fatal(err)
		}
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	if err := validateLargeZIPBytes(t, duplicate.Bytes()); err == nil || err.Error() != "zip_duplicate_path" {
		t.Fatalf("duplicate path error = %v", err)
	}

	var bomb bytes.Buffer
	bombWriter := zip.NewWriter(&bomb)
	header := &zip.FileHeader{Name: "bomb"}
	header.Method = zip.Deflate
	entry, err := bombWriter.CreateHeader(header)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := io.Copy(entry, io.LimitReader(zeroReader{}, 32<<20)); err != nil {
		t.Fatal(err)
	}
	if err := bombWriter.Close(); err != nil {
		t.Fatal(err)
	}
	if err := validateLargeZIPBytes(t, bomb.Bytes()); err == nil || err.Error() != "zip_compression_ratio" {
		t.Fatalf("bomb ratio error = %v", err)
	}
}

func TestLargeExactObjectValidationConcurrentReplayIsStable(t *testing.T) {
	body := testArtifactZip(t, map[string]string{"file": "content"})
	command := largeExactObjectCommand(body)
	state := largeExactObjectState(t, body, command)
	var wait sync.WaitGroup
	failures := make(chan error, 32)
	for index := 0; index < 32; index++ {
		wait.Add(1)
		go func() {
			defer wait.Done()
			result, err := state.validateLargeExactObject(context.Background(), command)
			if err != nil {
				failures <- err
				return
			}
			if !result.Valid || result.Object == nil || result.Claim != command.Claim {
				failures <- errArtifactInvalid
			}
		}()
	}
	wait.Wait()
	close(failures)
	for err := range failures {
		t.Error(err)
	}
}

func TestLargeExactObjectWireDispatchIsStrict(t *testing.T) {
	body := testArtifactZip(t, map[string]string{"file": "content"})
	command := largeExactObjectCommand(body)
	state := largeExactObjectState(t, body, command)
	wire, err := msgpack.Marshal(command)
	if err != nil {
		t.Fatal(err)
	}
	response, code := artifactValidationWire(context.Background(), state, wire)
	if code != artifactValidationCodeOK {
		t.Fatalf("wire code = %d", code)
	}
	var result effect.StorageLargeExactObjectValidationResultV1
	if err := msgpack.Unmarshal(response, &result); err != nil || !result.Valid {
		t.Fatalf("wire result = %#v, %v", result, err)
	}
	var altered map[string]any
	if err := msgpack.Unmarshal(wire, &altered); err != nil {
		t.Fatal(err)
	}
	altered["bucket"] = "caller-bucket"
	unknown, err := msgpack.Marshal(altered)
	if err != nil {
		t.Fatal(err)
	}
	if _, code := artifactValidationWire(context.Background(), state, unknown); code != artifactValidationCodeDecode {
		t.Fatalf("unknown authority code = %d", code)
	}
}

func TestLargeExactObjectHostSourceIsApplicationNeutral(t *testing.T) {
	source, err := os.ReadFile(filepath.Join("large_exact_object_validation.go"))
	if err != nil {
		t.Fatal(err)
	}
	lower := strings.ToLower(string(source))
	for _, forbidden := range []string{"minecraft", "evolution", "world-archive", "sessions"} {
		if strings.Contains(lower, forbidden) {
			t.Fatalf("generic host contains application vocabulary %q", forbidden)
		}
	}
}

func FuzzLargeExactObjectZIPStructure(f *testing.F) {
	f.Add([]byte("not a zip"))
	var valid bytes.Buffer
	writer := zip.NewWriter(&valid)
	entry, _ := writer.Create("file")
	_, _ = io.WriteString(entry, "content")
	_ = writer.Close()
	f.Add(valid.Bytes())
	f.Fuzz(func(t *testing.T, wire []byte) {
		file, err := os.CreateTemp(t.TempDir(), "archive-*.zip")
		if err != nil {
			t.Fatal(err)
		}
		if _, err := file.Write(wire); err != nil {
			t.Fatal(err)
		}
		limits := effect.StorageLargeExactObjectValidationFixedLimitsV1()
		_ = validateLargeExactObjectZIP(file, int64(len(wire)), limits)
		_ = file.Close()
	})
}

type zeroReader struct{}

func (zeroReader) Read(value []byte) (int, error) {
	for index := range value {
		value[index] = 0
	}
	return len(value), nil
}

func validateLargeZIPBytes(t *testing.T, body []byte) error {
	t.Helper()
	file, err := os.CreateTemp(t.TempDir(), "archive-*.zip")
	if err != nil {
		t.Fatal(err)
	}
	defer file.Close()
	if _, err := file.Write(body); err != nil {
		t.Fatal(err)
	}
	return validateLargeExactObjectZIP(file, int64(len(body)), effect.StorageLargeExactObjectValidationFixedLimitsV1())
}

func errorsIsGenerationMismatch(err error) bool {
	return err == errArtifactGenerationMismatch
}
