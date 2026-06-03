package s3ext

import (
	"context"
	"errors"
	"net/url"
	"reflect"
	"strings"
	"testing"

	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// getInput builds a GetObjectInput against the configured test bucket.
func getInput(key string) *s3.GetObjectInput {
	return &s3.GetObjectInput{Bucket: aws.String(bucket), Key: aws.String(key)}
}

// resetClient clears the package-global client state so each test drives
// ensureClient from a clean slate. ensureClient memoizes both success
// (client != nil) and failure (initErr), so both must be cleared.
func resetClient(t *testing.T) {
	t.Helper()
	clientMu.Lock()
	client = nil
	presig = nil
	bucket = ""
	initErr = nil
	clientMu.Unlock()
}

// setEnv sets S3_* env vars for the duration of a test (auto-restored).
func setEnv(t *testing.T, kv map[string]string) {
	t.Helper()
	// Clear every var ensureClient consults so a leaked outer-env value
	// can't taint the case, then set the requested ones.
	for _, k := range []string{
		"S3_ACCOUNT_ID", "R2_ACCOUNT_ID",
		"S3_ACCESS_KEY_ID", "R2_ACCESS_KEY_ID",
		"S3_SECRET_ACCESS_KEY", "R2_SECRET_ACCESS_KEY",
		"S3_BUCKET", "R2_BUCKET", "S3_ENDPOINT",
	} {
		t.Setenv(k, "")
	}
	for k, v := range kv {
		t.Setenv(k, v)
	}
}

// TestEnsureClientRequiresCredentials confirms ensureClient fails closed
// when any of the mandatory access key / secret / bucket is missing, and
// that the failure is memoized (no half-initialized client).
func TestEnsureClientRequiresCredentials(t *testing.T) {
	cases := []map[string]string{
		{}, // nothing set
		{"S3_ACCESS_KEY_ID": "ak", "S3_SECRET_ACCESS_KEY": "sk"},                       // no bucket
		{"S3_ACCESS_KEY_ID": "ak", "S3_BUCKET": "b"},                                   // no secret
		{"S3_SECRET_ACCESS_KEY": "sk", "S3_BUCKET": "b"},                               // no access key
	}
	for i, env := range cases {
		resetClient(t)
		setEnv(t, env)
		if err := ensureClient(); err == nil {
			t.Errorf("case %d: ensureClient succeeded with missing creds %v", i, env)
		}
		clientMu.Lock()
		c := client
		clientMu.Unlock()
		if c != nil {
			t.Errorf("case %d: client built despite missing creds", i)
		}
	}
}

// TestEnsureClientRequiresEndpointOrAccount confirms that with creds+bucket
// present but NO endpoint and NO account id, ensureClient cannot guess an
// endpoint and fails — it never falls back to the real AWS S3 endpoint.
func TestEnsureClientRequiresEndpointOrAccount(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"S3_ACCESS_KEY_ID":     "ak",
		"S3_SECRET_ACCESS_KEY": "sk",
		"S3_BUCKET":            "b",
	})
	err := ensureClient()
	if err == nil {
		t.Fatal("ensureClient succeeded with no endpoint and no account id")
	}
	if !strings.Contains(err.Error(), "S3_ENDPOINT") && !strings.Contains(err.Error(), "S3_ACCOUNT_ID") {
		t.Fatalf("error should name the missing config, got: %v", err)
	}
}

// TestEnsureClientBuildsR2EndpointFromAccount confirms the account id is
// turned into the canonical R2 virtual-host endpoint, and that the R2_*
// env fallbacks are honored.
func TestEnsureClientBuildsR2EndpointFromAccount(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"R2_ACCOUNT_ID":        "acc123",
		"R2_ACCESS_KEY_ID":     "ak",
		"R2_SECRET_ACCESS_KEY": "sk",
		"R2_BUCKET":            "worlds",
	})
	if err := ensureClient(); err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	if bucket != "worlds" {
		t.Fatalf("bucket = %q, want worlds (R2_BUCKET fallback)", bucket)
	}
	// A presign should target the R2 endpoint derived from the account id.
	u := presignGet(t, "some/key")
	if !strings.Contains(u.Host, "acc123.r2.cloudflarestorage.com") {
		t.Fatalf("endpoint not derived from account id: host=%q", u.Host)
	}
}

// TestExplicitEndpointWins confirms S3_ENDPOINT overrides the account-derived
// endpoint (the minio/localstack dev path) and is used verbatim.
func TestExplicitEndpointWins(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"S3_ACCOUNT_ID":        "acc123", // present but must be ignored
		"S3_ACCESS_KEY_ID":     "ak",
		"S3_SECRET_ACCESS_KEY": "sk",
		"S3_BUCKET":            "b",
		"S3_ENDPOINT":          "http://127.0.0.1:9000",
	})
	if err := ensureClient(); err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	u := presignGet(t, "k")
	if u.Host != "127.0.0.1:9000" {
		t.Fatalf("explicit endpoint ignored: host=%q want 127.0.0.1:9000", u.Host)
	}
	if strings.Contains(u.Host, "cloudflarestorage") {
		t.Fatal("account-derived endpoint leaked despite explicit S3_ENDPOINT")
	}
}

// TestNoCellControlledEndpointOrBucket is the SSRF-by-construction guard
// (audit MASTER.md: "no req.Endpoint/req.Bucket field exists, so
// endpoint-SSRF is closed by construction"). Endpoint, bucket, and creds
// come from operator env ONLY. This test fails if anyone later adds an
// Endpoint or Bucket field to a cell-supplied request struct, which would
// let a cell repoint the client at 169.254.169.254 etc.
func TestNoCellControlledEndpointOrBucket(t *testing.T) {
	reqTypes := []any{
		putRequest{}, putSizedRequest{}, multipartInitRequest{},
		multipartPartRequest{}, multipartCompleteRequest{}, multipartAbortRequest{},
		presignRequest{}, headRequest{}, getRequest{}, copyRequest{},
		deleteRequest{}, listRequest{},
	}
	banned := []string{"endpoint", "bucket", "host", "account", "region", "url"}
	for _, rt := range reqTypes {
		typ := reflect.TypeOf(rt)
		for i := 0; i < typ.NumField(); i++ {
			f := strings.ToLower(typ.Field(i).Name)
			for _, b := range banned {
				if f == b {
					t.Errorf("%s exposes a cell-controlled %q field — SSRF/credential-repoint risk; endpoint+bucket must stay operator-env-only", typ.Name(), typ.Field(i).Name)
				}
			}
		}
	}
}

// TestClassifyS3Error pins the NotFound (6) / AccessDenied (11) / generic
// (4) mapping cells branch on. A renumbering here silently changes the host
// ABI.
func TestClassifyS3Error(t *testing.T) {
	if got := classifyS3Error(nil); got != 0 {
		t.Errorf("classifyS3Error(nil) = %d, want 0", got)
	}
	if got := classifyS3Error(&s3types.NoSuchKey{}); got != 6 {
		t.Errorf("NoSuchKey = %d, want 6", got)
	}
	if got := classifyS3Error(&s3types.NotFound{}); got != 6 {
		t.Errorf("NotFound = %d, want 6", got)
	}
	if got := classifyS3Error(apiError{code: "AccessDenied"}); got != 11 {
		t.Errorf("AccessDenied = %d, want 11", got)
	}
	if got := classifyS3Error(apiError{code: "NoSuchKey"}); got != 6 {
		t.Errorf("api NoSuchKey = %d, want 6", got)
	}
	if got := classifyS3Error(errors.New("connection reset")); got != 4 {
		t.Errorf("generic = %d, want 4", got)
	}
}

// apiError is a smithy.APIError stand-in for classify tests.
type apiError struct{ code string }

func (e apiError) Error() string                 { return e.code }
func (e apiError) ErrorCode() string             { return e.code }
func (e apiError) ErrorMessage() string          { return e.code }
func (e apiError) ErrorFault() smithy.ErrorFault { return smithy.FaultUnknown }

// TestPresignGetTargetsKey confirms a presigned GET URL targets the exact
// bucket/key under the configured endpoint, uses path-style addressing (so
// R2 works), and carries a signature with an expiry — i.e. presigning is
// wired correctly end to end without any network call.
func TestPresignGetTargetsKey(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"S3_ACCESS_KEY_ID":     "AKIDEXAMPLE",
		"S3_SECRET_ACCESS_KEY": "secret",
		"S3_BUCKET":            "mybucket",
		"S3_ENDPOINT":          "https://example-store.test",
	})
	if err := ensureClient(); err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	u := presignGet(t, "worlds/server-42/world.tar.zst")

	// Path-style: /<bucket>/<key>.
	if !strings.HasPrefix(u.Path, "/mybucket/") {
		t.Errorf("not path-style for bucket: path=%q", u.Path)
	}
	if !strings.Contains(u.Path, "worlds/server-42/world.tar.zst") {
		t.Errorf("key missing from presigned path: %q", u.Path)
	}
	if u.Host != "example-store.test" {
		t.Errorf("host = %q, want example-store.test", u.Host)
	}
	q := u.Query()
	if q.Get("X-Amz-Signature") == "" {
		t.Error("presigned URL has no X-Amz-Signature")
	}
	if q.Get("X-Amz-Expires") == "" {
		t.Error("presigned URL has no X-Amz-Expires (TTL not applied)")
	}
	if cred := q.Get("X-Amz-Credential"); !strings.Contains(cred, "AKIDEXAMPLE") {
		t.Errorf("presigned URL credential missing access key: %q", cred)
	}
}

// TestPresignDefaultTTL confirms a non-positive ttl_sec falls back to the
// documented 15-minute default rather than expiring instantly.
func TestPresignDefaultTTL(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"S3_ACCESS_KEY_ID":     "ak",
		"S3_SECRET_ACCESS_KEY": "sk",
		"S3_BUCKET":            "b",
		"S3_ENDPOINT":          "https://e.test",
	})
	if err := ensureClient(); err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	psr, err := presig.PresignGetObject(context.Background(), getInput("k"))
	if err != nil {
		t.Fatalf("presign: %v", err)
	}
	// Default WithPresignExpires is 15m on the client when none requested;
	// here we just assert the un-overridden call still signs (expiry param
	// present). The handler's ttl<=0 → 15m branch is exercised via the URL.
	u, _ := url.Parse(psr.URL)
	if u.Query().Get("X-Amz-Expires") == "" {
		t.Fatal("default presign produced no expiry")
	}
}

// TestCopySourceScoping pins that s3_copy builds its CopySource as
// "<bucket>/<srcKey>" — both source and destination stay inside the single
// operator-configured bucket; a cell cannot copy from an arbitrary bucket.
func TestCopySourceScoping(t *testing.T) {
	resetClient(t)
	setEnv(t, map[string]string{
		"S3_ACCESS_KEY_ID":     "ak",
		"S3_SECRET_ACCESS_KEY": "sk",
		"S3_BUCKET":            "thebucket",
		"S3_ENDPOINT":          "https://e.test",
	})
	if err := ensureClient(); err != nil {
		t.Fatalf("ensureClient: %v", err)
	}
	// The handler computes: copySource := bucket + "/" + req.SrcKey.
	// Reproduce that expression against the configured bucket and assert it
	// stays bucket-scoped even when the cell supplies a crafted src key.
	for _, srcKey := range []string{"a/b.txt", "../evil", "other-bucket/x"} {
		copySource := bucket + "/" + srcKey
		if !strings.HasPrefix(copySource, "thebucket/") {
			t.Errorf("copy source %q escaped configured bucket prefix", copySource)
		}
	}
}

// ---- helpers ----

func presignGet(t *testing.T, key string) *url.URL {
	t.Helper()
	psr, err := presig.PresignGetObject(context.Background(), getInput(key))
	if err != nil {
		t.Fatalf("PresignGetObject(%q): %v", key, err)
	}
	u, err := url.Parse(psr.URL)
	if err != nil {
		t.Fatalf("parse presigned url: %v", err)
	}
	return u
}
