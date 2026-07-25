// Package s3ext provides the storage.s3 capability for Pulp cells,
// backed by AWS SDK v2. Compatible with Cloudflare R2 via R2-style
// virtual-host endpoints (path-style addressing, auto region).
//
// Cell authors declare the capability in their manifest:
//
//	capabilities = ["storage.s3"]
//
// Deployments link the extension via blank import:
//
//	import _ "github.com/BananaLabs-OSS/Pulp-ext-s3"
//
// Configuration comes from environment variables:
//
//	S3_ACCOUNT_ID       — Cloudflare R2 account ID (used to build endpoint)
//	S3_ACCESS_KEY_ID    — access key
//	S3_SECRET_ACCESS_KEY — secret key
//	S3_BUCKET           — bucket name cells operate against
//	S3_ENDPOINT         — optional; overrides the R2 endpoint for local
//	                      development (minio, localstack) or other S3-compat
//
// Host imports exposed (all msgpack request/response, returning an
// error code):
//
//	s3_put(req_ptr, req_len) → code
//	  req: {key, body}
//	  Whole-body upload. Use only for small objects (< ~16 MB) — the
//	  body must fit in both cell and host memory simultaneously.
//
//	s3_put_sized(req_ptr, req_len) → code
//	  req: {key, body, content_length, content_type}
//	  Whole-body upload with ContentLength set explicitly on the
//	  PutObjectInput so the SDK issues a non-chunked, known-length
//	  PUT (avoids aws-chunked transfer-encoding). Mirrors
//	  r2.UploadSized — world archive uploads depend on this.
//
//	s3_put_multipart_init(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key, content_type}; resp: {upload_id}
//	  Begin a multipart upload. Pair with _part and _complete/_abort.
//	  Use for large objects (world archives, backups) — chunk size of
//	  8 MB is a reasonable default; min part size is 5 MB (S3 rule,
//	  R2 follows) except for the last part.
//
//	s3_put_multipart_part(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key, upload_id, part_number, data}; resp: {etag}
//	  Upload one chunk. part_number is 1-based, max 10 000.
//
//	s3_put_multipart_complete(req_ptr, req_len) → code
//	  req: {key, upload_id, parts:[{part_number, etag}]}
//	  Finalize — parts must be in ascending part_number order.
//
//	s3_put_multipart_abort(req_ptr, req_len) → code
//	  req: {key, upload_id}
//	  Cancel a multipart upload and discard any uploaded parts.
//
//	s3_presign(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key, ttl_sec}; resp: {url}  — GET URL for downloads
//
//	s3_presign_put(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key, ttl_sec}; resp: {url}  — PUT URL for direct uploads
//
//	s3_head(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key}; resp: {size, last_modified_unix}
//
//	s3_get(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {key}; resp: {body, content_type, content_length, etag}
//	  Whole-body fetch. Use only for small objects — the body must fit
//	  in both host and cell memory simultaneously.
//
//	s3_copy(req_ptr, req_len) → code
//	  req: {src_key, dst_key}
//
//	s3_delete(req_ptr, req_len) → code
//	  req: {key}
//
//	s3_list(req_ptr, req_len, resp_ptr_out, resp_len_out) → code
//	  req: {prefix, continuation_token, max_keys}
//	  resp: {entries:[{key,size,last_modified_unix}],
//	         next_continuation_token, is_truncated}
//
// Error codes: 0 ok, 1 empty input, 2 memory read failed, 3 decode
// failed, 4 S3 error (network, auth, other), 5 encode failed,
// 6 not found (NoSuchKey / NotFound), 7 alloc failed, 8 memory write
// failed, 10 missing required env vars, 11 access denied.
package s3ext

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"os"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

// classifyS3Error maps an SDK error to a host error code. Used by every
// S3 operation so cells can distinguish NotFound (6) and AccessDenied
// (11) from generic S3 failures (4). Put / PutSized / Copy on a missing
// source all benefit from the distinction.
func classifyS3Error(err error) uint32 {
	if err == nil {
		return 0
	}
	var nsk *s3types.NoSuchKey
	if errors.As(err, &nsk) {
		return 6
	}
	var nf *s3types.NotFound
	if errors.As(err, &nf) {
		return 6
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "NoSuchKey", "NotFound":
			return 6
		case "AccessDenied":
			return 11
		}
	}
	return 4
}

func init() {
	ext.Register(ext.Capability{
		Name:          "storage.s3",
		Setup:         setup,
		Register:      bindActive,
		Stub:          bindStub,
		Teardown:      teardown,
		TeardownScope: teardownScope,
	})
}

// ---- client setup --------------------------------------------------------

type s3ApplicationKey struct {
	applicationID string
	instanceID    string
}

type s3ClientState struct {
	mu      sync.Mutex
	client  *s3.Client
	presig  *s3.PresignClient
	bucket  string
	initErr error
	closed  bool
}

type s3Manager struct {
	mu      sync.Mutex
	clients map[s3ApplicationKey]*s3ClientState
}

func newS3Manager() *s3Manager {
	return &s3Manager{clients: map[s3ApplicationKey]*s3ClientState{}}
}

var manager = newS3Manager()

func applicationKey(scope ext.Scope) s3ApplicationKey {
	return s3ApplicationKey{applicationID: scope.ApplicationID(), instanceID: scope.ApplicationInstanceID()}
}

func setup(env ext.SetupEnv) error {
	scope := env.EffectiveScope()
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("storage.s3: invalid setup scope: %w", err)
	}
	_, err := manager.setup(scope)
	return err
}

func (m *s3Manager) setup(scope ext.Scope) (*s3ClientState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.clients[key]; state != nil {
		return state, nil
	}
	state := &s3ClientState{}
	m.clients[key] = state
	return state, nil
}

func (m *s3Manager) forScope(scope ext.Scope) (*s3ClientState, error) {
	if err := scope.Validate(); err != nil {
		return nil, err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	defer m.mu.Unlock()
	if state := m.clients[key]; state != nil {
		return state, nil
	}
	if !scope.IsLegacy() {
		return nil, fmt.Errorf("storage.s3: setup not called for application %s/%s", key.applicationID, key.instanceID)
	}
	state := &s3ClientState{}
	m.clients[key] = state
	return state, nil
}

func teardown(_ context.Context) error {
	return manager.teardown(ext.LegacyScope("default"))
}

func teardownScope(_ context.Context, scope ext.Scope) error {
	return manager.teardown(scope)
}

func (m *s3Manager) teardown(scope ext.Scope) error {
	if err := scope.Validate(); err != nil {
		return err
	}
	key := applicationKey(scope)
	m.mu.Lock()
	state := m.clients[key]
	delete(m.clients, key)
	m.mu.Unlock()
	if state == nil {
		return nil
	}
	state.close()
	return nil
}

func (s *s3ClientState) close() {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return
	}
	s.closed = true
	if s.client == nil {
		return
	}
	type idleCloser interface{ CloseIdleConnections() }
	if closer, ok := s.client.Options().HTTPClient.(idleCloser); ok {
		closer.CloseIdleConnections()
	}
}

func ensureClient() error {
	state, err := manager.forScope(ext.LegacyScope("default"))
	if err != nil {
		return err
	}
	return state.ensureClient()
}

func (s *s3ClientState) ensureClient() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("s3: client scope is closed")
	}
	if s.client != nil {
		return nil
	}
	if s.initErr != nil {
		return s.initErr
	}

	accountID := os.Getenv("S3_ACCOUNT_ID")
	if accountID == "" {
		accountID = os.Getenv("R2_ACCOUNT_ID")
	}
	accessKey := os.Getenv("S3_ACCESS_KEY_ID")
	if accessKey == "" {
		accessKey = os.Getenv("R2_ACCESS_KEY_ID")
	}
	secretKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	if secretKey == "" {
		secretKey = os.Getenv("R2_SECRET_ACCESS_KEY")
	}
	s.bucket = os.Getenv("S3_BUCKET")
	if s.bucket == "" {
		s.bucket = os.Getenv("R2_BUCKET")
	}
	endpoint := os.Getenv("S3_ENDPOINT")

	if accessKey == "" || secretKey == "" || s.bucket == "" {
		s.initErr = fmt.Errorf("s3: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET all required")
		return s.initErr
	}
	if endpoint == "" {
		if accountID == "" {
			s.initErr = fmt.Errorf("s3: set either S3_ENDPOINT or S3_ACCOUNT_ID")
			return s.initErr
		}
		endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
	}

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	s.client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
	s.presig = s3.NewPresignClient(s.client)
	return nil
}

// ---- host imports --------------------------------------------------------

type putRequest struct {
	Key  string `msgpack:"key"`
	Body []byte `msgpack:"body"`
}

type putSizedRequest struct {
	Key           string `msgpack:"key"`
	Body          []byte `msgpack:"body"`
	ContentLength int64  `msgpack:"content_length"`
	ContentType   string `msgpack:"content_type"`
}

type multipartInitRequest struct {
	Key         string `msgpack:"key"`
	ContentType string `msgpack:"content_type"`
}

type multipartInitResponse struct {
	UploadID string `msgpack:"upload_id"`
}

type multipartPartRequest struct {
	Key        string `msgpack:"key"`
	UploadID   string `msgpack:"upload_id"`
	PartNumber int32  `msgpack:"part_number"`
	Data       []byte `msgpack:"data"`
}

type multipartPartResponse struct {
	ETag string `msgpack:"etag"`
}

type multipartPart struct {
	PartNumber int32  `msgpack:"part_number"`
	ETag       string `msgpack:"etag"`
}

type multipartCompleteRequest struct {
	Key      string          `msgpack:"key"`
	UploadID string          `msgpack:"upload_id"`
	Parts    []multipartPart `msgpack:"parts"`
}

type multipartAbortRequest struct {
	Key      string `msgpack:"key"`
	UploadID string `msgpack:"upload_id"`
}

type presignRequest struct {
	Key    string `msgpack:"key"`
	TTLSec int64  `msgpack:"ttl_sec"`
}

type presignResponse struct {
	URL string `msgpack:"url"`
}

type headRequest struct {
	Key string `msgpack:"key"`
}

type headResponse struct {
	Size             int64 `msgpack:"size"`
	LastModifiedUnix int64 `msgpack:"last_modified_unix"`
}

type getRequest struct {
	Key string `msgpack:"key"`
}

type getResponse struct {
	Body          []byte `msgpack:"body"`
	ContentType   string `msgpack:"content_type"`
	ContentLength int64  `msgpack:"content_length"`
	ETag          string `msgpack:"etag"`
}

type copyRequest struct {
	SrcKey string `msgpack:"src_key"`
	DstKey string `msgpack:"dst_key"`
}

type deleteRequest struct {
	Key string `msgpack:"key"`
}

type listRequest struct {
	Prefix            string `msgpack:"prefix"`
	ContinuationToken string `msgpack:"continuation_token"`
	MaxKeys           int32  `msgpack:"max_keys"`
}

type listEntry struct {
	Key              string `msgpack:"key"`
	Size             int64  `msgpack:"size"`
	LastModifiedUnix int64  `msgpack:"last_modified_unix"`
}

type listResponse struct {
	Entries               []listEntry `msgpack:"entries"`
	NextContinuationToken string      `msgpack:"next_continuation_token"`
	IsTruncated           bool        `msgpack:"is_truncated"`
}

func bindActive(b wazero.HostModuleBuilder, cell ext.Cell) error {
	scope, err := ext.ValidatedScopeOf(cell)
	if err != nil {
		return fmt.Errorf("storage.s3: resolve cell scope: %w", err)
	}
	state, err := manager.forScope(scope)
	if err != nil {
		return err
	}
	nop2 := func(call func(context.Context, *s3ClientState, api.Module, uint32, uint32) uint32) func(context.Context, api.Module, uint32, uint32) uint32 {
		return func(ctx context.Context, module api.Module, p1, p2 uint32) uint32 {
			return call(ctx, state, module, p1, p2)
		}
	}
	nop4 := func(call func(context.Context, *s3ClientState, api.Module, uint32, uint32, uint32, uint32) uint32) func(context.Context, api.Module, uint32, uint32, uint32, uint32) uint32 {
		return func(ctx context.Context, module api.Module, p1, p2, p3, p4 uint32) uint32 {
			return call(ctx, state, module, p1, p2, p3, p4)
		}
	}
	b.NewFunctionBuilder().WithFunc(nop2(s3Put)).Export("s3_put")
	b.NewFunctionBuilder().WithFunc(nop2(s3PutSized)).Export("s3_put_sized")
	b.NewFunctionBuilder().WithFunc(nop4(s3PutMultipartInit)).Export("s3_put_multipart_init")
	b.NewFunctionBuilder().WithFunc(nop4(s3PutMultipartPart)).Export("s3_put_multipart_part")
	b.NewFunctionBuilder().WithFunc(nop2(s3PutMultipartComplete)).Export("s3_put_multipart_complete")
	b.NewFunctionBuilder().WithFunc(nop2(s3PutMultipartAbort)).Export("s3_put_multipart_abort")
	b.NewFunctionBuilder().WithFunc(nop4(s3Presign)).Export("s3_presign")
	b.NewFunctionBuilder().WithFunc(nop4(s3PresignPut)).Export("s3_presign_put")
	b.NewFunctionBuilder().WithFunc(nop4(s3Head)).Export("s3_head")
	b.NewFunctionBuilder().WithFunc(nop4(s3Get)).Export("s3_get")
	b.NewFunctionBuilder().WithFunc(nop2(s3Copy)).Export("s3_copy")
	b.NewFunctionBuilder().WithFunc(nop2(s3Delete)).Export("s3_delete")
	b.NewFunctionBuilder().WithFunc(nop4(s3List)).Export("s3_list")
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Cell) error {
	nop4 := func(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 { return 99 }
	nop2 := func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_put")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_put_sized")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_put_multipart_init")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_put_multipart_part")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_put_multipart_complete")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_put_multipart_abort")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_presign")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_presign_put")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_head")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_get")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_copy")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_delete")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_list")
	return nil
}

func s3Put(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req putRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	_, err := state.client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
		Body:   bytes.NewReader(req.Body),
	})
	if err != nil {
		return classifyS3Error(err)
	}
	return 0
}

// s3PutSized is like s3Put but sets ContentLength explicitly on the
// PutObjectInput so the SDK sends a non-chunked, known-length PUT.
// Mirrors r2.UploadSized — world archive uploads etc. depend on this
// to avoid aws-chunked transfer-encoding.
func s3PutSized(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req putSizedRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	size := req.ContentLength
	in := &s3.PutObjectInput{
		Bucket:        &state.bucket,
		Key:           &req.Key,
		Body:          bytes.NewReader(req.Body),
		ContentLength: &size,
	}
	if req.ContentType != "" {
		in.ContentType = &req.ContentType
	}
	if _, err := state.client.PutObject(ctx, in); err != nil {
		return classifyS3Error(err)
	}
	return 0
}

// s3PutMultipartInit begins a multipart upload. The returned upload_id
// must be passed to every subsequent part/complete/abort call for this
// key. No bytes are sent yet.
func s3PutMultipartInit(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req multipartInitRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	in := &s3.CreateMultipartUploadInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	}
	if req.ContentType != "" {
		in.ContentType = &req.ContentType
	}
	out, err := state.client.CreateMultipartUpload(ctx, in)
	if err != nil {
		return classifyS3Error(err)
	}
	resp := multipartInitResponse{}
	if out.UploadId != nil {
		resp.UploadID = *out.UploadId
	}
	return writeMsgpackResponse(ctx, m, resp, respPtrOut, respLenOut)
}

// s3PutMultipartPart uploads one chunk of a multipart upload. The cell
// sends part_number starting at 1; chunks other than the final one must
// be at least 5 MB (S3/R2 constraint). The returned etag must be passed
// back in _complete.
func s3PutMultipartPart(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req multipartPartRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	partNum := req.PartNumber
	out, err := state.client.UploadPart(ctx, &s3.UploadPartInput{
		Bucket:     &state.bucket,
		Key:        &req.Key,
		UploadId:   &req.UploadID,
		PartNumber: &partNum,
		Body:       bytes.NewReader(req.Data),
	})
	if err != nil {
		return classifyS3Error(err)
	}
	resp := multipartPartResponse{}
	if out.ETag != nil {
		resp.ETag = *out.ETag
	}
	return writeMsgpackResponse(ctx, m, resp, respPtrOut, respLenOut)
}

// s3PutMultipartComplete finalizes a multipart upload. Parts must be
// supplied in ascending part_number order with their etags. After this
// returns 0 the object is visible at key.
func s3PutMultipartComplete(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req multipartCompleteRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	parts := make([]s3types.CompletedPart, 0, len(req.Parts))
	for i := range req.Parts {
		p := req.Parts[i]
		partNum := p.PartNumber
		etag := p.ETag
		parts = append(parts, s3types.CompletedPart{
			PartNumber: &partNum,
			ETag:       &etag,
		})
	}
	_, err := state.client.CompleteMultipartUpload(ctx, &s3.CompleteMultipartUploadInput{
		Bucket:   &state.bucket,
		Key:      &req.Key,
		UploadId: &req.UploadID,
		MultipartUpload: &s3types.CompletedMultipartUpload{
			Parts: parts,
		},
	})
	if err != nil {
		return classifyS3Error(err)
	}
	return 0
}

// s3PutMultipartAbort cancels a multipart upload. Any uploaded parts are
// discarded by R2 (R2 otherwise bills for orphaned parts). Call this
// whenever _part fails partway so the cell can retry without leaking.
func s3PutMultipartAbort(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req multipartAbortRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	_, err := state.client.AbortMultipartUpload(ctx, &s3.AbortMultipartUploadInput{
		Bucket:   &state.bucket,
		Key:      &req.Key,
		UploadId: &req.UploadID,
	})
	if err != nil {
		return classifyS3Error(err)
	}
	return 0
}

func s3PresignPut(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req presignRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	ttl := time.Duration(req.TTLSec) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	psr, err := state.presig.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return classifyS3Error(err)
	}
	return writeMsgpackResponse(ctx, m, presignResponse{URL: psr.URL}, respPtrOut, respLenOut)
}

func s3Presign(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req presignRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	ttl := time.Duration(req.TTLSec) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	psr, err := state.presig.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return classifyS3Error(err)
	}
	return writeMsgpackResponse(ctx, m, presignResponse{URL: psr.URL}, respPtrOut, respLenOut)
}

func s3Head(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req headRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	out, err := state.client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	})
	if err != nil {
		return classifyS3Error(err)
	}
	resp := headResponse{}
	if out.ContentLength != nil {
		resp.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		resp.LastModifiedUnix = out.LastModified.Unix()
	}
	return writeMsgpackResponse(ctx, m, resp, respPtrOut, respLenOut)
}

// s3Get fetches an object's whole body. Use only for small objects —
// the body is held in host memory, msgpack-encoded, then copied into
// cell memory. For large downloads use s3_presign instead.
func s3Get(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req getRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	out, err := state.client.GetObject(ctx, &s3.GetObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	})
	if err != nil {
		return classifyS3Error(err)
	}
	defer out.Body.Close()
	body, err := io.ReadAll(out.Body)
	if err != nil {
		return 4
	}
	resp := getResponse{Body: body}
	if out.ContentType != nil {
		resp.ContentType = *out.ContentType
	}
	if out.ContentLength != nil {
		resp.ContentLength = *out.ContentLength
	}
	if out.ETag != nil {
		resp.ETag = *out.ETag
	}
	return writeMsgpackResponse(ctx, m, resp, respPtrOut, respLenOut)
}

func s3Copy(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req copyRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	copySource := state.bucket + "/" + url.PathEscape(req.SrcKey)
	_, err := state.client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &state.bucket,
		CopySource: &copySource,
		Key:        &req.DstKey,
	})
	if err != nil {
		return classifyS3Error(err)
	}
	return 0
}

func s3Delete(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req deleteRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	_, err := state.client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &state.bucket,
		Key:    &req.Key,
	})
	if err != nil {
		return classifyS3Error(err)
	}
	return 0
}

func s3List(ctx context.Context, state *s3ClientState, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
	if reqLen == 0 {
		return 1
	}
	data, ok := m.Memory().Read(reqPtr, reqLen)
	if !ok {
		return 2
	}
	var req listRequest
	if err := msgpack.Unmarshal(data, &req); err != nil {
		return 3
	}
	if err := state.ensureClient(); err != nil {
		return 10
	}
	in := &s3.ListObjectsV2Input{
		Bucket: &state.bucket,
	}
	if req.Prefix != "" {
		in.Prefix = &req.Prefix
	}
	if req.ContinuationToken != "" {
		in.ContinuationToken = &req.ContinuationToken
	}
	if req.MaxKeys > 0 {
		mk := req.MaxKeys
		in.MaxKeys = &mk
	}
	out, err := state.client.ListObjectsV2(ctx, in)
	if err != nil {
		return classifyS3Error(err)
	}
	resp := listResponse{
		Entries: make([]listEntry, 0, len(out.Contents)),
	}
	for _, obj := range out.Contents {
		e := listEntry{}
		if obj.Key != nil {
			e.Key = *obj.Key
		}
		if obj.Size != nil {
			e.Size = *obj.Size
		}
		if obj.LastModified != nil {
			e.LastModifiedUnix = obj.LastModified.Unix()
		}
		resp.Entries = append(resp.Entries, e)
	}
	if out.NextContinuationToken != nil {
		resp.NextContinuationToken = *out.NextContinuationToken
	}
	if out.IsTruncated != nil {
		resp.IsTruncated = *out.IsTruncated
	}
	return writeMsgpackResponse(ctx, m, resp, respPtrOut, respLenOut)
}

// writeMsgpackResponse encodes v and places the bytes in the cell's
// linear memory via pulp_alloc, storing (ptr, len) at the out addresses.
// Shared by any host import that returns structured data.
func writeMsgpackResponse(ctx context.Context, m api.Module, v any, respPtrOut, respLenOut uint32) uint32 {
	encoded, err := msgpack.Marshal(v)
	if err != nil {
		return 5
	}
	allocFn := m.ExportedFunction("pulp_alloc")
	if allocFn == nil {
		return 7
	}
	var ptr uint32
	if len(encoded) > 0 {
		res, err := allocFn.Call(ctx, uint64(len(encoded)))
		if err != nil || len(res) == 0 {
			return 7
		}
		ptr = uint32(res[0])
		if ptr == 0 {
			return 7
		}
		if !m.Memory().Write(ptr, encoded) {
			return 8
		}
	}
	if !m.Memory().WriteUint32Le(respPtrOut, ptr) {
		return 8
	}
	if !m.Memory().WriteUint32Le(respLenOut, uint32(len(encoded))) {
		return 8
	}
	return 0
}
