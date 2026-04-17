// Package s3ext provides the storage.s3 capability for Pulp plugins,
// backed by AWS SDK v2. Compatible with Cloudflare R2 via R2-style
// virtual-host endpoints (path-style addressing, auto region).
//
// Plugin authors declare the capability in their manifest:
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
//	S3_BUCKET           — bucket name plugins operate against
//	S3_ENDPOINT         — optional; overrides the R2 endpoint for local
//	                      development (minio, localstack) or other S3-compat
//
// Host imports exposed (all msgpack request/response, returning an
// error code):
//
//	s3_put(req_ptr, req_len) → code
//	  req: {key, body}
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
//	s3_copy(req_ptr, req_len) → code
//	  req: {src_key, dst_key}
//
//	s3_delete(req_ptr, req_len) → code
//	  req: {key}
//
// Error codes: 0 ok, 1 empty input, 2 memory read failed, 3 decode
// failed, 4 S3 error (network, auth, not found, etc.), 5 encode failed,
// 7 alloc failed, 8 memory write failed, 10 missing required env vars.
package s3ext

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sync"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/aws"
	"github.com/aws/aws-sdk-go-v2/credentials"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	"github.com/tetratelabs/wazero"
	"github.com/tetratelabs/wazero/api"
	"github.com/vmihailenco/msgpack/v5"
)

func init() {
	ext.Register(ext.Capability{
		Name:     "storage.s3",
		Register: bindActive,
		Stub:     bindStub,
	})
}

// ---- client setup --------------------------------------------------------

var (
	clientMu sync.Mutex
	client   *s3.Client
	presig   *s3.PresignClient
	bucket   string
	initErr  error
)

func ensureClient() error {
	clientMu.Lock()
	defer clientMu.Unlock()
	if client != nil {
		return nil
	}
	if initErr != nil {
		return initErr
	}

	accountID := os.Getenv("S3_ACCOUNT_ID")
	accessKey := os.Getenv("S3_ACCESS_KEY_ID")
	secretKey := os.Getenv("S3_SECRET_ACCESS_KEY")
	bucket = os.Getenv("S3_BUCKET")
	endpoint := os.Getenv("S3_ENDPOINT")

	if accessKey == "" || secretKey == "" || bucket == "" {
		initErr = fmt.Errorf("s3: S3_ACCESS_KEY_ID, S3_SECRET_ACCESS_KEY, S3_BUCKET all required")
		return initErr
	}
	if endpoint == "" {
		if accountID == "" {
			initErr = fmt.Errorf("s3: set either S3_ENDPOINT or S3_ACCOUNT_ID")
			return initErr
		}
		endpoint = fmt.Sprintf("https://%s.r2.cloudflarestorage.com", accountID)
	}

	cfg := aws.Config{
		Region:      "auto",
		Credentials: credentials.NewStaticCredentialsProvider(accessKey, secretKey, ""),
	}
	client = s3.NewFromConfig(cfg, func(o *s3.Options) {
		o.BaseEndpoint = &endpoint
		o.UsePathStyle = true
	})
	presig = s3.NewPresignClient(client)
	return nil
}

// ---- host imports --------------------------------------------------------

type putRequest struct {
	Key  string `msgpack:"key"`
	Body []byte `msgpack:"body"`
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

type copyRequest struct {
	SrcKey string `msgpack:"src_key"`
	DstKey string `msgpack:"dst_key"`
}

type deleteRequest struct {
	Key string `msgpack:"key"`
}

func bindActive(b wazero.HostModuleBuilder, _ ext.Plugin) error {
	b.NewFunctionBuilder().WithFunc(s3Put).Export("s3_put")
	b.NewFunctionBuilder().WithFunc(s3Presign).Export("s3_presign")
	b.NewFunctionBuilder().WithFunc(s3PresignPut).Export("s3_presign_put")
	b.NewFunctionBuilder().WithFunc(s3Head).Export("s3_head")
	b.NewFunctionBuilder().WithFunc(s3Copy).Export("s3_copy")
	b.NewFunctionBuilder().WithFunc(s3Delete).Export("s3_delete")
	return nil
}

func bindStub(b wazero.HostModuleBuilder, _ ext.Plugin) error {
	nop4 := func(_ context.Context, _ api.Module, _, _, _, _ uint32) uint32 { return 99 }
	nop2 := func(_ context.Context, _ api.Module, _, _ uint32) uint32 { return 99 }
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_put")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_presign")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_presign_put")
	b.NewFunctionBuilder().WithFunc(nop4).Export("s3_head")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_copy")
	b.NewFunctionBuilder().WithFunc(nop2).Export("s3_delete")
	return nil
}

func s3Put(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	_, err := client.PutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &req.Key,
		Body:   bytes.NewReader(req.Body),
	})
	if err != nil {
		return 4
	}
	return 0
}

func s3PresignPut(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	ttl := time.Duration(req.TTLSec) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	psr, err := presig.PresignPutObject(ctx, &s3.PutObjectInput{
		Bucket: &bucket,
		Key:    &req.Key,
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, presignResponse{URL: psr.URL}, respPtrOut, respLenOut)
}

func s3Presign(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	ttl := time.Duration(req.TTLSec) * time.Second
	if ttl <= 0 {
		ttl = 15 * time.Minute
	}
	psr, err := presig.PresignGetObject(ctx, &s3.GetObjectInput{
		Bucket: &bucket,
		Key:    &req.Key,
	}, s3.WithPresignExpires(ttl))
	if err != nil {
		return 4
	}
	return writeMsgpackResponse(ctx, m, presignResponse{URL: psr.URL}, respPtrOut, respLenOut)
}

func s3Head(ctx context.Context, m api.Module, reqPtr, reqLen, respPtrOut, respLenOut uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	out, err := client.HeadObject(ctx, &s3.HeadObjectInput{
		Bucket: &bucket,
		Key:    &req.Key,
	})
	if err != nil {
		return 4
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

func s3Copy(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	copySource := bucket + "/" + req.SrcKey
	_, err := client.CopyObject(ctx, &s3.CopyObjectInput{
		Bucket:     &bucket,
		CopySource: &copySource,
		Key:        &req.DstKey,
	})
	if err != nil {
		return 4
	}
	return 0
}

func s3Delete(ctx context.Context, m api.Module, reqPtr, reqLen uint32) uint32 {
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
	if err := ensureClient(); err != nil {
		return 10
	}
	_, err := client.DeleteObject(ctx, &s3.DeleteObjectInput{
		Bucket: &bucket,
		Key:    &req.Key,
	})
	if err != nil {
		return 4
	}
	return 0
}

// writeMsgpackResponse encodes v and places the bytes in the plugin's
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
