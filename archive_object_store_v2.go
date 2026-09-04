package s3ext

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/service/s3"
	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
	"github.com/aws/smithy-go"
)

// ErrObjectGenerationMismatch means a conditional exact-object mutation lost
// its generation fence. Callers must re-plan from owner state; silently
// retrying the same mutation against a newer object would overwrite data.
var ErrObjectGenerationMismatch = errors.New("s3: object generation mismatch")

// ObjectGenerationAbsent is the explicit owner fence for an object that must
// not exist. All other generation values are treated as exact ETags.
const ObjectGenerationAbsent = "absent"

// ExactObject describes one exact object. Generation is its normalized ETag
// (without HTTP quotes), which is also the value accepted by the conditional
// mutation methods below.
type ExactObject struct {
	Key          string
	Generation   string
	Size         int64
	ContentType  string
	Metadata     map[string]string
	LastModified time.Time
}

type ExactObjectBody struct {
	ExactObject
	Body []byte
}

type ExactObjectWrite struct {
	Key                string
	Body               io.Reader
	ContentLength      int64
	ContentType        string
	ContentEncoding    string
	ExpectedGeneration string
	Metadata           map[string]string
}

type ExactObjectCopy struct {
	SourceKey             string
	SourceGeneration      string
	DestinationKey        string
	DestinationGeneration string
	ContentType           string
	ContentEncoding       string
	Metadata              map[string]string
}

type ExactObjectDeleteResult struct {
	AlreadyAbsent bool
}

// ScopedArchiveObjectStoreV2 is the host-only, application-scoped object
// surface used by archive-owner/v2 executors. It deliberately has no list or
// prefix-delete operation. Every mutation is against one validated key and is
// protected by either an exact ETag or the explicit "absent" fence.
type ScopedArchiveObjectStoreV2 struct {
	headObject   func(context.Context, *s3ClientState, *s3.HeadObjectInput) (*s3.HeadObjectOutput, error)
	getObject    func(context.Context, *s3ClientState, *s3.GetObjectInput) (*s3.GetObjectOutput, error)
	putObject    func(context.Context, *s3ClientState, *s3.PutObjectInput) (*s3.PutObjectOutput, error)
	copyObject   func(context.Context, *s3ClientState, *s3.CopyObjectInput) (*s3.CopyObjectOutput, error)
	deleteObject func(context.Context, *s3ClientState, *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error)
	presignGet   func(context.Context, *s3ClientState, *s3.GetObjectInput, time.Duration) (string, error)
}

func NewScopedArchiveObjectStoreV2() *ScopedArchiveObjectStoreV2 {
	return &ScopedArchiveObjectStoreV2{}
}

func (s *ScopedArchiveObjectStoreV2) HeadExactObject(ctx context.Context, scope ext.Scope, key string) (ExactObject, error) {
	if s == nil {
		return ExactObject{}, errors.New("s3: scoped archive object store is required")
	}
	state, err := archiveStateForScope(scope, key)
	if err != nil {
		return ExactObject{}, err
	}
	input := &s3.HeadObjectInput{Bucket: &state.bucket, Key: &key}
	call := s.headObject
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.HeadObjectInput) (*s3.HeadObjectOutput, error) {
			return state.client.HeadObject(ctx, input)
		}
	}
	out, err := call(ctx, state, input)
	if err != nil {
		return ExactObject{}, archiveObjectError("head", key, err)
	}
	return exactObjectFromHead(key, out)
}

// ReadExactObject reads one bounded object. It is intended for small durable
// host receipts and database envelopes, never multi-gigabyte world archives.
func (s *ScopedArchiveObjectStoreV2) ReadExactObject(ctx context.Context, scope ext.Scope, key string, maxBytes int64) (ExactObjectBody, error) {
	if s == nil {
		return ExactObjectBody{}, errors.New("s3: scoped archive object store is required")
	}
	if maxBytes <= 0 {
		return ExactObjectBody{}, errors.New("s3: positive exact-object read limit is required")
	}
	state, err := archiveStateForScope(scope, key)
	if err != nil {
		return ExactObjectBody{}, err
	}
	call := s.getObject
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.GetObjectInput) (*s3.GetObjectOutput, error) {
			return state.client.GetObject(ctx, input)
		}
	}
	out, err := call(ctx, state, &s3.GetObjectInput{Bucket: &state.bucket, Key: &key})
	if err != nil {
		return ExactObjectBody{}, archiveObjectError("read", key, err)
	}
	if out.Body == nil {
		return ExactObjectBody{}, fmt.Errorf("s3: read %q returned no body", key)
	}
	defer out.Body.Close()
	if out.ContentLength != nil && *out.ContentLength > maxBytes {
		return ExactObjectBody{}, fmt.Errorf("s3: read %q exceeds %d bytes", key, maxBytes)
	}
	body, err := io.ReadAll(io.LimitReader(out.Body, maxBytes+1))
	if err != nil {
		return ExactObjectBody{}, fmt.Errorf("s3: read %q: %w", key, err)
	}
	if int64(len(body)) > maxBytes {
		return ExactObjectBody{}, fmt.Errorf("s3: read %q exceeds %d bytes", key, maxBytes)
	}
	object := ExactObject{
		Key: key, Generation: normalizeETag(out.ETag), Size: int64(len(body)),
		ContentType: stringValue(out.ContentType), Metadata: cloneMetadata(out.Metadata),
	}
	if out.ContentLength != nil {
		object.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		object.LastModified = out.LastModified.UTC()
	}
	if object.Generation == "" {
		return ExactObjectBody{}, fmt.Errorf("s3: read %q returned no generation", key)
	}
	return ExactObjectBody{ExactObject: object, Body: body}, nil
}

func (s *ScopedArchiveObjectStoreV2) WriteExactObject(ctx context.Context, scope ext.Scope, req ExactObjectWrite) (ExactObject, error) {
	if s == nil {
		return ExactObject{}, errors.New("s3: scoped archive object store is required")
	}
	if req.Body == nil {
		return ExactObject{}, errors.New("s3: exact-object write body is required")
	}
	if err := validateExpectedGeneration(req.ExpectedGeneration); err != nil {
		return ExactObject{}, err
	}
	if err := validateArchiveMetadata(req.Metadata); err != nil {
		return ExactObject{}, err
	}
	state, err := archiveStateForScope(scope, req.Key)
	if err != nil {
		return ExactObject{}, err
	}
	input := &s3.PutObjectInput{
		Bucket: &state.bucket, Key: &req.Key, Body: req.Body,
		Metadata: cloneMetadata(req.Metadata),
	}
	if req.ContentLength >= 0 {
		input.ContentLength = &req.ContentLength
	}
	if req.ContentType != "" {
		input.ContentType = &req.ContentType
	}
	if req.ContentEncoding != "" {
		input.ContentEncoding = &req.ContentEncoding
	}
	applyWriteFence(req.ExpectedGeneration, &input.IfMatch, &input.IfNoneMatch)
	call := s.putObject
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.PutObjectInput) (*s3.PutObjectOutput, error) {
			return state.client.PutObject(ctx, input)
		}
	}
	out, err := call(ctx, state, input)
	if err != nil {
		return ExactObject{}, archiveObjectError("write", req.Key, err)
	}
	object := ExactObject{
		Key: req.Key, Generation: normalizeETag(out.ETag), Size: req.ContentLength,
		ContentType: req.ContentType, Metadata: cloneMetadata(req.Metadata),
	}
	if object.Generation == "" {
		return s.HeadExactObject(ctx, scope, req.Key)
	}
	return object, nil
}

func (s *ScopedArchiveObjectStoreV2) CopyExactObject(ctx context.Context, scope ext.Scope, req ExactObjectCopy) (ExactObject, error) {
	if s == nil {
		return ExactObject{}, errors.New("s3: scoped archive object store is required")
	}
	if !safeExactObjectKey(req.SourceKey) || !safeExactObjectKey(req.DestinationKey) {
		return ExactObject{}, errors.New("s3: exact safe copy source and destination keys are required")
	}
	if req.SourceGeneration == "" || req.SourceGeneration == ObjectGenerationAbsent {
		return ExactObject{}, errors.New("s3: exact copy source generation is required")
	}
	if err := validateExpectedGeneration(req.DestinationGeneration); err != nil {
		return ExactObject{}, err
	}
	if err := validateArchiveMetadata(req.Metadata); err != nil {
		return ExactObject{}, err
	}
	state, err := archiveStateForScope(scope, req.DestinationKey)
	if err != nil {
		return ExactObject{}, err
	}
	copySource := state.bucket + "/" + url.PathEscape(req.SourceKey)
	input := &s3.CopyObjectInput{
		Bucket: &state.bucket, Key: &req.DestinationKey, CopySource: &copySource,
		CopySourceIfMatch: quotedETag(req.SourceGeneration),
		Metadata:          cloneMetadata(req.Metadata), MetadataDirective: s3types.MetadataDirectiveReplace,
	}
	if req.ContentType != "" {
		input.ContentType = &req.ContentType
	}
	if req.ContentEncoding != "" {
		input.ContentEncoding = &req.ContentEncoding
	}
	applyWriteFence(req.DestinationGeneration, &input.IfMatch, &input.IfNoneMatch)
	call := s.copyObject
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.CopyObjectInput) (*s3.CopyObjectOutput, error) {
			return state.client.CopyObject(ctx, input)
		}
	}
	out, err := call(ctx, state, input)
	if err != nil {
		return ExactObject{}, archiveObjectError("copy", req.DestinationKey, err)
	}
	generation := ""
	if out.CopyObjectResult != nil {
		generation = normalizeETag(out.CopyObjectResult.ETag)
	}
	object := ExactObject{
		Key: req.DestinationKey, Generation: generation,
		ContentType: req.ContentType, Metadata: cloneMetadata(req.Metadata),
	}
	if object.Generation == "" {
		return s.HeadExactObject(ctx, scope, req.DestinationKey)
	}
	return object, nil
}

func (s *ScopedArchiveObjectStoreV2) DeleteExactObject(ctx context.Context, scope ext.Scope, key, expectedGeneration string) (ExactObjectDeleteResult, error) {
	if s == nil {
		return ExactObjectDeleteResult{}, errors.New("s3: scoped archive object store is required")
	}
	if expectedGeneration == "" || expectedGeneration == ObjectGenerationAbsent {
		return ExactObjectDeleteResult{}, errors.New("s3: exact delete generation is required")
	}
	state, err := archiveStateForScope(scope, key)
	if err != nil {
		return ExactObjectDeleteResult{}, err
	}
	current, err := s.HeadExactObject(ctx, scope, key)
	if errors.Is(err, ErrObjectNotFound) {
		return ExactObjectDeleteResult{AlreadyAbsent: true}, nil
	}
	if err != nil {
		return ExactObjectDeleteResult{}, err
	}
	if current.Generation != normalizeETagValue(expectedGeneration) {
		return ExactObjectDeleteResult{}, fmt.Errorf("%w: delete %q", ErrObjectGenerationMismatch, key)
	}
	input := &s3.DeleteObjectInput{
		Bucket: &state.bucket, Key: &key, IfMatch: quotedETag(expectedGeneration),
	}
	call := s.deleteObject
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.DeleteObjectInput) (*s3.DeleteObjectOutput, error) {
			return state.client.DeleteObject(ctx, input)
		}
	}
	if _, err := call(ctx, state, input); err != nil {
		if errors.Is(archiveObjectError("delete", key, err), ErrObjectNotFound) {
			return ExactObjectDeleteResult{AlreadyAbsent: true}, nil
		}
		return ExactObjectDeleteResult{}, archiveObjectError("delete", key, err)
	}
	return ExactObjectDeleteResult{}, nil
}

func (s *ScopedArchiveObjectStoreV2) PresignExactGet(ctx context.Context, scope ext.Scope, key string, ttl time.Duration) (string, error) {
	if s == nil {
		return "", errors.New("s3: scoped archive object store is required")
	}
	if ttl <= 0 || ttl > 7*24*time.Hour {
		return "", errors.New("s3: exact presign TTL must be between zero and seven days")
	}
	state, err := archiveStateForScope(scope, key)
	if err != nil {
		return "", err
	}
	input := &s3.GetObjectInput{Bucket: &state.bucket, Key: &key}
	call := s.presignGet
	if call == nil {
		call = func(ctx context.Context, state *s3ClientState, input *s3.GetObjectInput, ttl time.Duration) (string, error) {
			out, err := state.presig.PresignGetObject(ctx, input, s3.WithPresignExpires(ttl))
			if err != nil {
				return "", err
			}
			return out.URL, nil
		}
	}
	value, err := call(ctx, state, input, ttl)
	if err != nil {
		return "", fmt.Errorf("s3: presign %q: %w", key, err)
	}
	parsed, err := url.Parse(value)
	if err != nil || (parsed.Scheme != "https" && parsed.Scheme != "http") || parsed.Host == "" {
		return "", fmt.Errorf("s3: presign %q returned an invalid URL", key)
	}
	return value, nil
}

func (s *ScopedArchiveObjectStoreV2) TeardownScope(scope ext.Scope) error {
	if s == nil {
		return nil
	}
	return manager.teardown(scope)
}

func archiveStateForScope(scope ext.Scope, key string) (*s3ClientState, error) {
	if err := scope.Validate(); err != nil {
		return nil, fmt.Errorf("s3: archive scope: %w", err)
	}
	if !safeExactObjectKey(key) {
		return nil, errors.New("s3: exact safe object key is required")
	}
	state, err := manager.setup(scope)
	if err != nil {
		return nil, fmt.Errorf("s3: archive scope setup: %w", err)
	}
	if err := state.ensureClient(); err != nil {
		return nil, err
	}
	return state, nil
}

func exactObjectFromHead(key string, out *s3.HeadObjectOutput) (ExactObject, error) {
	if out == nil {
		return ExactObject{}, fmt.Errorf("s3: head %q returned no result", key)
	}
	object := ExactObject{
		Key: key, Generation: normalizeETag(out.ETag), ContentType: stringValue(out.ContentType),
		Metadata: cloneMetadata(out.Metadata),
	}
	if out.ContentLength != nil {
		object.Size = *out.ContentLength
	}
	if out.LastModified != nil {
		object.LastModified = out.LastModified.UTC()
	}
	if object.Generation == "" {
		return ExactObject{}, fmt.Errorf("s3: head %q returned no generation", key)
	}
	return object, nil
}

func validateExpectedGeneration(value string) error {
	value = normalizeETagValue(value)
	if value == "" || len(value) > 256 || strings.ContainsAny(value, "\r\n\x00") {
		return errors.New("s3: expected object generation is required")
	}
	return nil
}

func applyWriteFence(generation string, ifMatch, ifNoneMatch **string) {
	if generation == ObjectGenerationAbsent {
		*ifNoneMatch = stringPointer("*")
		return
	}
	*ifMatch = quotedETag(generation)
}

func quotedETag(value string) *string {
	normalized := normalizeETagValue(value)
	quoted := strconv.Quote(normalized)
	return &quoted
}

func normalizeETag(value *string) string {
	if value == nil {
		return ""
	}
	return normalizeETagValue(*value)
}

func normalizeETagValue(value string) string {
	return strings.Trim(strings.TrimSpace(value), `"`)
}

func archiveObjectError(operation, key string, err error) error {
	if err == nil {
		return nil
	}
	if classifyS3Error(err) == scopedObjectStoreNotFoundCode {
		return fmt.Errorf("%w: %s", ErrObjectNotFound, key)
	}
	var apiErr smithy.APIError
	if errors.As(err, &apiErr) {
		switch apiErr.ErrorCode() {
		case "PreconditionFailed", "ConditionalRequestConflict":
			return fmt.Errorf("%w: %s %s", ErrObjectGenerationMismatch, operation, key)
		}
	}
	return fmt.Errorf("s3: %s %q: %w", operation, key, err)
}

func validateArchiveMetadata(values map[string]string) error {
	if len(values) > 32 {
		return errors.New("s3: exact-object metadata exceeds 32 fields")
	}
	for key, value := range values {
		if key == "" || len(key) > 128 || len(value) > 1024 ||
			strings.ContainsAny(key, "\r\n\x00") || strings.ContainsAny(value, "\r\n\x00") {
			return errors.New("s3: exact-object metadata is invalid")
		}
	}
	return nil
}

func cloneMetadata(values map[string]string) map[string]string {
	if len(values) == 0 {
		return nil
	}
	out := make(map[string]string, len(values))
	for key, value := range values {
		out[strings.ToLower(key)] = value
	}
	return out
}

func stringPointer(value string) *string {
	return &value
}

func stringValue(value *string) string {
	if value == nil {
		return ""
	}
	return *value
}
