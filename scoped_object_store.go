package s3ext

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/BananaLabs-OSS/Pulp/ext"
	"github.com/aws/aws-sdk-go-v2/service/s3"
)

// ErrObjectNotFound is returned when a provider explicitly reports that an
// exact object is absent. DeleteObject is normally idempotent in S3/R2, so most
// missing-object replays already return nil; callers may safely treat either
// nil or this sentinel as the requested object being absent.
var ErrObjectNotFound = errors.New("s3: object not found")

const scopedObjectStoreNotFoundCode uint32 = 6

// ScopedObjectStore is the minimal host-owned API for provider-neutral effect
// executors that need one exact R2 object operation. It deliberately does not
// expose list, prefix delete, credentials, or the raw AWS client.
//
// Client state is keyed by application identity through the same manager used
// by the storage.s3 capability, so two Pulp applications never share mutable
// client lifecycle or teardown.
type ScopedObjectStore struct {
	deleteObject func(context.Context, *s3ClientState, string) error
}

func NewScopedObjectStore() *ScopedObjectStore {
	return &ScopedObjectStore{}
}

// DeleteObject removes exactly one validated key in the supplied application
// scope. It never accepts a prefix-like trailing slash and never expands,
// lists, or derives additional keys.
func (s *ScopedObjectStore) DeleteObject(ctx context.Context, scope ext.Scope, key string) error {
	if s == nil {
		return errors.New("s3: scoped object store is required")
	}
	if err := scope.Validate(); err != nil {
		return fmt.Errorf("s3: scoped object store scope: %w", err)
	}
	if !safeExactObjectKey(key) {
		return errors.New("s3: exact safe object key is required")
	}
	state, err := manager.setup(scope)
	if err != nil {
		return fmt.Errorf("s3: scoped object store setup: %w", err)
	}
	if err := state.ensureClient(); err != nil {
		return err
	}
	deleteObject := s.deleteObject
	if deleteObject == nil {
		deleteObject = func(ctx context.Context, state *s3ClientState, key string) error {
			_, err := state.client.DeleteObject(ctx, &s3.DeleteObjectInput{Bucket: &state.bucket, Key: &key})
			return err
		}
	}
	if err := deleteObject(ctx, state, key); err != nil {
		if classifyS3Error(err) == scopedObjectStoreNotFoundCode {
			return fmt.Errorf("%w: %s", ErrObjectNotFound, key)
		}
		return err
	}
	return nil
}

// TeardownScope releases only the application identity that owned the scoped
// object operations. It is idempotent and shares the capability's existing
// lifecycle path.
func (s *ScopedObjectStore) TeardownScope(scope ext.Scope) error {
	if s == nil {
		return nil
	}
	return manager.teardown(scope)
}

func safeExactObjectKey(value string) bool {
	return value != "" && len(value) <= 1024 && !strings.HasSuffix(value, "/") &&
		!strings.Contains(value, "..") && !strings.ContainsAny(value, "\\\r\n\x00")
}
