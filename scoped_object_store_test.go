package s3ext

import (
	"context"
	"errors"
	"testing"

	s3types "github.com/aws/aws-sdk-go-v2/service/s3/types"
)

func TestScopedObjectStoreDeletesOnlyExactKeyInApplicationScope(t *testing.T) {
	resetClient(t)
	left := scopedS3(t, "sessions", "blue", "sessions-fleet")
	right := scopedS3(t, "evolution", "default", "sessions-fleet")
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1")

	var states []*s3ClientState
	var keys []string
	store := &ScopedObjectStore{deleteObject: func(_ context.Context, state *s3ClientState, key string) error {
		states = append(states, state)
		keys = append(keys, key)
		return nil
	}}
	if err := store.DeleteObject(context.Background(), left, "worlds/world-1.zip"); err != nil {
		t.Fatalf("left DeleteObject: %v", err)
	}
	if err := store.DeleteObject(context.Background(), right, "uploads/upload-1.zip"); err != nil {
		t.Fatalf("right DeleteObject: %v", err)
	}
	if len(states) != 2 || states[0] == states[1] {
		t.Fatalf("application scopes shared client state: %#v", states)
	}
	if keys[0] != "worlds/world-1.zip" || keys[1] != "uploads/upload-1.zip" {
		t.Fatalf("keys = %#v", keys)
	}
}

func TestScopedObjectStoreRejectsPrefixAndUnsafeKeysBeforeProviderCall(t *testing.T) {
	resetClient(t)
	scope := scopedS3(t, "sessions", "blue", "sessions-fleet")
	calls := 0
	store := &ScopedObjectStore{deleteObject: func(context.Context, *s3ClientState, string) error {
		calls++
		return nil
	}}
	for _, key := range []string{"", "worlds/", "../other-app/world.zip", "worlds\\world.zip", "worlds/\n.zip"} {
		if err := store.DeleteObject(context.Background(), scope, key); err == nil {
			t.Errorf("DeleteObject(%q) succeeded", key)
		}
	}
	if calls != 0 {
		t.Fatalf("unsafe keys reached provider %d times", calls)
	}
}

func TestScopedObjectStoreClassifiesNotFoundForReplay(t *testing.T) {
	resetClient(t)
	scope := scopedS3(t, "sessions", "blue", "sessions-fleet")
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1")
	store := &ScopedObjectStore{deleteObject: func(context.Context, *s3ClientState, string) error {
		return &s3types.NoSuchKey{}
	}}
	if err := store.DeleteObject(context.Background(), scope, "worlds/missing.zip"); !errors.Is(err, ErrObjectNotFound) {
		t.Fatalf("not-found error = %v", err)
	}
}

func TestScopedObjectStoreTeardownReleasesOnlyOwningApplication(t *testing.T) {
	resetClient(t)
	left := scopedS3(t, "sessions", "blue", "sessions-fleet")
	right := scopedS3(t, "evolution", "default", "sessions-fleet")
	t.Setenv("S3_ACCESS_KEY_ID", "test")
	t.Setenv("S3_SECRET_ACCESS_KEY", "test")
	t.Setenv("S3_BUCKET", "test")
	t.Setenv("S3_ENDPOINT", "http://127.0.0.1")
	store := &ScopedObjectStore{deleteObject: func(context.Context, *s3ClientState, string) error { return nil }}
	if err := store.DeleteObject(context.Background(), left, "worlds/left.zip"); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteObject(context.Background(), right, "worlds/right.zip"); err != nil {
		t.Fatal(err)
	}
	if err := store.TeardownScope(left); err != nil {
		t.Fatal(err)
	}
	manager.mu.Lock()
	_, leftExists := manager.clients[applicationKey(left)]
	_, rightExists := manager.clients[applicationKey(right)]
	manager.mu.Unlock()
	if leftExists || !rightExists {
		t.Fatalf("left/right state after teardown = %v/%v", leftExists, rightExists)
	}
}
