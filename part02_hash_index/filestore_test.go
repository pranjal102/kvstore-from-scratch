package kvstorefromscratchpart2

import (
	"testing"
)

func TestFileStore_PutGetDel(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := ConnectFileStore(tmpDir)
	if err != nil {
		t.Fatalf("ConnectFileStore failed: %v", err)
	}
	defer store.Close()

	key := "foo"
	val := "bar"

	// Test Put
	if err := store.Put(key, val); err != nil {
		t.Fatalf("Put failed: %v", err)
	}

	// Test Get
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != val {
		t.Errorf("Get returned %q, want %q", got, val)
	}

	// Test Del
	if err := store.Del(key); err != nil {
		t.Fatalf("Del failed: %v", err)
	}

	// Test Get after Del
	_, err = store.Get(key)
	if err != ErrKeyDoesntExist {
		t.Errorf("Get after Del returned err %v, want ErrKeyDoesntExist", err)
	}
}

func TestFileStore_GetNonExistentKey(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := ConnectFileStore(tmpDir)
	if err != nil {
		t.Fatalf("ConnectFileStore failed: %v", err)
	}
	defer store.Close()

	_, err = store.Get("doesnotexist")
	if err != ErrKeyDoesntExist {
		t.Errorf("Get non-existent key returned err %v, want ErrKeyDoesntExist", err)
	}
}

func TestFileStore_PutOverwrite(t *testing.T) {
	tmpDir := t.TempDir()

	store, err := ConnectFileStore(tmpDir)
	if err != nil {
		t.Fatalf("ConnectFileStore failed: %v", err)
	}
	defer store.Close()

	key := "foo"
	val1 := "bar"
	val2 := "baz"

	if err := store.Put(key, val1); err != nil {
		t.Fatalf("Put failed: %v", err)
	}
	if err := store.Put(key, val2); err != nil {
		t.Fatalf("Put overwrite failed: %v", err)
	}
	got, err := store.Get(key)
	if err != nil {
		t.Fatalf("Get failed: %v", err)
	}
	if got != val2 {
		t.Errorf("Get after overwrite returned %q, want %q", got, val2)
	}
}
