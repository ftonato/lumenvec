package storage

import (
	"errors"
	"testing"
)

type badValue struct{}

func (badValue) MarshalJSON() ([]byte, error) {
	return nil, errors.New("boom")
}

func TestLevelDBStoreCRUD(t *testing.T) {
	store, err := NewLevelDBStore(t.TempDir())
	if err != nil {
		t.Fatalf("NewLevelDBStore() error = %v", err)
	}

	if err := store.Put("k1", map[string]int{"a": 1}); err != nil {
		t.Fatalf("Put() error = %v", err)
	}

	var got map[string]int
	if err := store.Get("k1", &got); err != nil {
		t.Fatalf("Get() error = %v", err)
	}
	if got["a"] != 1 {
		t.Fatal("unexpected stored value")
	}

	if err := store.Delete("k1"); err != nil {
		t.Fatalf("Delete() error = %v", err)
	}
	got = nil
	if err := store.Get("k1", &got); err != nil {
		t.Fatalf("Get() after delete error = %v", err)
	}
	if got != nil {
		t.Fatal("expected nil after delete")
	}
}

func TestLevelDBStorePutMarshalError(t *testing.T) {
	store, _ := NewLevelDBStore(t.TempDir())
	if err := store.Put("bad", badValue{}); err == nil {
		t.Fatal("expected marshal error")
	}
}

func TestLevelDBStoreIterate(t *testing.T) {
	store, _ := NewLevelDBStore(t.TempDir())
	_ = store.Put("user:1", map[string]string{"id": "1"})
	_ = store.Put("user:2", map[string]string{"id": "2"})
	_ = store.Put("other:1", map[string]string{"id": "3"})

	var seen int
	if err := store.Iterate("user:", func(key string, value interface{}) bool {
		seen++
		return seen < 2
	}); err != nil {
		t.Fatalf("Iterate() error = %v", err)
	}
	if seen != 2 {
		t.Fatalf("expected 2 iterations, got %d", seen)
	}
}

func TestLevelDBStoreClose(t *testing.T) {
	store, _ := NewLevelDBStore(t.TempDir())
	if err := store.Close(); err != nil {
		t.Fatalf("Close() error = %v", err)
	}
}
