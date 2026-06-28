package filestore_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"state-store/task/outbox"
	"state-store/task/outbox/filestore"
)

func setup(t *testing.T) (string, *filestore.FileStore, context.Context) {
	t.Helper()
	dir := t.TempDir()
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	return dir, store, context.Background()
}

func TestFileStore_NewCreatesDir(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "sub", "nested")
	store, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if store == nil {
		t.Fatal("store is nil")
	}
	if _, err := os.Stat(dir); os.IsNotExist(err) {
		t.Error("directory should have been created")
	}
}

func TestFileStore_AppendAndFetch(t *testing.T) {
	_, store, ctx := setup(t)

	msg1 := &outbox.Message{
		ID:        "msg-001",
		EventType: "send_email",
		Payload:   json.RawMessage(`{"to":"a@b.com"}`),
		Status:    outbox.StatusPending,
		CreatedAt: time.Now().Add(-2 * time.Hour),
	}
	msg2 := &outbox.Message{
		ID:        "msg-002",
		EventType: "notify_slack",
		Payload:   json.RawMessage(`{"channel":"general"}`),
		Status:    outbox.StatusPending,
		CreatedAt: time.Now().Add(-1 * time.Hour),
	}

	if err := store.Append(ctx, msg1); err != nil {
		t.Fatalf("Append msg1: %v", err)
	}
	if err := store.Append(ctx, msg2); err != nil {
		t.Fatalf("Append msg2: %v", err)
	}

	err := store.Append(ctx, &outbox.Message{ID: "msg-001", EventType: "x"})
	if !errors.Is(err, outbox.ErrDuplicateID) {
		t.Errorf("Append duplicate: error = %v, want ErrDuplicateID", err)
	}

	pending, err := store.FetchPending(ctx)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("FetchPending count = %d, want 2", len(pending))
	}

	if pending[0].ID != "msg-001" {
		t.Errorf("pending[0].ID = %s, want msg-001", pending[0].ID)
	}
	if pending[1].ID != "msg-002" {
		t.Errorf("pending[1].ID = %s, want msg-002", pending[1].ID)
	}
}

func TestFileStore_Append_SetsDefaults(t *testing.T) {
	_, store, ctx := setup(t)

	msg := &outbox.Message{ID: "auto-defaults", EventType: "test"}
	if err := store.Append(ctx, msg); err != nil {
		t.Fatalf("Append: %v", err)
	}

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 1 {
		t.Fatalf("count = %d, want 1", len(pending))
	}
	if pending[0].Status != outbox.StatusPending {
		t.Errorf("default Status = %s, want pending", pending[0].Status)
	}
	if pending[0].CreatedAt.IsZero() {
		t.Error("CreatedAt should be set")
	}
}

func TestFileStore_MarkProcessed(t *testing.T) {
	_, store, ctx := setup(t)

	store.Append(ctx, &outbox.Message{ID: "prog-1", EventType: "test"})

	if err := store.MarkProcessed(ctx, "prog-1"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 0 {
		t.Errorf("pending after MarkProcessed = %d, want 0", len(pending))
	}

	if err := store.MarkProcessed(ctx, "prog-1"); err != nil {
		t.Errorf("MarkProcessed (idempotent): %v", err)
	}

	if err := store.MarkProcessed(ctx, "nonexistent"); err != nil {
		t.Errorf("MarkProcessed (nonexistent): %v", err)
	}
}

func TestFileStore_MarkFailed(t *testing.T) {
	_, store, ctx := setup(t)

	store.Append(ctx, &outbox.Message{ID: "fail-1", EventType: "test"})

	if err := store.MarkFailed(ctx, "fail-1", "connection refused"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	if err := store.MarkFailed(ctx, "nonexistent", "err"); err != nil {
		t.Errorf("MarkFailed (nonexistent): %v", err)
	}

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 0 {
		t.Errorf("pending after MarkFailed = %d, want 0", len(pending))
	}
}

func TestFileStore_UpdateStatus(t *testing.T) {
	_, store, ctx := setup(t)

	store.Append(ctx, &outbox.Message{ID: "upd-1", EventType: "test"})

	if err := store.UpdateStatus(ctx, "upd-1", outbox.StatusProcessing, 1, "retrying"); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 1 {
		t.Fatalf("pending count = %d, want 1", len(pending))
	}
	if pending[0].Status != outbox.StatusProcessing {
		t.Errorf("status = %s, want processing", pending[0].Status)
	}
	if pending[0].Retries != 1 {
		t.Errorf("retries = %d, want 1", pending[0].Retries)
	}
	if pending[0].LastError != "retrying" {
		t.Errorf("lastError = %s, want retrying", pending[0].LastError)
	}
}

func TestFileStore_UpdateStatus_NotFound(t *testing.T) {
	_, store, ctx := setup(t)

	err := store.UpdateStatus(ctx, "no-such-msg", outbox.StatusProcessing, 0, "")
	if err == nil {
		t.Error("UpdateStatus on nonexistent should error")
	}
}

func TestFileStore_AppendAtomic_NoPartialWrite(t *testing.T) {
	dir, store, ctx := setup(t)

	if err := store.Append(ctx, &outbox.Message{ID: "atomic-1", EventType: "test"}); err != nil {
		t.Fatalf("Append: %v", err)
	}

	entries, _ := os.ReadDir(dir)
	for _, e := range entries {
		if filepath.Ext(e.Name()) == ".tmp" {
			t.Errorf("orphan .tmp file found: %s", e.Name())
		}
	}
}

func TestFileStore_Cleanup(t *testing.T) {
	dir := t.TempDir()
	_, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tmpFile := filepath.Join(dir, "orphan.msg.json.tmp")
	if err := os.WriteFile(tmpFile, []byte("garbage"), 0644); err != nil {
		t.Fatalf("create orphan: %v", err)
	}

	store2, err := filestore.New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
		os.WriteFile(tmpFile, []byte("garbage"), 0644)
		store2.Cleanup()
		if _, err := os.Stat(tmpFile); !os.IsNotExist(err) {
			t.Error("Cleanup should have removed orphan .tmp file")
		}
	}
}

func TestFileStore_FetchPending_EmptyDir(t *testing.T) {
	_, store, ctx := setup(t)

	pending, err := store.FetchPending(ctx)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if pending == nil {
		t.Error("FetchPending on empty dir should return empty slice, not nil")
	}
	if len(pending) != 0 {
		t.Errorf("pending count = %d, want 0", len(pending))
	}
}

func TestFileStore_FetchPending_FiltersStatus(t *testing.T) {
	_, store, ctx := setup(t)

	store.Append(ctx, &outbox.Message{ID: "f-pend", EventType: "t", Status: outbox.StatusPending})
	store.Append(ctx, &outbox.Message{ID: "f-comp", EventType: "t", Status: outbox.StatusCompleted})
	store.Append(ctx, &outbox.Message{ID: "f-fail", EventType: "t", Status: outbox.StatusFailed})
	store.Append(ctx, &outbox.Message{ID: "f-proc", EventType: "t", Status: outbox.StatusProcessing})

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 2 {
		t.Errorf("pending count = %d, want 2 (pending + processing only)", len(pending))
	}
	ids := make(map[string]bool)
	for _, m := range pending {
		ids[m.ID] = true
	}
	if !ids["f-pend"] || !ids["f-proc"] {
		t.Errorf("unexpected pending IDs: %v", ids)
	}
}

func TestFileStore_FetchPending_SortOrder(t *testing.T) {
	_, store, ctx := setup(t)

	t1 := time.Now().Add(-3 * time.Hour)
	t2 := time.Now().Add(-2 * time.Hour)
	t3 := time.Now().Add(-1 * time.Hour)

	store.Append(ctx, &outbox.Message{ID: "s-3", EventType: "t", CreatedAt: t3})
	store.Append(ctx, &outbox.Message{ID: "s-1", EventType: "t", CreatedAt: t1})
	store.Append(ctx, &outbox.Message{ID: "s-2", EventType: "t", CreatedAt: t2})

	pending, _ := store.FetchPending(ctx)
	if len(pending) != 3 {
		t.Fatalf("count = %d, want 3", len(pending))
	}

	expected := []string{"s-1", "s-2", "s-3"}
	for i, exp := range expected {
		if pending[i].ID != exp {
			t.Errorf("pending[%d].ID = %s, want %s", i, pending[i].ID, exp)
		}
	}
}

func TestFileStore_CompileTimeInterfaceCheck(t *testing.T) {
	store, _ := filestore.New(t.TempDir())
	var _ outbox.Store = store
}
