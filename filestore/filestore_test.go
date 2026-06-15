package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"state-store/statestore"
)

func TestFileRepository_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-001"
	original := []byte(`{"task_id":"task-001","phase":"running"}`)

	if err := repo.Save(ctx, taskID, original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	data, err := repo.Load(ctx, taskID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(data) != string(original) {
		t.Errorf("data = %s, want %s", data, original)
	}
}

func TestFileRepository_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	data, err := repo.Load(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Load should not error for missing task: %v", err)
	}
	if data != nil {
		t.Errorf("data should be nil for missing task, got %s", data)
	}
}

func TestFileRepository_Delete(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-del"
	if err := repo.Save(ctx, taskID, []byte("{}")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Delete(ctx, taskID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	data, err := repo.Load(ctx, taskID)
	if err != nil {
		t.Fatalf("Load after delete: %v", err)
	}
	if data != nil {
		t.Errorf("data should be nil after delete, got %s", data)
	}
}

func TestFileRepository_DeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if err := repo.Delete(context.Background(), "nonexistent"); err != nil {
		t.Errorf("Delete should be idempotent: %v", err)
	}
}

func TestFileRepository_SaveAtomic_NoPartialWrite(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-atomic"

	if err := repo.Save(ctx, taskID, []byte("atomic-data")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	tmpPath := filepath.Join(dir, taskID+".state.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error(".tmp file should not exist after successful Save")
	}

	statePath := filepath.Join(dir, taskID+".state")
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		t.Error(".state file should exist after Save")
	}
}

func TestFileRepository_Cleanup(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	tmpPath := filepath.Join(dir, "orphan.state.tmp")
	if err := os.WriteFile(tmpPath, []byte("garbage"), 0644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	if err := repo.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Cleanup should remove orphan .tmp files")
	}
}

func TestFileRepository_ImplementsInterface(t *testing.T) {
	var _ statestore.StateRepository = (*FileRepository)(nil)
}

func TestFileRepository_NewCreatesDir(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "subdir", "statestore")
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if repo == nil {
		t.Fatal("repo should not be nil")
	}
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir() {
		t.Error("path should be a directory")
	}
}
