package importpkg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"state-store/phys"
	"state-store/statestore"
)

type stubDataTarget struct {
	inserted []phys.Row
}

func (t *stubDataTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.inserted = append(t.inserted, rows...)
	return int64(len(rows)), nil
}

func TestImportEngine_NormalFlow(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")

	rows := []phys.Row{
		{"id": "1", "name": "alice"},
		{"id": "2", "name": "bob"},
		{"id": "3", "name": "carol"},
	}
	f, _ := os.Create(srcPath)
	for _, r := range rows {
		data, _ := json.Marshal(r)
		f.Write(append(data, '\n'))
	}
	f.Close()

	target := &stubDataTarget{}
	eng := New(srcPath, target, WithBatchSize(2))

	state := &statestore.BaseTaskState{
		TaskID:   "import-001",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	// Phase pending → running
	lsn, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Errorf("phase = %q, want running", state.Phase)
	}
	_ = lsn

	// Batch 1 (2 rows)
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute batch 1: %v", err)
	}
	if len(target.inserted) != 2 {
		t.Errorf("inserted rows = %d, want 2", len(target.inserted))
	}

	// Batch 2 (1 row) → completed
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute batch 2: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("phase = %q, want completed", state.Phase)
	}
	if len(target.inserted) != 3 {
		t.Errorf("total inserted = %d, want 3", len(target.inserted))
	}
}

func TestImportEngine_Compensate(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")
	os.WriteFile(srcPath, []byte(`{"id":"1"}`+"\n"), 0644)

	eng := New(srcPath, &stubDataTarget{})

	if err := eng.Compensate(context.Background(), 5); err != nil {
		t.Errorf("Compensate should succeed when LSN <= file size: %v", err)
	}

	if err := eng.Compensate(context.Background(), 99999); err == nil {
		t.Error("Compensate should fail when LSN > file size")
	}
}

func TestImportEngine_Progress(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")
	os.WriteFile(srcPath, make([]byte, 1000), 0644)

	eng := New(srcPath, &stubDataTarget{})
	state := statestore.BaseTaskState{
		Phase:   statestore.PhaseRunning,
		Payload: json.RawMessage(`{"current_read_offset":500}`),
	}
	prog := eng.Progress(state)
	if prog != 50 {
		t.Errorf("progress = %d, want 50", prog)
	}
}
