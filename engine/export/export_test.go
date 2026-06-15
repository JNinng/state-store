package export

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"state-store/phys"
	"state-store/statestore"
)

type stubDataSource struct {
	pages   [][]phys.Row
	pageIdx int
}

func (s *stubDataSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if s.pageIdx >= len(s.pages) {
		return nil, io.EOF
	}
	result := s.pages[s.pageIdx]
	s.pageIdx++
	return result, nil
}

func makeRows(n int) []phys.Row {
	rows := make([]phys.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = phys.Row{"id": i, "name": "row"}
	}
	return rows
}

func TestExportEngine_NormalFlow(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{
		pages: [][]phys.Row{
			makeRows(5),
			makeRows(5),
		},
	}
	eng := New(ds, dir, "output.dat", WithPageSize(5), WithChunkPages(2))

	state := &statestore.BaseTaskState{
		TaskID:   "export-001",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// Phase pending → running
	lsn, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	if lsn != 0 {
		t.Errorf("lsn = %d, want 0", lsn)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Errorf("phase = %q, want running", state.Phase)
	}

	// Phase running: iterate until merging
	for state.Phase == statestore.PhaseRunning {
		lsn, err = eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute running: %v", err)
		}
	}
	if state.Phase != statestore.PhaseMerging {
		t.Errorf("phase after running = %q, want merging", state.Phase)
	}

	// Phase merging → completed
	for state.Phase == statestore.PhaseMerging {
		lsn, err = eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute merging: %v", err)
		}
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want completed", state.Phase)
	}
	_ = lsn
}

func TestExportEngine_Compensate(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{}
	eng := New(ds, dir, "output.dat")

	finalPath := filepath.Join(dir, "output.dat")
	os.WriteFile(finalPath, make([]byte, 1000), 0644)

	err := eng.Compensate(context.Background(), 500)
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}

	info, _ := os.Stat(finalPath)
	if info.Size() > 500 {
		t.Errorf("file size = %d, want <= 500", info.Size())
	}
}

func TestExportEngine_Progress(t *testing.T) {
	eng := New(nil, "", "")
	state := statestore.BaseTaskState{
		Phase:   statestore.PhaseRunning,
		Payload: json.RawMessage(`{"current_chunk_idx":5,"total_chunks":10}`),
	}
	prog := eng.Progress(state)
	if prog != 50 {
		t.Errorf("progress = %d, want 50", prog)
	}
}
