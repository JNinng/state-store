package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"state-store/engine"
	"state-store/engine/export"
	importpkg "state-store/engine/import"
	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
)

// inmemSource is DataSource in-memory for integration testing.
type inmemSource struct {
	data [][]phys.Row
	idx  int
}

func (s *inmemSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if s.idx >= len(s.data) {
		return nil, io.EOF
	}
	result := s.data[s.idx]
	s.idx++
	return result, nil
}

// inmemTarget is DataTarget in-memory for integration testing.
type inmemTarget struct {
	rows []phys.Row
}

func (t *inmemTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.rows = append(t.rows, rows...)
	return int64(len(rows)), nil
}

func TestExportThenImport_EndToEnd(t *testing.T) {
	dir := t.TempDir()

	// Prepare test data (2 pages, 3+2 rows)
	sourceData := [][]phys.Row{
		{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
			{"id": 3, "name": "carol"},
		},
		{
			{"id": 4, "name": "dave"},
			{"id": 5, "name": "eve"},
		},
	}

	// === Export phase ===
	exportRepo, err := filestore.New(filepath.Join(dir, "export-state"))
	if err != nil {
		t.Fatalf("create export repo: %v", err)
	}

	exportSrc := &inmemSource{data: sourceData}
	exportEng := export.New(exportSrc, dir, "exported.dat",
		export.WithPageSize(3), export.WithChunkPages(1))

	err = engine.Run(context.Background(), exportRepo, exportEng, "export-task-1")
	if err != nil {
		t.Fatalf("export Run: %v", err)
	}

	// Verify final file exists
	finalPath := filepath.Join(dir, "exported.dat")
	info, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("stat exported file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("exported file is empty")
	}

	// Verify state is completed
	data, err := exportRepo.Load(context.Background(), "export-task-1")
	if err != nil {
		t.Fatalf("load export state: %v", err)
	}
	var exportState statestore.BaseTaskState
	json.Unmarshal(data, &exportState)
	if exportState.Phase != statestore.PhaseCompleted {
		t.Errorf("export phase = %q, want completed", exportState.Phase)
	}

	// === Import phase ===
	importRepo, err := filestore.New(filepath.Join(dir, "import-state"))
	if err != nil {
		t.Fatalf("create import repo: %v", err)
	}

	importTarget := &inmemTarget{}
	importEng := importpkg.New(finalPath, importTarget, importpkg.WithBatchSize(2))

	err = engine.Run(context.Background(), importRepo, importEng, "import-task-1")
	if err != nil {
		t.Fatalf("import Run: %v", err)
	}

	// Verify imported row count
	if len(importTarget.rows) != 5 {
		t.Errorf("imported rows = %d, want 5", len(importTarget.rows))
	}

	// Verify import state
	data, err = importRepo.Load(context.Background(), "import-task-1")
	if err != nil {
		t.Fatalf("load import state: %v", err)
	}
	var importState statestore.BaseTaskState
	json.Unmarshal(data, &importState)
	if importState.Phase != statestore.PhaseCompleted {
		t.Errorf("import phase = %q, want completed", importState.Phase)
	}
}

func TestExportResume_MidCrash(t *testing.T) {
	dir := t.TempDir()

	// 10 pages x 2 rows each
	pages := make([][]phys.Row, 10)
	for i := 0; i < 10; i++ {
		pages[i] = []phys.Row{{"page": i, "idx": 0}, {"page": i, "idx": 1}}
	}

	repo, err := filestore.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	exportSrc := &inmemSource{data: pages}
	exportEng := export.New(exportSrc, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(3))

	err = engine.Run(context.Background(), repo, exportEng, "resume-test")
	if err != nil {
		t.Fatalf("export Run: %v", err)
	}

	// Verify exported line count
	finalPath := filepath.Join(dir, "result.dat")
	finalData, _ := os.ReadFile(finalPath)
	lines := 0
	for _, b := range finalData {
		if b == '\n' {
			lines++
		}
	}
	if lines != 20 {
		t.Errorf("exported lines = %d, want 20", lines)
	}
}
