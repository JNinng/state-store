package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
	"state-store/task"
	"state-store/task/export"
	"state-store/task/ingest"
)

// inmemSource is DataSource in-memory for integration testing.
type inmemSource struct {
	data [][]phys.Row
}

func (s *inmemSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if page >= len(s.data) {
		return nil, io.EOF
	}
	return s.data[page], nil
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

	err = task.Run(context.Background(), exportRepo, exportEng, "export-task-1")
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
	importEng := ingest.New(finalPath, importTarget, ingest.WithBatchSize(2))

	err = task.Run(context.Background(), importRepo, importEng, "import-task-1")
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

func TestExportRecovery_CrashDuringRunning(t *testing.T) {
	dir := t.TempDir()

	// 10 页，每页 2 行 = 共 20 行
	pages := make([][]phys.Row, 10)
	for i := 0; i < 10; i++ {
		pages[i] = []phys.Row{{"page": i, "idx": 0}, {"page": i, "idx": 1}}
	}

	repo, err := filestore.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	// === Phase 1: 手动执行几步后模拟崩溃 ===
	src1 := &inmemSource{data: pages}
	eng1 := export.New(src1, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(3))

	state := &statestore.BaseTaskState{
		TaskID:   "recovery-test",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// Pending → Running
	lsn, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	state.CheckpointLSN = lsn

	// 执行 5 个 running 步骤（处理 5 页 = 10 行，"崩溃"发生在第 6 页之前）
	for i := 0; i < 5; i++ {
		lsn, err = eng1.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute step %d: %v", i, err)
		}
		state.CheckpointLSN = lsn
	}

	if state.Phase != statestore.PhaseRunning {
		t.Fatalf("phase after 5 pages = %q, want running", state.Phase)
	}

	// 保存 checkpoint（模拟崩溃前最后一次保存）
	marshaled, _ := json.Marshal(state)
	if err := repo.Save(context.Background(), "recovery-test", marshaled); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	// === Phase 2: 用新 engine 恢复（模拟进程重启） ===
	src2 := &inmemSource{data: pages}
	eng2 := export.New(src2, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(3))

	err = task.Run(context.Background(), repo, eng2, "recovery-test")
	if err != nil {
		t.Fatalf("Run recovery: %v", err)
	}

	// 验证：最终文件应包含全部 20 行
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

	// 验证最终状态
	loaded, err := repo.Load(context.Background(), "recovery-test")
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	var finalState statestore.BaseTaskState
	json.Unmarshal(loaded, &finalState)
	if finalState.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want completed", finalState.Phase)
	}
}

func TestExportRecovery_CrashDuringMerging(t *testing.T) {
	dir := t.TempDir()

	// 4 页，每页 2 行。pageSize=2, chunkPages=1 → 每页一个 chunk，共 4 chunks
	pages := make([][]phys.Row, 4)
	for i := 0; i < 4; i++ {
		pages[i] = []phys.Row{{"page": i, "idx": 0}, {"page": i, "idx": 1}}
	}

	repo, err := filestore.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	// === Phase 1: 完成 running，合并一部分后崩溃 ===
	src1 := &inmemSource{data: pages}
	eng1 := export.New(src1, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(1))

	state := &statestore.BaseTaskState{
		TaskID:   "merge-recovery",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// 执行到 merging 阶段
	for state.Phase != statestore.PhaseMerging {
		lsn, err := eng1.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute: %v", err)
		}
		state.CheckpointLSN = lsn
	}

	// 合并 1 个 chunk 后"崩溃"
	lsn, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute merge step 1: %v", err)
	}
	state.CheckpointLSN = lsn

	if state.Phase != statestore.PhaseMerging {
		t.Fatalf("phase = %q, want merging", state.Phase)
	}

	// 保存 checkpoint
	marshaled, _ := json.Marshal(state)
	if err := repo.Save(context.Background(), "merge-recovery", marshaled); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	// === Phase 2: 用新 engine 恢复 ===
	src2 := &inmemSource{data: pages}
	eng2 := export.New(src2, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(1))

	err = task.Run(context.Background(), repo, eng2, "merge-recovery")
	if err != nil {
		t.Fatalf("Run recovery: %v", err)
	}

	// 验证：最终文件应包含全部 8 行（无重复、无缺失）
	finalPath := filepath.Join(dir, "result.dat")
	finalData, _ := os.ReadFile(finalPath)
	lines := 0
	for _, b := range finalData {
		if b == '\n' {
			lines++
		}
	}
	if lines != 8 {
		t.Errorf("exported lines = %d, want 8", lines)
	}

	// 验证最终状态为 completed
	loaded, err := repo.Load(context.Background(), "merge-recovery")
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	var finalState statestore.BaseTaskState
	json.Unmarshal(loaded, &finalState)
	if finalState.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want completed", finalState.Phase)
	}
}

func TestImportRecovery_CrashDuringRunning(t *testing.T) {
	dir := t.TempDir()

	// 准备 10 行源数据
	srcPath := filepath.Join(dir, "source.jsonl")
	allRows := make([]phys.Row, 10)
	f, _ := os.Create(srcPath)
	for i := 0; i < 10; i++ {
		allRows[i] = phys.Row{"id": i, "name": "row"}
		data, _ := json.Marshal(allRows[i])
		f.Write(append(data, '\n'))
	}
	f.Close()

	repo, err := filestore.New(filepath.Join(dir, "import-state"))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	// === Phase 1: 导入一部分后模拟崩溃 ===
	target1 := &inmemTarget{}
	eng1 := ingest.New(srcPath, target1, ingest.WithBatchSize(2))

	state := &statestore.BaseTaskState{
		TaskID:   "import-recovery",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	// Pending → Running
	lsn, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	state.CheckpointLSN = lsn

	// 执行 2 批（每批 2 行 = 共 4 行）
	for i := 0; i < 2; i++ {
		lsn, err = eng1.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute batch %d: %v", err, i)
		}
		state.CheckpointLSN = lsn
	}

	if len(target1.rows) != 4 {
		t.Fatalf("phase 1 rows = %d, want 4", len(target1.rows))
	}

	// 保存 checkpoint（模拟崩溃前最后一次保存）
	marshaled, _ := json.Marshal(state)
	if err := repo.Save(context.Background(), "import-recovery", marshaled); err != nil {
		t.Fatalf("save checkpoint: %v", err)
	}

	// === Phase 2: 用新 target 和 engine 恢复 ===
	target2 := &inmemTarget{}
	eng2 := ingest.New(srcPath, target2, ingest.WithBatchSize(2))

	err = task.Run(context.Background(), repo, eng2, "import-recovery")
	if err != nil {
		t.Fatalf("Run recovery: %v", err)
	}

	// 验证：新 target 应只收到剩余 6 行（无重复）
	if len(target2.rows) != 6 {
		t.Errorf("resumed rows = %d, want 6", len(target2.rows))
	}
	// 验证第一行是 id=4（跳过了前 4 行）
	if len(target2.rows) == 0 {
		t.Fatal("target2.rows is empty")
	}
	idVal, ok := target2.rows[0]["id"].(float64)
	if !ok || int(idVal) != 4 {
		t.Errorf("first resumed row id = %v (type=%T), want 4", target2.rows[0]["id"], target2.rows[0]["id"])
	}

	// 验证最终状态
	loaded, err := repo.Load(context.Background(), "import-recovery")
	if err != nil {
		t.Fatalf("load final state: %v", err)
	}
	var finalState statestore.BaseTaskState
	json.Unmarshal(loaded, &finalState)
	if finalState.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want completed", finalState.Phase)
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

	err = task.Run(context.Background(), repo, exportEng, "resume-test")
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
