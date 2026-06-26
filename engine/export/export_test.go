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
	pages [][]phys.Row
}

func (s *stubDataSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if page >= len(s.pages) {
		return nil, io.EOF
	}
	return s.pages[page], nil
}

func makeRows(n int) []phys.Row {
	rows := make([]phys.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = phys.Row{"id": i, "name": "row"}
	}
	return rows
}

func TestEngine_NormalFlow(t *testing.T) {
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

func TestEngine_Compensate(t *testing.T) {
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

func TestEngine_Compensate_NoFile(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{}
	eng := New(ds, dir, "output.dat")

	// 最终文件不存在时 Compensate 应为 no-op（不报错）
	err := eng.Compensate(context.Background(), 500)
	if err != nil {
		t.Fatalf("Compensate should succeed when file doesn't exist: %v", err)
	}

	// 确认没有创建文件
	_, err = os.Stat(filepath.Join(dir, "output.dat"))
	if !os.IsNotExist(err) {
		t.Error("Compensate should not create the file")
	}
}

func TestEngine_Compensate_FileSmallerThanLSN(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{}
	eng := New(ds, dir, "output.dat")

	finalPath := filepath.Join(dir, "output.dat")
	os.WriteFile(finalPath, make([]byte, 100), 0644)

	// 文件比 LSN 小，不应截断
	err := eng.Compensate(context.Background(), 500)
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}

	info, _ := os.Stat(finalPath)
	if info.Size() != 100 {
		t.Errorf("file size = %d, want 100 (should not truncate when smaller than LSN)", info.Size())
	}
}

func TestEngine_ResumeAfterInterrupt(t *testing.T) {
	dir := t.TempDir()

	// 3 页数据，每页 3 行
	pages := [][]phys.Row{
		makeRows(3),
		makeRows(3),
		makeRows(3),
	}

	// === 第一阶段：模拟执行到一半后中断 ===
	ds1 := &stubDataSource{pages: pages}
	eng1 := New(ds1, dir, "output.dat", WithPageSize(3), WithChunkPages(2))

	state := &statestore.BaseTaskState{
		TaskID:   "export-resume",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// Pending → Running
	_, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}

	// 执行两步 running（page 0 和 page 1，共 2 页 = 1 个 chunk）
	for i := 0; i < 2; i++ {
		_, err = eng1.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute step %d: %v", i, err)
		}
	}
	if state.Phase != statestore.PhaseRunning {
		t.Fatalf("phase after 2 pages = %q, want running", state.Phase)
	}

	// 保存 checkpoint 状态（模拟崩溃前最后一次保存）
	savedLSN := state.CheckpointLSN
	savedPayload := make([]byte, len(state.Payload))
	copy(savedPayload, state.Payload)

	// === 第二阶段：用新引擎恢复（模拟进程重启）===
	ds2 := &stubDataSource{pages: pages}
	eng2 := New(ds2, dir, "output.dat", WithPageSize(3), WithChunkPages(2))

	// 从保存的状态恢复
	state2 := &statestore.BaseTaskState{
		TaskID:        "export-resume",
		TaskType:      "export",
		Phase:         statestore.PhaseRunning,
		CheckpointLSN: savedLSN,
		Payload:       savedPayload,
	}

	// 先 Compensate
	err = eng2.Compensate(context.Background(), state2.CheckpointLSN)
	if err != nil {
		t.Fatalf("Compensate on resume: %v", err)
	}

	// 继续 Execute 直到完成
	for state2.Phase != statestore.PhaseCompleted {
		_, err = eng2.Execute(context.Background(), state2)
		if err != nil {
			t.Fatalf("Execute after resume (phase=%q): %v", state2.Phase, err)
		}
	}

	// 验证最终文件包含全部 9 行
	finalPath := filepath.Join(dir, "output.dat")
	data, _ := os.ReadFile(finalPath)
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 9 {
		t.Errorf("exported lines = %d, want 9", lines)
	}
}

func TestEngine_Progress(t *testing.T) {
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

func TestEngine_IdempotentExecute_RunningPhase(t *testing.T) {

	dir := t.TempDir()

	// 3 页数据，每页 3 行，共 9 行。pageSize=3, chunkPages=2
	pages := [][]phys.Row{
		makeRows(3),
		makeRows(3),
		makeRows(3),
	}

	ds := &stubDataSource{pages: pages}
	eng := New(ds, dir, "output.dat", WithPageSize(3), WithChunkPages(2))

	state := &statestore.BaseTaskState{
		TaskID:   "export-idempotent",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// Step 1: Pending → Running
	_, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Fatalf("phase after pending = %q, want running", state.Phase)
	}

	// 保存 Step 1 后的 payload（作为回退基准）
	snapshotPayload := make([]byte, len(state.Payload))
	copy(snapshotPayload, state.Payload)
	snapshotLSN := state.CheckpointLSN

	// Step 2: 执行 2 步 running（处理 page 0 和 page 1，形成 1 个 chunk）
	for i := 0; i < 2; i++ {
		_, err = eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute running step %d: %v", i, err)
		}
	}
	if state.Phase != statestore.PhaseRunning {
		t.Fatalf("phase after 2 running steps = %q, want running", state.Phase)
	}

	// Step 3: 模拟 "Execute 成功但 Save 未发生"
	// 将 state 的 Payload 和 CheckpointLSN 回退到 Step 1 后的值
	state.Payload = make([]byte, len(snapshotPayload))
	copy(state.Payload, snapshotPayload)
	state.CheckpointLSN = snapshotLSN

	// Step 4: 用回退后的 state 重新 Execute
	// （相当于同一批数据被重复处理——引擎应幂等）
	for state.Phase != statestore.PhaseCompleted && state.Phase != statestore.PhaseFailed {
		_, err = eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute after rewind (phase=%q): %v", state.Phase, err)
		}
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Fatalf("final phase = %q, want completed", state.Phase)
	}

	// 验证：最终文件中行数 = 数据源总行数（无重复、无缺失）
	finalPath := filepath.Join(dir, "output.dat")
	data, err := os.ReadFile(finalPath)
	if err != nil {
		t.Fatalf("read final file: %v", err)
	}
	lines := 0
	for _, b := range data {
		if b == '\n' {
			lines++
		}
	}
	if lines != 9 {
		t.Errorf("exported lines = %d, want 9 (no duplicates, no missing)", lines)
	}
}
