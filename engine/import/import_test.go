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

func TestEngine_NormalFlow(t *testing.T) {
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

func TestEngine_Compensate(t *testing.T) {
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

func TestEngine_ResumeFromOffset(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")

	// 5 行数据
	rows := []phys.Row{
		{"id": "1"}, {"id": "2"}, {"id": "3"}, {"id": "4"}, {"id": "5"},
	}
	f, _ := os.Create(srcPath)
	for _, r := range rows {
		data, _ := json.Marshal(r)
		f.Write(append(data, '\n'))
	}
	f.Close()

	// 先执行前 2 行（batchSize=2），然后中断
	target1 := &stubDataTarget{}
	eng1 := New(srcPath, target1, WithBatchSize(2))

	state := &statestore.BaseTaskState{
		TaskID:   "import-resume",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	// Pending → Running
	_, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}

	// 第一批（2 行）
	_, err = eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute batch 1: %v", err)
	}
	if len(target1.inserted) != 2 {
		t.Errorf("first batch rows = %d, want 2", len(target1.inserted))
	}
	savedPayload := make([]byte, len(state.Payload))
	copy(savedPayload, state.Payload)

	// === 模拟崩溃恢复：用新 target 和新 engine ===
	target2 := &stubDataTarget{}
	eng2 := New(srcPath, target2, WithBatchSize(2))

	state2 := &statestore.BaseTaskState{
		TaskID:   "import-resume",
		TaskType: "import",
		Phase:    statestore.PhaseRunning,
		Payload:  savedPayload,
	}

	// Compensate 验证源文件完整
	err = eng2.Compensate(context.Background(), state2.CheckpointLSN)
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}

	// 继续执行直到完成（应跳过已读的 2 行，读剩余 3 行）
	for state2.Phase != statestore.PhaseCompleted {
		_, err = eng2.Execute(context.Background(), state2)
		if err != nil {
			t.Fatalf("Execute after resume: %v", err)
		}
	}

	// 验证：只导入了剩余 3 行（无重复）
	if len(target2.inserted) != 3 {
		t.Errorf("resumed inserted rows = %d, want 3 (should skip first 2)", len(target2.inserted))
	}
	// 验证导入的是正确的行
	if target2.inserted[0]["id"] != "3" {
		t.Errorf("first resumed row id = %v, want 3", target2.inserted[0]["id"])
	}
	if target2.inserted[2]["id"] != "5" {
		t.Errorf("last resumed row id = %v, want 5", target2.inserted[2]["id"])
	}
}

func TestEngine_ResumeToEOF(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")

	// 只有 2 行，batchSize=5（比数据多）
	rows := []phys.Row{
		{"id": "1"}, {"id": "2"},
	}
	f, _ := os.Create(srcPath)
	for _, r := range rows {
		data, _ := json.Marshal(r)
		f.Write(append(data, '\n'))
	}
	f.Close()

	// 先执行一批（读取全部数据到 EOF）
	target1 := &stubDataTarget{}
	eng1 := New(srcPath, target1, WithBatchSize(5))

	state := &statestore.BaseTaskState{
		TaskID:   "import-eof-resume",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	// Pending → Running
	eng1.Execute(context.Background(), state)
	// 一批读取全部并完成
	_, err := eng1.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Fatalf("phase = %q, want completed", state.Phase)
	}
	if len(target1.inserted) != 2 {
		t.Errorf("inserted = %d, want 2", len(target1.inserted))
	}

	// 模拟恢复：已经 completed 的状态再次 Execute 应该是安全的
	target2 := &stubDataTarget{}
	eng2 := New(srcPath, target2, WithBatchSize(5))

	// 用已完成的状态再次 Execute
	_, err = eng2.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute on completed state: %v", err)
	}
	// 不应再插入任何行
	if len(target2.inserted) != 0 {
		t.Errorf("should not insert rows on completed state, got %d", len(target2.inserted))
	}
}

func TestEngine_Progress(t *testing.T) {
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
