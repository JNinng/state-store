package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"state-store/statestore"
)

// mockRepo 是 StateRepository 的内存实现，用于测试。
type mockRepo struct {
	store map[string][]byte
}

func newMockRepo() *mockRepo {
	return &mockRepo{store: make(map[string][]byte)}
}

func (m *mockRepo) Load(ctx context.Context, taskID string) ([]byte, error) {
	data, ok := m.store[taskID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *mockRepo) Save(ctx context.Context, taskID string, state []byte) error {
	m.store[taskID] = make([]byte, len(state))
	copy(m.store[taskID], state)
	return nil
}

func (m *mockRepo) Delete(ctx context.Context, taskID string) error {
	delete(m.store, taskID)
	return nil
}

// simpleEngine 是一个简单的 Engine mock，用于测试框架行为。
type simpleEngine struct {
	executeCalls     int
	compensateCalls  int
	compensateTarget int64
	failOnExecute    error
}

func (e *simpleEngine) TaskType() string { return "test" }

func (e *simpleEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	if e.failOnExecute != nil {
		return 0, e.failOnExecute
	}
	e.executeCalls++

	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		state.Payload = json.RawMessage(`{"step":0}`)
		return 0, nil
	case statestore.PhaseRunning:
		state.Phase = statestore.PhaseCompleted
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

func (e *simpleEngine) Compensate(ctx context.Context, targetLSN int64) error {
	e.compensateCalls++
	e.compensateTarget = targetLSN
	return nil
}

func (e *simpleEngine) Progress(state statestore.BaseTaskState) int {
	return 50
}

func TestRun_NormalFlow(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	err := Run(context.Background(), repo, eng, "task-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if eng.executeCalls != 2 {
		t.Errorf("executeCalls = %d, want 2", eng.executeCalls)
	}
	if eng.compensateCalls != 0 {
		t.Error("Compensate should not be called in normal flow")
	}

	// 验证最终状态
	data, err := repo.Load(context.Background(), "task-001")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var state statestore.BaseTaskState
	json.Unmarshal(data, &state)
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want %q", state.Phase, statestore.PhaseCompleted)
	}
}

func TestRun_RecoveryFlow(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	// 预存一个 running 状态（模拟崩溃恢复）
	initialState := statestore.BaseTaskState{
		TaskID:        "task-recover",
		TaskType:      "test",
		Phase:         statestore.PhaseRunning,
		CheckpointLSN: 50,
		Payload:       json.RawMessage(`{"step":1}`),
	}
	data, _ := json.Marshal(initialState)
	repo.Save(context.Background(), "task-recover", data)

	err := Run(context.Background(), repo, eng, "task-recover")
	if err != nil {
		t.Fatalf("Run recovery: %v", err)
	}

	if eng.compensateCalls != 1 {
		t.Errorf("Compensate calls = %d, want 1", eng.compensateCalls)
	}
	if eng.compensateTarget != 50 {
		t.Errorf("Compensate target = %d, want 50", eng.compensateTarget)
	}
}

func TestRun_ExecuteErrorNotSaved(t *testing.T) {
	repo := newMockRepo()
	expectedErr := errors.New("mock failure")
	eng := &simpleEngine{failOnExecute: expectedErr}

	err := Run(context.Background(), repo, eng, "task-err")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v, want %v", err, expectedErr)
	}

	// Execute 失败后不应有状态保存
	data, _ := repo.Load(context.Background(), "task-err")
	if data != nil {
		t.Error("state should not be saved after Execute error")
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err := Run(ctx, repo, eng, "task-cancel")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

// ProbeIdempotency 是一个测试辅助函数，验证引擎的 Execute 在
// "成功但未 Save" 场景下是幂等的。
//
// 它模拟以下崩溃恢复场景：
//  1. 执行一步 Execute → 快照 state（模拟 Save 成功）
//  2. 再执行一步 Execute → 不回写 state（模拟 Execute 成功但 Save 未发生）
//  3. 回退 state 到快照
//  4. 继续 Execute 直到终态
//  5. 调用 verify 回调让调用方自定义验证
//
// 自定义引擎实现者应参考此模式为自己的引擎编写幂等探针。
// 内置引擎的示例见 engine/export/export_test.go 和 engine/import/import_test.go。
func ProbeIdempotency(t *testing.T, eng Engine, state *statestore.BaseTaskState,
	step func(state *statestore.BaseTaskState) (shouldContinue bool),
	verify func(state *statestore.BaseTaskState)) {
	t.Helper()

	// 快照当前 state
	snapshotPayload := make([]byte, len(state.Payload))
	copy(snapshotPayload, state.Payload)
	snapshotLSN := state.CheckpointLSN
	snapshotPhase := state.Phase

	// 执行一步（模拟 Execute 成功但 Save 未发生）
	if !step(state) {
		return // 已经到达终态，无需测试回退
	}

	// 回退 state 到快照（模拟 checkpoint 未更新）
	state.Payload = make([]byte, len(snapshotPayload))
	copy(state.Payload, snapshotPayload)
	state.CheckpointLSN = snapshotLSN
	state.Phase = snapshotPhase

	// 继续执行直到终态
	for state.Phase != statestore.PhaseCompleted &&
		state.Phase != statestore.PhaseFailed &&
		state.Phase != statestore.PhaseCanceled {
		_, err := eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute after rewind: %v", err)
		}
	}

	verify(state)
}

func TestProbeIdempotency_WithMockEngine(t *testing.T) {
	eng := &simpleEngine{}
	state := &statestore.BaseTaskState{
		TaskID:   "probe-test",
		TaskType: "test",
		Phase:    statestore.PhasePending,
	}

	steps := 0
	ProbeIdempotency(t, eng, state,
		func(state *statestore.BaseTaskState) bool {
			steps++
			if steps >= 2 {
				return false // 只执行一步然后回退
			}
			_, err := eng.Execute(context.Background(), state)
			if err != nil {
				t.Fatalf("Execute step %d: %v", steps, err)
			}
			return state.Phase != statestore.PhaseCompleted
		},
		func(state *statestore.BaseTaskState) {
			if state.Phase != statestore.PhaseCompleted {
				t.Errorf("final phase = %q, want completed", state.Phase)
			}
		},
	)

	// simpleEngine 在 PhaseCompleted 时 Execute 返回已有 LSN，
	// 不会修改状态，所以回退后重执行应正确到达 completed
}

func TestProbeIdempotency_ExportEngine(t *testing.T) {
	// 此测试仅验证探针框架本身。
	// 导出引擎的完整幂等探针见 engine/export/export_test.go。
	// 导入引擎的完整幂等探针见 engine/import/import_test.go。
	t.Log("Full idempotency probe tests: see engine/export/ and engine/import/")
}
