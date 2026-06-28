package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"state-store/statestore"
	"state-store/task"
)

func TestRunWithOutbox_NormalFlow(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()
	outboxStore := NewInMemoryStore()

	eng := NewEngine("test_task", []*Message{
		{ID: "run-1", EventType: "send_email", Payload: json.RawMessage(`{"to":"a@b.com"}`)},
	})

	state, err := RunWithOutbox(ctx, repo, eng, "task-run-001", outboxStore)
	if err != nil {
		t.Fatalf("RunWithOutbox: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("phase = %s, want completed", state.Phase)
	}

	// 验证消息已写入 Store
	pending, _ := outboxStore.FetchPending(ctx)
	if len(pending) != 1 {
		t.Fatalf("pending count = %d, want 1", len(pending))
	}
	if pending[0].ID != "run-1" {
		t.Errorf("message ID = %s, want run-1", pending[0].ID)
	}
	if pending[0].Status != StatusPending {
		t.Errorf("status = %s, want pending", pending[0].Status)
	}
	if pending[0].TaskID != "task-run-001" {
		t.Errorf("TaskID = %s, want task-run-001", pending[0].TaskID)
	}
}

func TestRunWithOutbox_IdempotentRerun(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()
	outboxStore := NewInMemoryStore()

	eng := NewEngine("test_task", []*Message{
		{ID: "idem-1", EventType: "log"},
	})

	// 第一次运行
	_, err := RunWithOutbox(ctx, repo, eng, "task-idem-001", outboxStore)
	if err != nil {
		t.Fatalf("first RunWithOutbox: %v", err)
	}

	// 第二次运行（用新的 engine 实例模拟重启）
	eng2 := NewEngine("test_task", []*Message{
		{ID: "idem-1", EventType: "log"},
	})
	_, err = RunWithOutbox(ctx, repo, eng2, "task-idem-001", outboxStore)
	if err != nil {
		t.Fatalf("second RunWithOutbox: %v", err)
	}

	// Store 中不应有重复
	pending, _ := outboxStore.FetchPending(ctx)
	if len(pending) != 1 {
		t.Errorf("pending count = %d, want 1 (no duplicates)", len(pending))
	}
}

func TestRunWithOutbox_NoOutboxInPayload(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()
	outboxStore := NewInMemoryStore()

	// 使用一个不产生 outbox 消息的 engine
	eng := &noOutboxEngine{}

	state, err := RunWithOutbox(ctx, repo, eng, "task-no-obox", outboxStore)
	if err != nil {
		t.Fatalf("RunWithOutbox: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("phase = %s, want completed", state.Phase)
	}

	// Store 应为空
	pending, _ := outboxStore.FetchPending(ctx)
	if len(pending) != 0 {
		t.Errorf("pending count = %d, want 0", len(pending))
	}
}

func TestRunWithOutbox_PropagatesError(t *testing.T) {
	ctx := context.Background()
	repo := newMockRepo()
	outboxStore := NewInMemoryStore()

	// engine 返回错误
	eng := &failingEngine{}

	_, err := RunWithOutbox(ctx, repo, eng, "task-fail-001", outboxStore)
	if err == nil {
		t.Fatal("expected error from failing engine")
	}
}

func TestRunWithOutbox_ContextCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	repo := newMockRepo()
	outboxStore := NewInMemoryStore()
	eng := NewEngine("test_task", []*Message{{ID: "ctx-1", EventType: "log"}})

	_, err := RunWithOutbox(ctx, repo, eng, "task-ctx-001", outboxStore)
	if err == nil {
		t.Fatal("expected context error")
	}
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

// ---- 测试辅助 engine ----

// noOutboxEngine 不产生 outbox 消息的简单 engine。
type noOutboxEngine struct{}

func (e *noOutboxEngine) TaskType() string { return "no_outbox" }

func (e *noOutboxEngine) Execute(_ context.Context, state *statestore.BaseTaskState) (int64, error) {
	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		// 不写 outbox 到 Payload
		state.Payload = json.RawMessage(`{"custom":"data"}`)
		return 0, nil
	case statestore.PhaseRunning:
		state.Phase = statestore.PhaseCompleted
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

func (e *noOutboxEngine) Compensate(_ context.Context, _ int64) error { return nil }

func (e *noOutboxEngine) Progress(state statestore.BaseTaskState) int {
	if state.Phase == statestore.PhaseCompleted {
		return 100
	}
	return 50
}

var _ task.Engine = (*noOutboxEngine)(nil)

// failingEngine 始终返回错误的 engine。
type failingEngine struct{}

func (e *failingEngine) TaskType() string { return "failing" }

func (e *failingEngine) Execute(_ context.Context, _ *statestore.BaseTaskState) (int64, error) {
	return 0, errors.New("simulated failure")
}

func (e *failingEngine) Compensate(_ context.Context, _ int64) error { return nil }

func (e *failingEngine) Progress(_ statestore.BaseTaskState) int { return 0 }

var _ task.Engine = (*failingEngine)(nil)
