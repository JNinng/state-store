package outbox

import (
	"context"
	"encoding/json"
	"testing"

	"state-store/statestore"
	"state-store/task"
)

func TestEngine_TaskType(t *testing.T) {
	eng := NewEngine("my_task", nil)
	if eng.TaskType() != "my_task" {
		t.Errorf("TaskType() = %q, want %q", eng.TaskType(), "my_task")
	}
}

func TestEngine_Messages(t *testing.T) {
	msgs := []*Message{
		{ID: "msg-1", EventType: "send_email"},
		{ID: "msg-2", EventType: "notify_slack"},
	}
	eng := NewEngine("notify", msgs)

	got := eng.Messages()
	if len(got) != 2 {
		t.Fatalf("Messages() length = %d, want 2", len(got))
	}
	if got[0].ID != "msg-1" || got[1].ID != "msg-2" {
		t.Error("Messages() returned unexpected IDs")
	}
}

func TestEngine_Progress(t *testing.T) {
	eng := NewEngine("test", nil)

	// pending 阶段
	pendingState := statestore.BaseTaskState{Phase: statestore.PhasePending}
	if p := eng.Progress(pendingState); p != 50 {
		t.Errorf("Progress(pending) = %d, want 50", p)
	}

	// running 阶段
	runningState := statestore.BaseTaskState{Phase: statestore.PhaseRunning}
	if p := eng.Progress(runningState); p != 50 {
		t.Errorf("Progress(running) = %d, want 50", p)
	}

	// completed 阶段
	completedState := statestore.BaseTaskState{Phase: statestore.PhaseCompleted}
	if p := eng.Progress(completedState); p != 100 {
		t.Errorf("Progress(completed) = %d, want 100", p)
	}
}

func TestEngine_Execute_FullFlow(t *testing.T) {
	msgs := []*Message{
		{ID: "msg-1", EventType: "send_email", Payload: json.RawMessage(`{"to":"a@b.com"}`)},
	}
	eng := NewEngine("notify", msgs)

	// Step 1: pending → running
	state := &statestore.BaseTaskState{Phase: statestore.PhasePending}
	lsn, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute(pending): %v", err)
	}
	if lsn != 0 {
		t.Errorf("Execute(pending) LSN = %d, want 0", lsn)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Errorf("Phase after pending = %s, want running", state.Phase)
	}
	if state.Payload == nil {
		t.Fatal("Payload should not be nil after pending→running")
	}
	// 验证 Payload 包含 outbox 消息
	var payload struct {
		Outbox []*Message `json:"outbox"`
	}
	if err := json.Unmarshal(state.Payload, &payload); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(payload.Outbox) != 1 || payload.Outbox[0].ID != "msg-1" {
		t.Errorf("payload outbox = %+v, want [msg-1]", payload.Outbox)
	}

	// Step 2: running → completed
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute(running): %v", err)
	}
	if lsn != 100 {
		t.Errorf("Execute(running) LSN = %d, want 100", lsn)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("Phase after running = %s, want completed", state.Phase)
	}

	// Step 3: completed → 幂等，返回当前 CheckpointLSN
	// 注意：直接调用 Execute 时 CheckpointLSN 不会自动更新（由 task.Run 负责），
	// 此处 state.CheckpointLSN 仍为 0
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute(completed): %v", err)
	}
	if lsn != state.CheckpointLSN {
		t.Errorf("Execute(completed) LSN = %d, want current CheckpointLSN (%d)", lsn, state.CheckpointLSN)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("Phase should stay completed, got %s", state.Phase)
	}
}

func TestEngine_Compensate(t *testing.T) {
	eng := NewEngine("notify", nil)
	if err := eng.Compensate(context.Background(), 0); err != nil {
		t.Errorf("Compensate() should always return nil, got %v", err)
	}
}

func TestEngine_IntegrationWithTaskRun(t *testing.T) {
	msgs := []*Message{
		{ID: "int-1", EventType: "log"},
	}
	eng := NewEngine("integration_test", msgs)
	repo := newMockRepo()
	ctx := context.Background()

	err := task.Run(ctx, repo, eng, "task-int-001")
	if err != nil {
		t.Fatalf("task.Run: %v", err)
	}

	// 验证状态已持久化
	data, err := repo.Load(ctx, "task-int-001")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if data == nil {
		t.Fatal("state should be persisted after Run")
	}

	var state statestore.BaseTaskState
	if err := json.Unmarshal(data, &state); err != nil {
		t.Fatalf("unmarshal persisted state: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("persisted phase = %s, want completed", state.Phase)
	}

	// 验证 Messages() 可被调度层访问
	extracted := eng.Messages()
	if len(extracted) != 1 || extracted[0].ID != "int-1" {
		t.Errorf("Messages() = %+v, want [int-1]", extracted)
	}
}
