package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"os"
	"sync/atomic"
	"testing"
	"time"

	"state-store/engine"
	"state-store/statestore"
)

// ============================================================
// HandlerRegistry 测试
// ============================================================

func TestHandlerRegistry_RegisterAndGet(t *testing.T) {
	registry := NewHandlerRegistry()

	registry.Register("test_event", func(msg *Message) error {
		return nil
	})

	handler := registry.Get("test_event")
	if handler == nil {
		t.Fatal("handler should not be nil after Register")
	}

	h := registry.Get("nonexistent")
	if h != nil {
		t.Error("Get for unregistered event should return nil")
	}
}

func TestHandlerRegistry_Unregister(t *testing.T) {
	registry := NewHandlerRegistry()

	registry.Register("event_a", func(msg *Message) error { return nil })
	registry.Unregister("event_a")

	if h := registry.Get("event_a"); h != nil {
		t.Error("Get after Unregister should return nil")
	}

	// 删除不存在的 key 不 panic
	registry.Unregister("nonexistent")
}

func TestHandlerRegistry_Overwrite(t *testing.T) {
	registry := NewHandlerRegistry()

	var order []string
	registry.Register("event", func(msg *Message) error {
		order = append(order, "first")
		return nil
	})
	registry.Register("event", func(msg *Message) error {
		order = append(order, "second")
		return nil
	})

	registry.Get("event")(nil)
	if len(order) != 1 || order[0] != "second" {
		t.Errorf("overwrite failed: order=%v", order)
	}
}

// ============================================================
// InMemoryStore 测试
// ============================================================

func TestInMemoryStore_AppendAndFetch(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	msg1 := &Message{ID: "msg-1", EventType: "email", Status: StatusPending}
	msg2 := &Message{ID: "msg-2", EventType: "slack", Status: StatusPending}

	if err := store.Append(ctx, msg1); err != nil {
		t.Fatalf("Append msg1: %v", err)
	}
	if err := store.Append(ctx, msg2); err != nil {
		t.Fatalf("Append msg2: %v", err)
	}

	// 重复 ID 应报错
	if err := store.Append(ctx, msg1); err == nil {
		t.Error("Append with duplicate ID should error")
	}

	pending, err := store.FetchPending(ctx)
	if err != nil {
		t.Fatalf("FetchPending: %v", err)
	}
	if len(pending) != 2 {
		t.Fatalf("pending count = %d, want 2", len(pending))
	}
	if pending[0].ID != "msg-1" || pending[1].ID != "msg-2" {
		t.Errorf("order wrong: %v", pending)
	}
}

func TestInMemoryStore_MarkProcessed(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	store.Append(ctx, &Message{ID: "msg-1", Status: StatusPending})

	// 标记完成
	if err := store.MarkProcessed(ctx, "msg-1"); err != nil {
		t.Fatalf("MarkProcessed: %v", err)
	}

	if store.CountByStatus(StatusPending) != 0 {
		t.Error("pending count should be 0 after processed")
	}
	if store.CountByStatus(StatusCompleted) != 1 {
		t.Error("completed count should be 1")
	}

	// 幂等：重复标记不报错
	if err := store.MarkProcessed(ctx, "msg-1"); err != nil {
		t.Errorf("MarkProcessed should be idempotent: %v", err)
	}

	// 不存在的消息不报错
	if err := store.MarkProcessed(ctx, "nonexistent"); err != nil {
		t.Errorf("MarkProcessed for non-existent should not error: %v", err)
	}
}

func TestInMemoryStore_MarkFailed(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	store.Append(ctx, &Message{ID: "msg-1", Status: StatusPending})

	if err := store.MarkFailed(ctx, "msg-1", "something went wrong"); err != nil {
		t.Fatalf("MarkFailed: %v", err)
	}

	msg := store.GetMessage("msg-1")
	if msg.Status != StatusFailed {
		t.Errorf("status = %q, want failed", msg.Status)
	}
	if msg.LastError != "something went wrong" {
		t.Errorf("lastError = %q, want 'something went wrong'", msg.LastError)
	}
}

func TestInMemoryStore_UpdateStatus(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	store.Append(ctx, &Message{ID: "msg-1", EventType: "test"})

	if err := store.UpdateStatus(ctx, "msg-1", StatusProcessing, 2, "retrying"); err != nil {
		t.Fatalf("UpdateStatus: %v", err)
	}

	msg := store.GetMessage("msg-1")
	if msg.Status != StatusProcessing || msg.Retries != 2 || msg.LastError != "retrying" {
		t.Errorf("unexpected state: %+v", msg)
	}
}

func TestInMemoryStore_CountByStatus(t *testing.T) {
	store := NewInMemoryStore()
	ctx := context.Background()

	store.Append(ctx, &Message{ID: "a", Status: StatusPending})
	store.Append(ctx, &Message{ID: "b", Status: StatusPending})
	store.Append(ctx, &Message{ID: "c", Status: StatusCompleted})

	if n := store.CountByStatus(StatusPending); n != 2 {
		t.Errorf("pending = %d, want 2", n)
	}
	if n := store.CountByStatus(StatusCompleted); n != 1 {
		t.Errorf("completed = %d, want 1", n)
	}
}

// ============================================================
// Dispatcher 测试
// ============================================================

func TestDispatcher_DispatchPending(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx := context.Background()

	var processedMsgs []string
	registry.Register("send_email", func(msg *Message) error {
		processedMsgs = append(processedMsgs, msg.ID)
		return nil
	})
	registry.Register("send_slack", func(msg *Message) error {
		processedMsgs = append(processedMsgs, msg.ID)
		return nil
	})

	store.Append(ctx, &Message{ID: "email-1", EventType: "send_email", Status: StatusPending})
	store.Append(ctx, &Message{ID: "slack-1", EventType: "send_slack", Status: StatusPending})
	store.Append(ctx, &Message{ID: "email-2", EventType: "send_email", Status: StatusPending})

	dispatcher := NewDispatcher(store, registry)
	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}

	if processed != 3 {
		t.Errorf("processed = %d, want 3", processed)
	}
	if len(processedMsgs) != 3 {
		t.Errorf("handler calls = %d, want 3", len(processedMsgs))
	}
	if store.CountByStatus(StatusCompleted) != 3 {
		t.Errorf("completed = %d, want 3", store.CountByStatus(StatusCompleted))
	}
}

func TestDispatcher_NoHandler(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx := context.Background()

	store.Append(ctx, &Message{ID: "unknown", EventType: "unknown_type", Status: StatusPending})

	dispatcher := NewDispatcher(store, registry)
	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if processed != 1 {
		t.Errorf("processed = %d, want 1", processed)
	}

	msg := store.GetMessage("unknown")
	if msg.Status != StatusFailed {
		t.Errorf("status = %q, want failed", msg.Status)
	}
	if msg.LastError == "" {
		t.Error("LastError should not be empty for unhandled message")
	}
}

func TestDispatcher_RetryAndMaxRetries(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx := context.Background()

	var callCount int32
	registry.Register("flaky_event", func(msg *Message) error {
		atomic.AddInt32(&callCount, 1)
		return errors.New("temporary failure")
	})

	store.Append(ctx, &Message{
		ID: "flaky-1", EventType: "flaky_event", Status: StatusPending,
		MaxRetries: 3,
	})

	dispatcher := NewDispatcher(store, registry, WithMaxRetries(3))

	// 第一次调度：handler 失败，回退到 pending（retries=1）
	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if processed != 1 {
		t.Errorf("first dispatch processed = %d, want 1", processed)
	}

	msg := store.GetMessage("flaky-1")
	if msg.Status != StatusPending {
		t.Errorf("after first fail status = %q, want pending", msg.Status)
	}
	if msg.Retries != 1 {
		t.Errorf("after first fail retries = %d, want 1", msg.Retries)
	}

	// 第二次调度：retries=1 → handler 失败 → retries=2
	dispatcher.DispatchPending(ctx)
	msg = store.GetMessage("flaky-1")
	if msg.Retries != 2 {
		t.Errorf("after second fail retries = %d, want 2", msg.Retries)
	}

	// 第三次调度：retries=2 → handler 失败 → retries=3 → 达到 max → 标记 failed
	dispatcher.DispatchPending(ctx)
	msg = store.GetMessage("flaky-1")
	if msg.Status != StatusFailed {
		t.Errorf("after max retries status = %q, want failed", msg.Status)
	}
	if msg.Retries != 3 {
		t.Errorf("after max retries retries = %d, want 3", msg.Retries)
	}

	if atomic.LoadInt32(&callCount) != 3 {
		t.Errorf("handler calls = %d, want 3", callCount)
	}
}

func TestDispatcher_HandlerRecovers(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx := context.Background()

	var callCount int32
	registry.Register("recover_event", func(msg *Message) error {
		count := atomic.AddInt32(&callCount, 1)
		if count < 3 {
			return errors.New("still failing")
		}
		return nil // 第三次成功
	})

	store.Append(ctx, &Message{
		ID: "recover-1", EventType: "recover_event", Status: StatusPending,
		MaxRetries: 5,
	})

	dispatcher := NewDispatcher(store, registry)

	// 执行 3 次调度（每次 handler 失败，状态回 pending）
	for i := 0; i < 2; i++ {
		dispatcher.DispatchPending(ctx)
	}
	// 第三次：handler 成功
	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if processed != 1 {
		t.Errorf("third dispatch processed = %d, want 1", processed)
	}

	msg := store.GetMessage("recover-1")
	if msg.Status != StatusCompleted {
		t.Errorf("status = %q, want completed", msg.Status)
	}
}

func TestDispatcher_ContextCancellation(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()

	store.Append(context.Background(), &Message{ID: "msg", EventType: "test", Status: StatusPending})

	dispatcher := NewDispatcher(store, registry)
	_, err := dispatcher.DispatchPending(ctx)
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}

// ============================================================
// Saga 测试
// ============================================================

func TestSagaCoordinator_NormalFlow(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var stepOrder []string

	saga := &Saga{
		Name: "test_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_forward")
					actx["step1_result"] = "done"
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_compensate")
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_forward")
					if actx["step1_result"] != "done" {
						t.Error("step_2 should have access to step_1 result")
					}
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_compensate")
					return nil
				},
			},
			{
				Name: "step_3",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_forward")
					return nil
				},
				Compensation: nil, // 幂等步骤，无需补偿
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if len(stepOrder) != 3 {
		t.Errorf("stepOrder length = %d, want 3", len(stepOrder))
	}
	if stepOrder[0] != "1_forward" || stepOrder[1] != "2_forward" || stepOrder[2] != "3_forward" {
		t.Errorf("unexpected order: %v", stepOrder)
	}

	// 验证没有步骤被补偿
	for i, ss := range state.StepStatuses {
		if ss != StepCompleted {
			t.Errorf("step %d status = %q, want completed", i, ss)
		}
	}
}

func TestSagaCoordinator_CompensationOnFailure(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var stepOrder []string

	saga := &Saga{
		Name: "compensate_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_forward")
					actx["balance"] = 100
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_compensate")
					// 退款
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_forward")
					return errors.New("step_2 failed: insufficient inventory")
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_compensate")
					return nil
				},
			},
			{
				Name: "step_3",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_forward")
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_compensate")
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-comp-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaFailed {
		t.Errorf("status = %q, want failed", state.Status)
	}

	// 应该执行：1_forward → 2_forward(fail) → 1_compensate（逆序）
	expected := []string{"1_forward", "2_forward", "1_compensate"}
	if len(stepOrder) != 3 {
		t.Fatalf("stepOrder length = %d, want 3: %v", len(stepOrder), stepOrder)
	}
	for i, want := range expected {
		if stepOrder[i] != want {
			t.Errorf("stepOrder[%d] = %q, want %q", i, stepOrder[i], want)
		}
	}

	// step_1 应被补偿
	if state.StepStatuses[0] != StepCompensated {
		t.Errorf("step_0 status = %q, want compensated", state.StepStatuses[0])
	}
	// step_2 应标记失败（未补偿，因为它本身失败了）
	if state.StepStatuses[1] != StepFailed {
		t.Errorf("step_1 status = %q, want failed", state.StepStatuses[1])
	}
	// step_3 仍为 pending（从未执行）
	if state.StepStatuses[2] != StepPending {
		t.Errorf("step_2 status = %q, want pending", state.StepStatuses[2])
	}
}

func TestSagaCoordinator_RetryOnFailure(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var attempts int
	saga := &Saga{
		Name:              "retry_saga",
		DefaultMaxRetries: 2,
		Steps: []SagaStep{
			{
				Name:       "flaky_step",
				MaxRetries: 3,
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					attempts++
					if attempts < 3 {
						return errors.New("temporary error")
					}
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-retry-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestSagaCoordinator_ResumeCompletedSaga(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	saga := &Saga{
		Name: "idempotent_saga",
		Steps: []SagaStep{
			{
				Name: "only_step",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	// 先完整执行
	_, err := coordinator.Run(ctx, saga, "resume-test")
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Resume 已完成 saga——应直接返回完成状态
	state, err := coordinator.Resume(ctx, saga, "resume-test")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
}

func TestSagaCoordinator_ContextCancellation(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx, cancel := context.WithCancel(context.Background())

	saga := &Saga{
		Name: "cancel_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					cancel() // 第一步就取消 ctx
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "cancel-001")
	if err == nil {
		// 上下文取消后，继续执行会被拦截
		t.Log("context cancelled after step 1")
	}
	_ = state
}

func TestRunSaga_Convenience(t *testing.T) {
	ctx := context.Background()
	saga := &Saga{
		Name: "convenience",
		Steps: []SagaStep{
			{
				Name: "simple",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	state, err := RunSaga(ctx, saga, "conv-001")
	if err != nil {
		t.Fatalf("RunSaga: %v", err)
	}
	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
}

func TestInMemorySagaStore(t *testing.T) {
	store := NewInMemorySagaStore()
	ctx := context.Background()

	// Load 不存在的 saga
	state, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state != nil {
		t.Error("Load should return nil for non-existent saga")
	}

	// Save
	original := &SagaState{
		SagaID:   "test",
		SagaName: "test_saga",
		Status:   SagaRunning,
		ActionCtx: map[string]interface{}{
			"key": "value",
		},
	}
	if err := store.Save(ctx, original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 修改原始——不影响已保存的
	original.ActionCtx["key"] = "modified"

	// Load 验证隔离
	loaded, err := store.Load(ctx, "test")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.ActionCtx["key"] != "value" {
		t.Errorf("ActionCtx not isolated: %v", loaded.ActionCtx)
	}

	// Delete
	if err := store.Delete(ctx, "test"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	after, _ := store.Load(ctx, "test")
	if after != nil {
		t.Error("after Delete, Load should return nil")
	}
}

// ============================================================
// 集成示例：调度层如何使用 Outbox/Saga 模式
// ============================================================

// mockRepo 是 StateRepository 的内存实现（与 engine_test.go 一致）。
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

// TestSchedulingLayer_OutboxPattern 演示调度层如何使用 Outbox 模式。
//
// 场景：导出任务完成后需要发送邮件通知。
// 邮件发送是不可逆操作，不能直接在 engine.Execute 中执行。
// 正确做法：
//  1. 引擎在 Payload 中写入 outbox 意图记录
//  2. 调度层在 engine.Run 成功后将 outbox 记录写入 Store
//  3. Dispatcher 分发 outbox 消息，真正执行发邮件操作
func TestSchedulingLayer_OutboxPattern(t *testing.T) {
	// ---- 准备 ----
	ctx := context.Background()

	// Store：持久化待发消息
	outboxStore := NewInMemoryStore()

	// HandlerRegistry：注册真正的不可逆操作
	var actualEmails []string
	registry := NewHandlerRegistry()
	registry.Register("send_email", func(msg *Message) error {
		var payload map[string]string
		json.Unmarshal(msg.Payload, &payload)
		actualEmails = append(actualEmails, payload["to"])
		return nil
	})

	// ---- 引擎（仅做可逆操作） ----
	// 引擎的 Payload 中包含 outbox 意图记录
	eng := &outboxAwareEngine{
		outboxMessages: []*Message{
			{ID: "email-1", EventType: "send_email", Payload: json.RawMessage(`{"to":"admin@example.com"}`)},
		},
	}

	repo := newMockRepo()

	// ---- 调度层 ----
	// 步骤 1：执行引擎（checkpoint 保护的可逆操作）
	err := engine.Run(ctx, repo, eng, "task-with-outbox")
	if err != nil {
		t.Fatalf("engine.Run: %v", err)
	}

	// 步骤 2：提取 outbox 意图并写入 Store
	// （从最终的 task state Payload 中提取，此处直接从引擎获取）
	for _, msg := range eng.outboxMessages {
		msg.Status = StatusPending
		if err := outboxStore.Append(ctx, msg); err != nil {
			t.Fatalf("append outbox: %v", err)
		}
	}

	// 步骤 3：分发 outbox 消息（执行不可逆操作）
	dispatcher := NewDispatcher(outboxStore, registry)
	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("dispatch: %v", err)
	}

	// ---- 验证 ----
	if processed != 1 {
		t.Errorf("processed = %d, want 1", processed)
	}
	if len(actualEmails) != 1 || actualEmails[0] != "admin@example.com" {
		t.Errorf("actual emails = %v, want [admin@example.com]", actualEmails)
	}
	if outboxStore.CountByStatus(StatusCompleted) != 1 {
		t.Error("outbox message should be marked completed")
	}
}

// outboxAwareEngine 是一个在 Payload 中嵌入 outbox 消息的示例引擎。
type outboxAwareEngine struct {
	outboxMessages []*Message
}

func (e *outboxAwareEngine) TaskType() string { return "outbox_example" }

func (e *outboxAwareEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		// 在 Payload 中写入 outbox 意图
		outboxData, _ := json.Marshal(e.outboxMessages)
		state.Payload = json.RawMessage(fmt.Sprintf(`{"outbox":%s}`, string(outboxData)))
		return 0, nil
	case statestore.PhaseRunning:
		state.Phase = statestore.PhaseCompleted
		state.Message = "done"
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

func (e *outboxAwareEngine) Compensate(ctx context.Context, targetLSN int64) error {
	return nil
}

func (e *outboxAwareEngine) Progress(state statestore.BaseTaskState) int {
	return 50
}

var _ engine.Engine = (*outboxAwareEngine)(nil)

// TestSchedulingLayer_SagaPattern 演示调度层如何使用 Saga 模式。
//
// 场景：订单结账流程——扣减库存 → 扣款 → 发确认邮件。
// 如果中间某步失败，需补偿已完成的步骤。
func TestSchedulingLayer_SagaPattern(t *testing.T) {
	ctx := context.Background()

	// 模拟外部系统状态
	inventory := map[string]int{"item-1": 10}
	balance := 100
	var emailsSent []string

	saga := &Saga{
		Name:              "order_checkout",
		DefaultMaxRetries: 1,
		Steps: []SagaStep{
			{
				Name: "reserve_inventory",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					itemID := "item-1"
					if inventory[itemID] <= 0 {
						return errors.New("out of stock")
					}
					inventory[itemID]--
					actx["reserved_item"] = itemID
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					// 释放库存
					itemID := actx["reserved_item"].(string)
					inventory[itemID]++
					return nil
				},
			},
			{
				Name: "deduct_balance",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					amount := 50
					if balance < amount {
						return errors.New("insufficient balance")
					}
					balance -= amount
					actx["deducted_amount"] = amount
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					// 退款
					amount := actx["deducted_amount"].(int)
					balance += amount
					return nil
				},
			},
			{
				Name: "send_confirmation",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					// 发邮件是不可逆操作，但 Saga 中它放在最后一步
					// （因为前面步骤都可补偿）
					emailsSent = append(emailsSent, "user@example.com")
					return nil
				},
				Compensation: nil, // 邮件一旦发出无法补偿（可另发"取消通知"）
			},
		},
	}

	// 正常执行
	coordinator := NewSagaCoordinator(NewInMemorySagaStore())
	state, err := coordinator.Run(ctx, saga, "checkout-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if inventory["item-1"] != 9 {
		t.Errorf("inventory = %d, want 9", inventory["item-1"])
	}
	if balance != 50 {
		t.Errorf("balance = %d, want 50", balance)
	}
	if len(emailsSent) != 1 {
		t.Errorf("emails = %d, want 1", len(emailsSent))
	}
}

// TestSchedulingLayer_SagaCompensationInAction 演示 Saga 失败时的补偿行为。
func TestSchedulingLayer_SagaCompensationInAction(t *testing.T) {
	ctx := context.Background()

	inventory := map[string]int{"item-1": 10}
	balance := 0 // 余额不足，将在第 2 步失败

	saga := &Saga{
		Name: "failed_checkout",
		Steps: []SagaStep{
			{
				Name: "reserve_inventory",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					inventory["item-1"]--
					actx["reserved_item"] = "item-1"
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					inventory["item-1"]++
					return nil
				},
			},
			{
				Name: "deduct_balance",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					if balance < 50 {
						return errors.New("insufficient balance")
					}
					balance -= 50
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					balance += 50
					return nil
				},
			},
		},
	}

	coordinator := NewSagaCoordinator(NewInMemorySagaStore())
	state, err := coordinator.Run(ctx, saga, "failed-checkout-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaFailed {
		t.Errorf("status = %q, want failed", state.Status)
	}

	// 库存应该恢复（补偿生效）
	if inventory["item-1"] != 10 {
		t.Errorf("inventory = %d, want 10 (compensation should restore)", inventory["item-1"])
	}

	// step_0 应被补偿
	if state.StepStatuses[0] != StepCompensated {
		t.Errorf("step_0 = %q, want compensated", state.StepStatuses[0])
	}
	// step_1 应标记失败
	if state.StepStatuses[1] != StepFailed {
		t.Errorf("step_1 = %q, want failed", state.StepStatuses[1])
	}
}

// ============================================================
// 调度层完整示例：带日志的 Outbox Dispatcher
// ============================================================

func TestDispatcher_WithLogger(t *testing.T) {
	store := NewInMemoryStore()
	registry := NewHandlerRegistry()
	ctx := context.Background()

	registry.Register("log_event", func(msg *Message) error {
		return nil
	})

	store.Append(ctx, &Message{ID: "log-1", EventType: "log_event", Status: StatusPending})

	// 使用标准库 log.Logger
	logger := log.New(os.Stderr, "[outbox] ", log.LstdFlags)
	dispatcher := NewDispatcher(store, registry, WithLogger(logger), WithPollInterval(10*time.Second))

	processed, err := dispatcher.DispatchPending(ctx)
	if err != nil {
		t.Fatalf("DispatchPending: %v", err)
	}
	if processed != 1 {
		t.Errorf("processed = %d, want 1", processed)
	}
}

// ============================================================
// 编译期接口检查
// ============================================================

// 确保 InMemoryStore 实现了 Store 接口
var _ Store = (*InMemoryStore)(nil)

// 确保 InMemorySagaStore 实现了 SagaStore 接口
var _ SagaStore = (*InMemorySagaStore)(nil)
