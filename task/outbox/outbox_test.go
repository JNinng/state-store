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

	"state-store/statestore"
	"state-store/task"
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
// 集成示例：调度层如何使用 Outbox 模式
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
	err := task.Run(ctx, repo, eng, "task-with-outbox")
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

var _ task.Engine = (*outboxAwareEngine)(nil)

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
