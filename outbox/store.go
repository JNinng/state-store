package outbox

import (
	"context"
	"fmt"
	"sort"
	"sync"
	"time"
)

// Store 是 outbox 消息的持久化抽象。
//
// 实现必须满足：
//   - Append 是原子操作——消息被完整持久化后才返回。
//   - FetchPending 按创建时间升序返回（FIFO）。
//   - MarkProcessed / MarkFailed 是幂等的——重复调用不报错。
type Store interface {
	// Append 追加一条消息到 outbox。
	Append(ctx context.Context, msg *Message) error

	// FetchPending 返回所有待处理的消息（status=pending 或 status=processing 且超时）。
	// 按 CreatedAt 升序排列。
	FetchPending(ctx context.Context) ([]*Message, error)

	// MarkProcessed 将消息标记为已完成。
	MarkProcessed(ctx context.Context, msgID string) error

	// MarkFailed 将消息标记为失败并记录错误信息。
	MarkFailed(ctx context.Context, msgID string, lastError string) error

	// UpdateStatus 原子更新消息的状态和重试计数。
	// 用于 Dispatcher 在处理过程中更新状态。
	UpdateStatus(ctx context.Context, msgID string, status MessageStatus, retries int, lastError string) error
}

// InMemoryStore 是 Store 的内存实现，适用于测试和单进程场景。
type InMemoryStore struct {
	mu       sync.RWMutex
	messages map[string]*Message
	order    []string // 消息 ID 按插入顺序排列
}

// NewInMemoryStore 创建一个新的 InMemoryStore。
func NewInMemoryStore() *InMemoryStore {
	return &InMemoryStore{
		messages: make(map[string]*Message),
		order:    make([]string, 0),
	}
}

// Append 追加消息。
func (s *InMemoryStore) Append(ctx context.Context, msg *Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	// 拷贝消息以避免外部修改
	clone := *msg
	if msg.Payload != nil {
		clone.Payload = make([]byte, len(msg.Payload))
		copy(clone.Payload, msg.Payload)
	}
	if clone.CreatedAt.IsZero() {
		clone.CreatedAt = time.Now()
	}
	if clone.Status == "" {
		clone.Status = StatusPending
	}

	if _, exists := s.messages[msg.ID]; exists {
		return fmt.Errorf("outbox: message %s already exists", msg.ID)
	}

	s.messages[msg.ID] = &clone
	s.order = append(s.order, msg.ID)
	return nil
}

// FetchPending 返回所有 pending 和超时的 processing 消息，按创建时间升序。
func (s *InMemoryStore) FetchPending(ctx context.Context) ([]*Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	s.mu.RLock()
	defer s.mu.RUnlock()

	var pending []*Message
	for _, id := range s.order {
		msg, ok := s.messages[id]
		if !ok {
			continue
		}
		if msg.Status == StatusPending || msg.Status == StatusProcessing {
			pending = append(pending, msg)
		}
	}

	// 按创建时间排序（稳定排序，同时间的保持插入顺序）
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})

	return pending, nil
}

// MarkProcessed 标记消息为已完成。
func (s *InMemoryStore) MarkProcessed(ctx context.Context, msgID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[msgID]
	if !ok {
		return nil // 幂等
	}

	now := time.Now()
	msg.Status = StatusCompleted
	msg.DeliveredAt = &now
	return nil
}

// MarkFailed 标记消息为失败。
func (s *InMemoryStore) MarkFailed(ctx context.Context, msgID string, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[msgID]
	if !ok {
		return nil // 幂等
	}

	msg.Status = StatusFailed
	msg.LastError = lastError
	return nil
}

// UpdateStatus 原子更新消息状态和重试计数。
func (s *InMemoryStore) UpdateStatus(ctx context.Context, msgID string, status MessageStatus, retries int, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	s.mu.Lock()
	defer s.mu.Unlock()

	msg, ok := s.messages[msgID]
	if !ok {
		return fmt.Errorf("outbox: message %s not found", msgID)
	}

	msg.Status = status
	msg.Retries = retries
	msg.LastError = lastError
	return nil
}

// GetMessage 按 ID 获取消息（用于测试和调试）。
func (s *InMemoryStore) GetMessage(id string) *Message {
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.messages[id]
}

// CountByStatus 返回指定状态的消息数量（用于测试和调试）。
func (s *InMemoryStore) CountByStatus(status MessageStatus) int {
	s.mu.RLock()
	defer s.mu.RUnlock()

	count := 0
	for _, msg := range s.messages {
		if msg.Status == status {
			count++
		}
	}
	return count
}
