// Package outbox 在调度层提供 Outbox 和 Saga 模式支持。
//
// 背景：task.Engine.Execute 的副作用约束要求 Execute 的物理副作用必须可补偿——
// 可截断（文件）或可幂等重放（数据库 UPSERT）。发邮件、扣款、发送消息队列等
// 不可逆操作不应直接在 Execute 中执行。
//
// 本包提供两种模式在调度层（调用 engine.Run 的层级）处理不可逆操作：
//
//	Outbox 模式：引擎在 Payload 中写入"意图记录"，调度层在 engine.Run 成功后
//	            从 Payload 提取 outbox 消息并分发。保证 at-least-once 投递。
//
//	Saga 模式：将多步骤分布式事务拆分为一系列本地事务，每步有正向操作和补偿操作。
//	          任一步骤失败时，按逆序执行已完成步骤的补偿操作。
//
// 两种模式都利用了 framework 的 checkpoint 机制——outbox/Saga 的状态与任务状态
// 一起原子持久化，崩溃恢复时不会丢失或重复。
package outbox

import (
	"encoding/json"
	"time"
)

// MessageStatus 表示 outbox 消息的处理状态。
type MessageStatus string

const (
	// StatusPending 表示消息尚未被分发。
	StatusPending MessageStatus = "pending"

	// StatusProcessing 表示消息正在被处理。
	StatusProcessing MessageStatus = "processing"

	// StatusCompleted 表示消息已成功处理。
	StatusCompleted MessageStatus = "completed"

	// StatusFailed 表示消息处理失败且已达最大重试次数。
	StatusFailed MessageStatus = "failed"
)

// Message 是一条待分发的 outbox 记录。
// 引擎在 Execute 中将不可逆操作的意图编码为 OutboxMessage，
// 写入 Payload，随 checkpoint 一起原子持久化。
type Message struct {
	// ID 是消息的唯一标识（建议用 UUID）。
	ID string `json:"id"`

	// TaskID 关联的任务 ID。
	TaskID string `json:"task_id"`

	// EventType 是消息类型，用于路由到对应的 Handler。
	// 示例："send_email", "notify_slack", "deduct_balance"。
	EventType string `json:"event_type"`

	// Payload 是 Handler 处理消息所需的业务数据。
	Payload json.RawMessage `json:"payload"`

	// Status 表示当前处理状态。
	Status MessageStatus `json:"status"`

	// Retries 是已重试次数。
	Retries int `json:"retries"`

	// MaxRetries 是最大重试次数。0 表示使用 Dispatcher 默认值。
	MaxRetries int `json:"max_retries"`

	// CreatedAt 是消息创建时间。
	CreatedAt time.Time `json:"created_at"`

	// LastError 记录最近一次处理失败的错误信息。
	LastError string `json:"last_error,omitempty"`

	// DeliveredAt 记录成功投递的时间。
	DeliveredAt *time.Time `json:"delivered_at,omitempty"`
}

// Handler 处理特定 EventType 的 outbox 消息。
// 实现必须保证幂等性——同一条消息可能被投递多次（at-least-once）。
// 返回 error 表示处理失败，Dispatcher 会根据策略重试。
type Handler func(msg *Message) error

// HandlerRegistry 将 EventType 映射到对应的 Handler。
// 使用 Register/Unregister 动态管理。
type HandlerRegistry struct {
	handlers map[string]Handler
}

// NewHandlerRegistry 创建一个空的 HandlerRegistry。
func NewHandlerRegistry() *HandlerRegistry {
	return &HandlerRegistry{
		handlers: make(map[string]Handler),
	}
}

// Register 为指定 EventType 注册 Handler。重复注册会覆盖。
func (r *HandlerRegistry) Register(eventType string, handler Handler) {
	r.handlers[eventType] = handler
}

// Unregister 注销指定 EventType 的 Handler。
func (r *HandlerRegistry) Unregister(eventType string) {
	delete(r.handlers, eventType)
}

// Get 获取指定 EventType 的 Handler。不存在返回 nil。
func (r *HandlerRegistry) Get(eventType string) Handler {
	return r.handlers[eventType]
}
