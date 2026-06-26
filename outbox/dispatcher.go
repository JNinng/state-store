package outbox

import (
	"context"
	"fmt"
	"log"
	"time"
)

// DispatcherOption 是 Dispatcher 的配置函数。
type DispatcherOption func(*Dispatcher)

// WithMaxRetries 设置全局默认最大重试次数。消息级别的 MaxRetries 优先。
func WithMaxRetries(n int) DispatcherOption {
	return func(d *Dispatcher) { d.maxRetries = n }
}

// WithPollInterval 设置轮询间隔。
func WithPollInterval(dur time.Duration) DispatcherOption {
	return func(d *Dispatcher) { d.pollInterval = dur }
}

// WithLogger 设置日志记录器。nil 表示静默。
func WithLogger(logger *log.Logger) DispatcherOption {
	return func(d *Dispatcher) { d.logger = logger }
}

// Dispatcher 轮询 Store 中的待处理消息并分发给注册的 Handler。
//
// 保证 at-least-once 投递：消息处理成功后标记为完成，
// 失败则根据重试策略进行重试。
//
// 使用方式：
//
//	// 方式一：批量分发（常用于 engine.Run 完成后）
//	dispatcher.DispatchPending(ctx)
//
//	// 方式二：后台轮询（独立 goroutine，持续分发）
//	go dispatcher.Start(ctx)
type Dispatcher struct {
	store        Store
	registry     *HandlerRegistry
	maxRetries   int
	pollInterval time.Duration
	logger       *log.Logger
}

// NewDispatcher 创建一个 Dispatcher。
func NewDispatcher(store Store, registry *HandlerRegistry, opts ...DispatcherOption) *Dispatcher {
	d := &Dispatcher{
		store:        store,
		registry:     registry,
		maxRetries:   3,
		pollInterval: 5 * time.Second,
	}
	for _, o := range opts {
		o(d)
	}
	return d
}

// DispatchPending 拉取并分发所有待处理的 outbox 消息。
// 返回成功处理的消息数和首个错误（如有）。
func (d *Dispatcher) DispatchPending(ctx context.Context) (int, error) {
	messages, err := d.store.FetchPending(ctx)
	if err != nil {
		return 0, fmt.Errorf("outbox dispatcher: fetch pending: %w", err)
	}

	processed := 0
	for _, msg := range messages {
		select {
		case <-ctx.Done():
			return processed, ctx.Err()
		default:
		}

		if err := d.dispatchOne(ctx, msg); err != nil {
			return processed, err
		}
		processed++
	}

	return processed, nil
}

// Start 启动后台轮询，持续分发待处理消息。
// 在 ctx 被取消时停止。
func (d *Dispatcher) Start(ctx context.Context) {
	d.logf("dispatcher started, poll interval: %v", d.pollInterval)

	ticker := time.NewTicker(d.pollInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			d.logf("dispatcher stopped")
			return
		case <-ticker.C:
			processed, err := d.DispatchPending(ctx)
			if err != nil && err != context.Canceled && err != context.DeadlineExceeded {
				d.logf("dispatch error: %v", err)
			}
			if processed > 0 {
				d.logf("processed %d messages", processed)
			}
		}
	}
}

// dispatchOne 分发单条消息。
func (d *Dispatcher) dispatchOne(ctx context.Context, msg *Message) error {
	handler := d.registry.Get(msg.EventType)
	if handler == nil {
		// 没有注册 handler——标记为失败
		errMsg := fmt.Sprintf("no handler registered for event type %q", msg.EventType)
		d.logf("message %s: %s", msg.ID, errMsg)
		return d.store.MarkFailed(ctx, msg.ID, errMsg)
	}

	maxRetries := msg.MaxRetries
	if maxRetries <= 0 {
		maxRetries = d.maxRetries
	}

	// 如果已达最大重试次数，标记失败
	if msg.Retries >= maxRetries {
		errMsg := fmt.Sprintf("max retries (%d) exceeded, last error: %s", maxRetries, msg.LastError)
		d.logf("message %s: %s", msg.ID, errMsg)
		return d.store.UpdateStatus(ctx, msg.ID, StatusFailed, msg.Retries, errMsg)
	}

	// 标记为处理中
	if err := d.store.UpdateStatus(ctx, msg.ID, StatusProcessing, msg.Retries, msg.LastError); err != nil {
		return fmt.Errorf("update status to processing: %w", err)
	}

	// 调用 handler
	if err := handler(msg); err != nil {
		retries := msg.Retries + 1
		errStr := err.Error()
		d.logf("message %s (event=%s, retry=%d/%d): %v", msg.ID, msg.EventType, retries, maxRetries, err)

		if retries >= maxRetries {
			// 达最大重试，使用 UpdateStatus 同时更新 retries 和 status
			if updateErr := d.store.UpdateStatus(ctx, msg.ID, StatusFailed, retries, errStr); updateErr != nil {
				return fmt.Errorf("mark failed after handler error: %w (original: %v)", updateErr, err)
			}
			return fmt.Errorf("outbox dispatcher: message %s failed after %d retries: %w", msg.ID, retries, err)
		}

		// 未达最大重试，回退到 pending 等待下次轮询
		if updateErr := d.store.UpdateStatus(ctx, msg.ID, StatusPending, retries, errStr); updateErr != nil {
			return fmt.Errorf("update status to pending: %w", updateErr)
		}
		return nil // 不是致命错误，等待下次轮询重试
	}

	// 成功——标记完成
	if err := d.store.MarkProcessed(ctx, msg.ID); err != nil {
		return fmt.Errorf("mark processed: %w", err)
	}
	d.logf("message %s (event=%s): processed successfully", msg.ID, msg.EventType)
	return nil
}

func (d *Dispatcher) logf(format string, args ...interface{}) {
	if d.logger != nil {
		d.logger.Printf(format, args...)
	}
}
