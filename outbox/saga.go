package outbox

import (
	"context"
	"encoding/json"
	"fmt"
	"sync"
	"time"
)

// SagaStatus 表示 Saga 的整体执行状态。
type SagaStatus string

const (
	SagaPending     SagaStatus = "pending"
	SagaRunning     SagaStatus = "running"
	SagaCompensating SagaStatus = "compensating"
	SagaCompleted   SagaStatus = "completed"
	SagaFailed      SagaStatus = "failed"
)

// StepStatus 表示 Saga 中单个步骤的执行状态。
type StepStatus string

const (
	StepPending     StepStatus = "pending"
	StepRunning     StepStatus = "running"
	StepCompleted   StepStatus = "completed"
	StepFailed      StepStatus = "failed"
	StepCompensated StepStatus = "compensated"
)

// SagaStep 定义 Saga 中的一个步骤。
// Action 是正向操作，Compensation 是补偿（回滚）操作。
// 如果步骤本身就是幂等的（如 UPSERT、创建目录），Compensation 可以为 nil。
type SagaStep struct {
	// Name 是步骤的可读名称（如 "deduct_balance", "create_order"）。
	Name string `json:"name"`

	// Action 执行正向操作。返回 error 表示步骤失败。
	// actionCtx 携带 saga 的共享上下文，允许步骤间传递数据。
	Action func(ctx context.Context, actionCtx map[string]interface{}) error `json:"-"`

	// Compensation 执行补偿操作。在 Saga 前进失败时调用。
	// 接收与 Action 相同的 actionCtx，以访问已计算的数据。
	Compensation func(ctx context.Context, actionCtx map[string]interface{}) error `json:"-"`

	// MaxRetries 是该步骤的最大重试次数。0 使用 SagaCoordinator 默认值。
	MaxRetries int `json:"max_retries"`

	// Timeout 是该步骤的超时时间。0 表示无超时。
	Timeout time.Duration `json:"-"`
}

// SagaState 是 Saga 执行状态的持久化表示。
// 实现 SagaStore 的存储会持久化此结构。
type SagaState struct {
	// SagaID 是 Saga 实例的唯一标识。
	SagaID string `json:"saga_id"`

	// SagaName 是 Saga 定义的名称（如 "order_checkout"）。
	SagaName string `json:"saga_name"`

	// Status 是当前执行状态。
	Status SagaStatus `json:"status"`

	// CurrentStep 是当前执行到的步骤索引（0-based）。
	CurrentStep int `json:"current_step"`

	// StepStatuses 是各步骤的执行状态。索引对应 Steps 数组。
	StepStatuses []StepStatus `json:"step_statuses"`

	// StepErrors 记录各步骤的错误信息。
	StepErrors []string `json:"step_errors,omitempty"`

	// ActionCtx 是步骤间共享的数据上下文。
	// 步骤通过此 map 传递中间结果。
	ActionCtx map[string]interface{} `json:"action_ctx,omitempty"`

	// CreatedAt 是 Saga 创建时间。
	CreatedAt time.Time `json:"created_at"`

	// UpdatedAt 是 Saga 最后更新时间。
	UpdatedAt time.Time `json:"updated_at"`
}

// SagaStore 持久化 Saga 执行状态。
type SagaStore interface {
	// Load 加载 Saga 状态。不存在返回 nil, nil。
	Load(ctx context.Context, sagaID string) (*SagaState, error)

	// Save 保存 Saga 状态（完整替换）。
	Save(ctx context.Context, state *SagaState) error

	// Delete 删除 Saga 状态。
	Delete(ctx context.Context, sagaID string) error
}

// InMemorySagaStore 是 SagaStore 的内存实现。
type InMemorySagaStore struct {
	mu     sync.RWMutex
	sagas  map[string]*SagaState
}

// NewInMemorySagaStore 创建新的 InMemorySagaStore。
func NewInMemorySagaStore() *InMemorySagaStore {
	return &InMemorySagaStore{
		sagas: make(map[string]*SagaState),
	}
}

func (s *InMemorySagaStore) Load(ctx context.Context, sagaID string) (*SagaState, error) {
	s.mu.RLock()
	defer s.mu.RUnlock()
	state, ok := s.sagas[sagaID]
	if !ok {
		return nil, nil
	}
	clone := *state
	return &clone, nil
}

func (s *InMemorySagaStore) Save(ctx context.Context, state *SagaState) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	clone := *state
	// 深拷贝 ActionCtx（简单值类型，无需递归拷贝）
	if state.ActionCtx != nil {
		clone.ActionCtx = make(map[string]interface{}, len(state.ActionCtx))
		for k, v := range state.ActionCtx {
			clone.ActionCtx[k] = v
		}
	}
	s.sagas[state.SagaID] = &clone
	return nil
}

func (s *InMemorySagaStore) Delete(ctx context.Context, sagaID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	delete(s.sagas, sagaID)
	return nil
}

// Saga 定义了一个 Saga 的步骤序列。
type Saga struct {
	// Name 是 Saga 的名称。
	Name string `json:"name"`

	// Steps 是 Saga 的步骤序列。按数组顺序执行。
	Steps []SagaStep `json:"steps"`

	// DefaultMaxRetries 是步骤级别的默认最大重试次数。
	DefaultMaxRetries int `json:"default_max_retries"`
}

// SagaCoordinator 编排 Saga 的执行。
//
// 执行流程：
//  1. 按顺序执行各步骤的 Action
//  2. 若某步骤 Action 失败→从当前步骤-1 开始逆序执行 Compensation
//  3. 所有步骤 Action 成功→Saga 完成
//  4. 补偿过程中某个 Compensation 失败→记录失败但继续补偿其余步骤
//     （补偿失败属于系统异常，需要人工介入或后台重试）
type SagaCoordinator struct {
	store      SagaStore
	logger     Logger
}

// Logger 是简单的日志接口。
type Logger interface {
	Printf(format string, args ...interface{})
}

type stdLogger struct{}

func (l *stdLogger) Printf(format string, args ...interface{}) {
	// 使用标准库 log 或由调用方注入
	fmt.Printf(format+"\n", args...)
}

// SagaCoordinatorOption 是 SagaCoordinator 的配置函数。
type SagaCoordinatorOption func(*SagaCoordinator)

// WithSagaLogger 设置日志记录器。
func WithSagaLogger(l Logger) SagaCoordinatorOption {
	return func(c *SagaCoordinator) { c.logger = l }
}

// NewSagaCoordinator 创建 SagaCoordinator。
func NewSagaCoordinator(store SagaStore, opts ...SagaCoordinatorOption) *SagaCoordinator {
	c := &SagaCoordinator{
		store:  store,
		logger: &stdLogger{},
	}
	for _, o := range opts {
		o(c)
	}
	return c
}

// Run 从头开始执行一个 Saga。
// sagaID 是此次执行的唯一标识，用于恢复。
func (c *SagaCoordinator) Run(ctx context.Context, saga *Saga, sagaID string) (*SagaState, error) {
	state := &SagaState{
		SagaID:       sagaID,
		SagaName:     saga.Name,
		Status:       SagaPending,
		CurrentStep:  0,
		StepStatuses: make([]StepStatus, len(saga.Steps)),
		StepErrors:   make([]string, len(saga.Steps)),
		ActionCtx:    make(map[string]interface{}),
		CreatedAt:    time.Now(),
		UpdatedAt:    time.Now(),
	}
	for i := range state.StepStatuses {
		state.StepStatuses[i] = StepPending
	}

	if err := c.store.Save(ctx, state); err != nil {
		return nil, fmt.Errorf("saga: save initial state: %w", err)
	}

	return c.execute(ctx, saga, state)
}

// Resume 从持久化的状态恢复执行 Saga。
// 用于进程崩溃后继续未完成的 Saga。
func (c *SagaCoordinator) Resume(ctx context.Context, saga *Saga, sagaID string) (*SagaState, error) {
	state, err := c.store.Load(ctx, sagaID)
	if err != nil {
		return nil, fmt.Errorf("saga: load state: %w", err)
	}
	if state == nil {
		return nil, fmt.Errorf("saga: saga %s not found", sagaID)
	}

	c.logf("resuming saga %s (%s) from step %d (%s)", sagaID, saga.Name, state.CurrentStep, state.Status)
	return c.execute(ctx, saga, state)
}

// execute 执行 Saga 的主循环。
func (c *SagaCoordinator) execute(ctx context.Context, saga *Saga, state *SagaState) (*SagaState, error) {
	switch state.Status {
	case SagaPending, SagaRunning:
		return c.runForward(ctx, saga, state)
	case SagaCompensating:
		return c.runCompensation(ctx, saga, state)
	case SagaCompleted, SagaFailed:
		return state, nil
	default:
		return nil, fmt.Errorf("saga: unknown status: %s", state.Status)
	}
}

// runForward 按顺序执行各步骤的 Action。
func (c *SagaCoordinator) runForward(ctx context.Context, saga *Saga, state *SagaState) (*SagaState, error) {
	state.Status = SagaRunning

	for i := state.CurrentStep; i < len(saga.Steps); i++ {
		step := saga.Steps[i]

		select {
		case <-ctx.Done():
			state.UpdatedAt = time.Now()
			_ = c.store.Save(ctx, state)
			return state, ctx.Err()
		default:
		}

		maxRetries := step.MaxRetries
		if maxRetries <= 0 {
			maxRetries = saga.DefaultMaxRetries
		}
		if maxRetries <= 0 {
			maxRetries = 1
		}

		c.logf("saga %s: executing step %d/%d %q", state.SagaID, i+1, len(saga.Steps), step.Name)

		state.CurrentStep = i
		state.StepStatuses[i] = StepRunning
		state.UpdatedAt = time.Now()
		if err := c.store.Save(ctx, state); err != nil {
			return state, fmt.Errorf("saga: save state before step %q: %w", step.Name, err)
		}

		var lastErr error
		success := false
		for retry := 0; retry < maxRetries; retry++ {
			if retry > 0 {
				c.logf("saga %s: retrying step %q (%d/%d)", state.SagaID, step.Name, retry+1, maxRetries)
			}

			// 创建可取消的 context（如果步骤设置了超时）
			stepCtx := ctx
			var cancel context.CancelFunc
			if step.Timeout > 0 {
				stepCtx, cancel = context.WithTimeout(ctx, step.Timeout)
			}

			lastErr = step.Action(stepCtx, state.ActionCtx)

			if cancel != nil {
				cancel()
			}

			if lastErr == nil {
				success = true
				break
			}

			c.logf("saga %s: step %q failed: %v", state.SagaID, step.Name, lastErr)
			state.StepErrors[i] = lastErr.Error()
		}

		if !success {
			// 步骤失败——开始补偿
			c.logf("saga %s: step %q failed after %d retries, starting compensation", state.SagaID, step.Name, maxRetries)
			state.StepStatuses[i] = StepFailed
			state.Status = SagaCompensating
			// 补偿从当前步骤的前一步开始（当前步骤未完成，无需补偿）
			state.CurrentStep = i - 1
			state.UpdatedAt = time.Now()
			if err := c.store.Save(ctx, state); err != nil {
				return state, fmt.Errorf("saga: save state before compensation: %w", err)
			}
			return c.runCompensation(ctx, saga, state)
		}

		state.StepStatuses[i] = StepCompleted
		c.logf("saga %s: step %q completed", state.SagaID, step.Name)
	}

	// 所有步骤完成
	state.Status = SagaCompleted
	state.UpdatedAt = time.Now()
	if err := c.store.Save(ctx, state); err != nil {
		return state, fmt.Errorf("saga: save completed state: %w", err)
	}
	c.logf("saga %s: completed successfully", state.SagaID)
	return state, nil
}

// runCompensation 按逆序执行已完成步骤的 Compensation。
func (c *SagaCoordinator) runCompensation(ctx context.Context, saga *Saga, state *SagaState) (*SagaState, error) {
	hadCompensationFailure := false

	for i := state.CurrentStep; i >= 0; i-- {
		// 只补偿已完成的步骤
		if state.StepStatuses[i] != StepCompleted {
			continue
		}

		step := saga.Steps[i]
		if step.Compensation == nil {
			// 无补偿操作的步骤——标记为已补偿（幂等步骤）
			state.StepStatuses[i] = StepCompensated
			continue
		}

		c.logf("saga %s: compensating step %d/%d %q", state.SagaID, i+1, len(saga.Steps), step.Name)

		if err := step.Compensation(ctx, state.ActionCtx); err != nil {
			c.logf("saga %s: compensation for step %q FAILED: %v", state.SagaID, step.Name, err)
			hadCompensationFailure = true
			state.StepErrors[i] = fmt.Sprintf("compensation failed: %v", err)
			// 补偿失败不阻止其他步骤的补偿——记录并继续
		}

		state.StepStatuses[i] = StepCompensated
		state.UpdatedAt = time.Now()
		_ = c.store.Save(ctx, state)
	}

	if hadCompensationFailure {
		state.Status = SagaFailed
		c.logf("saga %s: compensation completed WITH ERRORS — requires manual intervention", state.SagaID)
	} else {
		state.Status = SagaFailed
		c.logf("saga %s: compensation completed successfully", state.SagaID)
	}

	state.UpdatedAt = time.Now()
	if err := c.store.Save(ctx, state); err != nil {
		return state, fmt.Errorf("saga: save final state: %w", err)
	}
	return state, nil
}

// RunSaga 是便捷函数：用内存存储执行一个 Saga 并返回结果。
// 适用于简单的单进程场景。生产环境应使用持久化 SagaStore。
func RunSaga(ctx context.Context, saga *Saga, sagaID string) (*SagaState, error) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	return coordinator.Run(ctx, saga, sagaID)
}

// MarshalActionCtx 序列化 actionCtx 为 JSON（用于持久化到 Payload 中）。
func MarshalActionCtx(ctx map[string]interface{}) (json.RawMessage, error) {
	return json.Marshal(ctx)
}

// UnmarshalActionCtx 从 JSON 反序列化 actionCtx。
func UnmarshalActionCtx(data json.RawMessage) (map[string]interface{}, error) {
	if len(data) == 0 {
		return make(map[string]interface{}), nil
	}
	var ctx map[string]interface{}
	if err := json.Unmarshal(data, &ctx); err != nil {
		return nil, err
	}
	return ctx, nil
}

func (c *SagaCoordinator) logf(format string, args ...interface{}) {
	if c.logger != nil {
		c.logger.Printf(format, args...)
	}
}
