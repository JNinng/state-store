package task

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"state-store/statestore"
)

// Engine 定义异步任务引擎的生命周期钩子。
// 实现者只需关注业务逻辑，由框架负责 Checkpoint 协议和恢复编排。
type Engine interface {
	// TaskType 返回此引擎处理的任务类型（"export" / "import" 等）。
	TaskType() string

	// Execute 执行一步业务逻辑并返回新的安全偏移量（LSN）。
	// state 为指针，引擎可修改 Phase / Message / Payload。
	// Phase=pending 时引擎应初始化 Payload 并转为 running。
	//
	// 时序契约：Execute 先执行物理副作用（写文件、写数据库等），再返回新 LSN。
	// 框架在 Execute 返回后保存 checkpoint。因此崩溃恢复时，物理系统可能领先于
	// checkpoint——副作用已发生但 LSN 未记录。Compensate 只需将物理系统回退/截断
	// 到 LSN，无需前滚。
	//
	// 副作用约束：Execute 的物理副作用必须可补偿——可截断（文件）或可幂等重放
	// （数据库 UPSERT）。不可逆操作（发邮件、扣款、发送消息队列等）不适合直接
	// 使用此框架，应在调度层使用 outbox / saga 模式处理。
	Execute(ctx context.Context, state *statestore.BaseTaskState) (newLSN int64, err error)

	// Compensate 将物理系统对齐到 targetLSN。
	// 仅在恢复路径调用（Load 发现 Phase=running/merging 时）。
	Compensate(ctx context.Context, targetLSN int64) error

	// Progress 计算 0-100 的进度百分比。
	Progress(state statestore.BaseTaskState) int
}

// RunOption 是 Run 的可选配置。
type RunOption func(*runOptions)

type runOptions struct {
	maxRetries int
	retryDelay time.Duration
}

// WithRetry 启用 Execute 失败时的自动重试。
// Execute 将以 retryDelay 间隔重试最多 maxRetries 次。
// 重试仅适用于 Execute 步骤；Load / Save / Compensate 错误不重试。
func WithRetry(maxRetries int, delay time.Duration) RunOption {
	return func(o *runOptions) {
		o.maxRetries = maxRetries
		o.retryDelay = delay
	}
}

// Run 执行任务的主循环，自动处理初始化与恢复。
//
// 流程：
//  1. Load — 加载状态（nil → 自动初始化 Phase=pending）
//  2. Compensate — 恢复时对齐物理系统
//  3. Execute → Save 循环 — 每一步一个 checkpoint
//
// 可选配置通过 RunOption 传入（如 WithRetry）。
func Run(ctx context.Context, repo statestore.StateRepository, eng Engine, taskID string, opts ...RunOption) error {
	var cfg runOptions
	for _, o := range opts {
		o(&cfg)
	}

	// 1. 加载或初始化
	data, err := repo.Load(ctx, taskID)
	if err != nil {
		return fmt.Errorf("engine: load state: %w", err)
	}

	var state *statestore.BaseTaskState
	if data == nil {
		state = &statestore.BaseTaskState{
			TaskID:   taskID,
			TaskType: eng.TaskType(),
			Phase:    statestore.PhasePending,
		}
	} else {
		state = &statestore.BaseTaskState{}
		if err := json.Unmarshal(data, state); err != nil {
			return fmt.Errorf("engine: unmarshal state: %w", err)
		}
	}

	// 2. 恢复补偿
	if state.Phase == statestore.PhaseRunning || state.Phase == statestore.PhaseMerging {
		if err := eng.Compensate(ctx, state.CheckpointLSN); err != nil {
			return fmt.Errorf("engine: compensate: %w", err)
		}
	}

	// 3. 主循环
	for state.Phase == statestore.PhasePending ||
		state.Phase == statestore.PhaseRunning ||
		state.Phase == statestore.PhaseMerging ||
		state.Phase == statestore.PhaseVerifying {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		newLSN, err := eng.Execute(ctx, state)
		if err != nil && cfg.maxRetries > 0 {
			for retry := 0; retry < cfg.maxRetries; retry++ {
				select {
				case <-ctx.Done():
					return ctx.Err()
				case <-time.After(cfg.retryDelay):
				}
				newLSN, err = eng.Execute(ctx, state)
				if err == nil {
					break
				}
			}
		}
		if err != nil {
			return err
		}

		state.CheckpointLSN = newLSN
		state.Progress = eng.Progress(*state)

		marshaled, err := json.Marshal(state)
		if err != nil {
			return fmt.Errorf("engine: marshal state: %w", err)
		}
		if err := repo.Save(ctx, taskID, marshaled); err != nil {
			return fmt.Errorf("engine: save checkpoint: %w", err)
		}

		if state.Phase == statestore.PhaseCompleted || state.Phase == statestore.PhaseFailed {
			break
		}
	}

	return nil
}
