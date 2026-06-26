package engine

import (
	"context"
	"encoding/json"
	"fmt"

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
	Execute(ctx context.Context, state *statestore.BaseTaskState) (newLSN int64, err error)

	// Compensate 将物理系统对齐到 targetLSN。
	// 仅在恢复路径调用（Load 发现 Phase=running/merging 时）。
	Compensate(ctx context.Context, targetLSN int64) error

	// Progress 计算 0-100 的进度百分比。
	Progress(state statestore.BaseTaskState) int
}

// Run 执行任务的主循环，自动处理初始化与恢复。
//
// 流程：
//  1. Load — 加载状态（nil → 自动初始化 Phase=pending）
//  2. Compensate — 恢复时对齐物理系统
//  3. Execute → Save 循环 — 每一步一个 checkpoint
func Run(ctx context.Context, repo statestore.StateRepository, eng Engine, taskID string) error {
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
		state.Phase == statestore.PhaseMerging {

		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		newLSN, err := eng.Execute(ctx, state)
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
