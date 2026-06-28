package outbox

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"state-store/statestore"
	"state-store/task"
)

// RunWithOutbox 运行引擎并将 Payload 中的 outbox 消息同步到 Store。
//
// 这是生产级的"运行 + outbox 同步"桥接函数：
//  1. 调用 task.Run() 执行引擎（checkpoint 保护的可逆工作）
//  2. 从持久化的 StateRepository 加载 checkpointed 状态
//  3. 从 Payload 中提取 outbox 消息
//  4. 将消息写入 OutboxStore
//
// 崩溃安全：如果进程在 task.Run() 成功后崩溃，重启后重新调用
// RunWithOutbox 可以安全恢复——task.Run 对已完成任务幂等，
// outboxStore.Append 对重复 msg.ID 返回 ErrDuplicateID（被跳过）。
//
// Payload 约定：引擎必须在 Payload 中以 {"outbox": [...]} 格式
// 嵌入 outbox 消息。使用 outbox.NewEngine 创建的引擎自动遵循此约定。
//
// 调用方在函数返回后使用 Dispatcher 分发消息：
//
//	state, err := outbox.RunWithOutbox(ctx, repo, eng, taskID, outboxStore)
//	if err != nil { ... }
//	// 同步或异步分发
//	dispatcher.DispatchPending(ctx)
//	// 或: go dispatcher.Start(ctx)
func RunWithOutbox(ctx context.Context,
	repo statestore.StateRepository,
	eng task.Engine,
	taskID string,
	outboxStore Store,
	opts ...task.RunOption) (*statestore.BaseTaskState, error) {

	// 步骤 1: 运行引擎（已完成的 task 幂等跳过）
	if err := task.Run(ctx, repo, eng, taskID, opts...); err != nil {
		return nil, fmt.Errorf("outbox: task.Run: %w", err)
	}

	// 步骤 2: 从持久化 repo 加载 checkpointed 状态（崩溃安全）
	data, err := repo.Load(ctx, taskID)
	if err != nil {
		return nil, fmt.Errorf("outbox: load state after run: %w", err)
	}
	if data == nil {
		return nil, fmt.Errorf("outbox: task %s state not found after Run", taskID)
	}

	// 步骤 3: 反序列化
	var state statestore.BaseTaskState
	if err := json.Unmarshal(data, &state); err != nil {
		return nil, fmt.Errorf("outbox: unmarshal state: %w", err)
	}

	// 步骤 4: 从 Payload 提取 outbox 消息
	var acc Accumulator
	if err := acc.UnmarshalPayload(state.Payload); err != nil || acc.Len() == 0 {
		// 无 outbox 消息——引擎可能不使用 outbox 模式，正常返回
		return &state, nil
	}
	msgs := acc.Messages()

	// 步骤 5: 写入 OutboxStore
	for _, msg := range msgs {
		if msg.Status == "" {
			msg.Status = StatusPending
		}
		if msg.CreatedAt.IsZero() {
			msg.CreatedAt = time.Now()
		}
		if msg.TaskID == "" {
			msg.TaskID = taskID
		}

		if err := outboxStore.Append(ctx, msg); err != nil {
			// 重复 ID → 崩溃重试安全，跳过
			if errors.Is(err, ErrDuplicateID) {
				continue
			}
			return &state, fmt.Errorf("outbox: append message %s: %w", msg.ID, err)
		}
	}

	return &state, nil
}
