package outbox

import (
	"context"
	"encoding/json"
	"fmt"

	"state-store/statestore"
	"state-store/task"
)

// Engine 是一个通用的 outbox 引擎，自动完成 phase 转换和 Payload 序列化。
//
// 适用于"任务目的就是记录 outbox 意图"的场景——引擎自身只管理 outbox
// 消息的生命周期，真正的不可逆操作（发邮件、扣款、发送消息队列）由
// Dispatcher 在调度层执行。
//
// 使用方式：
//
//	eng := outbox.NewEngine("my_task", messages)
//	task.Run(ctx, repo, eng, taskID)
//
//	// 调度层提取并分发
//	for _, m := range eng.Messages() {
//	    m.Status = outbox.StatusPending
//	    outboxStore.Append(ctx, m)
//	}
//	dispatcher.DispatchPending(ctx)
type Engine struct {
	taskType string
	messages []*Message
}

// NewEngine 创建通用 outbox 引擎。
//
// taskType 是引擎的任务类型标识（对应 TaskType() 返回值）。
// messages 是引擎生命周期内要纳入 checkpoint 管理的 outbox 消息。
func NewEngine(taskType string, messages []*Message) *Engine {
	return &Engine{taskType: taskType, messages: messages}
}

// TaskType 返回任务类型标识。
func (e *Engine) TaskType() string { return e.taskType }

// Execute 执行一步业务逻辑并返回新的 LSN。
//
// Phase 转换：
//   - pending → running: 将 outbox 消息序列化写入 Payload
//   - running → completed: 标记任务完成
func (e *Engine) Execute(_ context.Context, state *statestore.BaseTaskState) (int64, error) {
	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		outboxJSON, _ := json.Marshal(e.messages)
		state.Payload = json.RawMessage(fmt.Sprintf(`{"outbox":%s}`, string(outboxJSON)))
		return 0, nil
	case statestore.PhaseRunning:
		state.Phase = statestore.PhaseCompleted
		state.Message = "done"
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

// Compensate 回退物理副作用。
// outbox 引擎自身不产生物理副作用，始终返回 nil。
func (e *Engine) Compensate(_ context.Context, _ int64) error {
	return nil
}

// Progress 返回任务完成百分比。
func (e *Engine) Progress(state statestore.BaseTaskState) int {
	if state.Phase == statestore.PhaseCompleted {
		return 100
	}
	return 50
}

// Messages 返回引擎中的 outbox 消息，供调度层在 engine.Run 后提取并写入
// OutboxStore 进行分发。
func (e *Engine) Messages() []*Message {
	return e.messages
}

// 编译期接口检查
var _ task.Engine = (*Engine)(nil)
