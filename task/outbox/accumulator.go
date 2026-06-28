package outbox

import (
	"encoding/json"
	"fmt"
)

// Accumulator 在内存中收集 outbox 消息，并提供 Payload 序列化/反序列化。
//
// 两种使用场景：
//
//  1. 嵌入自定义引擎（增量消息生产）：
//     type myEngine struct {
//         outbox.Accumulator
//         // ... 其他字段
//     }
//     func (e *myEngine) Execute(...) {
//         e.Add(&Message{...})                        // 逐步追加
//         state.Payload = e.MarshalPayload()          // 写入 Payload（框架自动 checkpoint）
//     }
//
//  2. 直接使用（预定义消息）：
//     acc := &Accumulator{}
//     acc.Add(&Message{...})
//     payload := acc.MarshalPayload()
//
// Accumulator 非并发安全——设计为在单 goroutine 中使用（与 task.Run 一致）。
type Accumulator struct {
	messages []*Message
}

// Add 追加一条消息。ID 重复时返回 ErrDuplicateID。
func (a *Accumulator) Add(msg *Message) error {
	for _, existing := range a.messages {
		if existing.ID == msg.ID {
			return fmt.Errorf("%w: %s", ErrDuplicateID, msg.ID)
		}
	}
	a.messages = append(a.messages, msg)
	return nil
}

// Messages 返回所有已收集的消息（按追加顺序）。
func (a *Accumulator) Messages() []*Message {
	return a.messages
}

// Len 返回已收集的消息数量。
func (a *Accumulator) Len() int {
	return len(a.messages)
}

// Reset 清空所有已收集的消息。
func (a *Accumulator) Reset() {
	a.messages = nil
}

// MarshalPayload 将消息序列化为 Payload 格式。
// 输出格式: {"outbox": [...]}
// 若无消息则返回 nil。
func (a *Accumulator) MarshalPayload() json.RawMessage {
	if len(a.messages) == 0 {
		return nil
	}
	outboxJSON, _ := json.Marshal(a.messages)
	return json.RawMessage(fmt.Sprintf(`{"outbox":%s}`, string(outboxJSON)))
}

// UnmarshalPayload 从 Payload 恢复消息（崩溃恢复用）。
// Payload 格式: {"outbox": [...]}
// 空 Payload 或格式不匹配时返回 nil（不报错）。
func (a *Accumulator) UnmarshalPayload(payload json.RawMessage) error {
	if len(payload) == 0 {
		return nil
	}

	var wrapper struct {
		Outbox []*Message `json:"outbox"`
	}
	if err := json.Unmarshal(payload, &wrapper); err != nil {
		return nil // 非 outbox Payload，不报错
	}

	a.messages = wrapper.Outbox
	return nil
}
