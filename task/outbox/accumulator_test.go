package outbox

import (
	"encoding/json"
	"errors"
	"testing"
)

func TestAccumulator_Add(t *testing.T) {
	var acc Accumulator

	if err := acc.Add(&Message{ID: "a", EventType: "t1"}); err != nil {
		t.Fatalf("Add: %v", err)
	}
	if err := acc.Add(&Message{ID: "b", EventType: "t2"}); err != nil {
		t.Fatalf("Add: %v", err)
	}

	if acc.Len() != 2 {
		t.Errorf("Len = %d, want 2", acc.Len())
	}
	msgs := acc.Messages()
	if msgs[0].ID != "a" || msgs[1].ID != "b" {
		t.Errorf("Messages order: %v, want [a b]", msgs)
	}
}

func TestAccumulator_Add_Duplicate(t *testing.T) {
	var acc Accumulator

	acc.Add(&Message{ID: "dup", EventType: "t"})
	err := acc.Add(&Message{ID: "dup", EventType: "t2"})

	if !errors.Is(err, ErrDuplicateID) {
		t.Errorf("duplicate error = %v, want ErrDuplicateID", err)
	}
	if acc.Len() != 1 {
		t.Errorf("Len after duplicate = %d, want 1", acc.Len())
	}
}

func TestAccumulator_MarshalPayload(t *testing.T) {
	var acc Accumulator

	// 空 accumulator
	if p := acc.MarshalPayload(); p != nil {
		t.Errorf("MarshalPayload empty = %s, want nil", string(p))
	}

	acc.Add(&Message{ID: "m1", EventType: "send_email", Payload: json.RawMessage(`{"to":"a"}`)})
	acc.Add(&Message{ID: "m2", EventType: "log"})

	payload := acc.MarshalPayload()
	if payload == nil {
		t.Fatal("MarshalPayload should not be nil")
	}

	// 验证格式: {"outbox": [...]}
	var wrapper struct {
		Outbox []*Message `json:"outbox"`
	}
	if err := json.Unmarshal(payload, &wrapper); err != nil {
		t.Fatalf("unmarshal payload: %v", err)
	}
	if len(wrapper.Outbox) != 2 {
		t.Errorf("outbox count = %d, want 2", len(wrapper.Outbox))
	}
	if wrapper.Outbox[0].ID != "m1" {
		t.Errorf("msg[0].ID = %s, want m1", wrapper.Outbox[0].ID)
	}
}

func TestAccumulator_UnmarshalPayload(t *testing.T) {
	var acc Accumulator

	// 先 Marshal → Unmarshal 到另一个 accumulator
	acc.Add(&Message{ID: "r1", EventType: "restore"})
	acc.Add(&Message{ID: "r2", EventType: "restore"})
	payload := acc.MarshalPayload()

	var restored Accumulator
	if err := restored.UnmarshalPayload(payload); err != nil {
		t.Fatalf("UnmarshalPayload: %v", err)
	}
	if restored.Len() != 2 {
		t.Errorf("restored Len = %d, want 2", restored.Len())
	}
	if restored.Messages()[0].ID != "r1" {
		t.Errorf("restored[0] = %s, want r1", restored.Messages()[0].ID)
	}
}

func TestAccumulator_UnmarshalPayload_Empty(t *testing.T) {
	var acc Accumulator

	if err := acc.UnmarshalPayload(nil); err != nil {
		t.Errorf("UnmarshalPayload(nil): %v", err)
	}
	if err := acc.UnmarshalPayload(json.RawMessage{}); err != nil {
		t.Errorf("UnmarshalPayload(empty): %v", err)
	}
	if acc.Len() != 0 {
		t.Errorf("Len after empty payload = %d, want 0", acc.Len())
	}
}

func TestAccumulator_UnmarshalPayload_NonOutbox(t *testing.T) {
	var acc Accumulator

	// 非 outbox 格式的 Payload 应该不报错
	err := acc.UnmarshalPayload(json.RawMessage(`{"custom":"data"}`))
	if err != nil {
		t.Errorf("UnmarshalPayload non-outbox: %v", err)
	}
	if acc.Len() != 0 {
		t.Errorf("Len after non-outbox payload = %d, want 0", acc.Len())
	}
}

func TestAccumulator_Reset(t *testing.T) {
	var acc Accumulator

	acc.Add(&Message{ID: "x", EventType: "t"})
	acc.Reset()

	if acc.Len() != 0 {
		t.Errorf("Len after Reset = %d, want 0", acc.Len())
	}
	if acc.MarshalPayload() != nil {
		t.Error("MarshalPayload after Reset should be nil")
	}
}
