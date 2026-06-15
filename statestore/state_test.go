package statestore

import (
	"encoding/json"
	"testing"
)

func TestBaseTaskState_JSONRoundTrip(t *testing.T) {
	payload := json.RawMessage(`{"current_page":3}`)
	original := BaseTaskState{
		TaskID:        "task-001",
		TaskType:      "export",
		Phase:         PhaseRunning,
		Message:       "processing page 3",
		Progress:      45,
		CheckpointLSN: 4096,
		Payload:       payload,
	}

	data, err := json.Marshal(original)
	if err != nil {
		t.Fatalf("marshal: %v", err)
	}

	var restored BaseTaskState
	if err := json.Unmarshal(data, &restored); err != nil {
		t.Fatalf("unmarshal: %v", err)
	}

	if restored.TaskID != original.TaskID {
		t.Errorf("TaskID: got %q, want %q", restored.TaskID, original.TaskID)
	}
	if restored.TaskType != original.TaskType {
		t.Errorf("TaskType: got %q, want %q", restored.TaskType, original.TaskType)
	}
	if restored.Phase != original.Phase {
		t.Errorf("Phase: got %q, want %q", restored.Phase, original.Phase)
	}
	if restored.CheckpointLSN != original.CheckpointLSN {
		t.Errorf("CheckpointLSN: got %d, want %d", restored.CheckpointLSN, original.CheckpointLSN)
	}
	if restored.Progress != original.Progress {
		t.Errorf("Progress: got %d, want %d", restored.Progress, original.Progress)
	}
}

func TestTaskPhaseConstants(t *testing.T) {
	phases := []TaskPhase{
		PhasePending, PhaseRunning, PhaseMerging,
		PhaseVerifying, PhaseCompleted, PhaseFailed, PhaseCanceled,
	}
	expected := []string{
		"pending", "running", "merging",
		"verifying", "completed", "failed", "canceled",
	}
	for i, p := range phases {
		if string(p) != expected[i] {
			t.Errorf("Phase[%d] = %q, want %q", i, p, expected[i])
		}
	}
}
