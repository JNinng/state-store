package statestore

import "context"

// StateRepository is the atomic persistence abstraction for task state.
//
// Implementations must satisfy:
//   - Save is an atomic full replacement, no partial merging allowed
//   - Save returns nil to indicate the state has been fully persisted
//   - Save returns an error to indicate the state is unchanged from before the call
//   - Load returns (nil, nil) for a non-existent task, not an error
//   - Delete is idempotent and returns nil for a non-existent task
type StateRepository interface {
	// Load retrieves the serialized byte stream of a task's state.
	// Must return nil, nil (not an error) when the task does not exist.
	Load(ctx context.Context, taskID string) ([]byte, error)

	// Save atomically replaces the full state of a task.
	Save(ctx context.Context, taskID string, state []byte) error

	// Delete removes task state and releases storage.
	Delete(ctx context.Context, taskID string) error
}
