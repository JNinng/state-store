package statestore

import "encoding/json"

// TaskPhase 表示异步任务的生命周期阶段。
type TaskPhase string

const (
	PhasePending   TaskPhase = "pending"
	PhaseRunning   TaskPhase = "running"
	PhaseMerging   TaskPhase = "merging"
	PhaseVerifying TaskPhase = "verifying"
	PhaseCompleted TaskPhase = "completed"
	PhaseFailed    TaskPhase = "failed"
	PhaseCanceled  TaskPhase = "canceled"
)

// BaseTaskState 是所有异步任务必须包含的公共状态字段。
// 由框架负责序列化/反序列化，引擎通过 Payload 扩展业务特有状态。
type BaseTaskState struct {
	TaskID        string          `json:"task_id"`
	TaskType      string          `json:"task_type"`
	Phase         TaskPhase       `json:"phase"`
	Message       string          `json:"message,omitempty"`
	Progress      int             `json:"progress"`
	CheckpointLSN int64           `json:"checkpoint_lsn"`
	Payload       json.RawMessage `json:"payload"`
}
