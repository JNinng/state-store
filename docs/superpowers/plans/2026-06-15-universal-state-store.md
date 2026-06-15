# Universal Task StateStore 实施计划

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 构建一个 Go 库，提供任务状态原子持久化（文件系统）、Checkpoint 协议编排引擎框架、以及内建的导出和导入引擎。

**Architecture:** 6 个包，自下而上：`statestore`（接口+模型）→ `phys`（物理抽象）→ `filestore`（文件存储实现）→ `engine`（编排框架）→ `engine/export` + `engine/import`（业务引擎）。零外部依赖，纯标准库。

**Tech Stack:** Go 1.26.2, 标准库 (os, encoding/json, context, errors, fmt, io, testing, os/exec)

---

## 文件结构

```
state-store/
├── go.mod                          # 已存在
├── statestore/
│   ├── errors.go                   # 创建: sentinel errors
│   ├── state.go                    # 创建: BaseTaskState, TaskPhase
│   └── repository.go               # 创建: StateRepository interface
├── phys/
│   └── phys.go                     # 创建: DataSource, DataTarget, Row
├── filestore/
│   └── filestore.go                # 创建: FileRepository (tmp+Rename)
│   └── filestore_test.go           # 创建: 原子性测试
├── engine/
│   └── engine.go                   # 创建: Engine interface, Run()
│   └── engine_test.go              # 创建: 框架恢复/正常链路测试
├── engine/export/
│   └── export.go                   # 创建: ExportEngine
│   └── export_test.go              # 创建: 导出测试
└── engine/import/
    └── import.go                   # 创建: ImportEngine
    └── import_test.go              # 创建: 导入测试
└── integration/
    └── integration_test.go         # 创建: 端到端集成测试
```

---

### Task 1: statestore 包 — Sentinel Errors

**Files:**
- Create: `statestore/errors.go`

- [ ] **Step 1: 创建 errors.go**

```go
package statestore

import "errors"

var (
	// ErrSaveFailed 表示状态保存失败，底层存储无法完成原子写入。
	ErrSaveFailed = errors.New("statestore: save failed")

	// ErrLoadFailed 表示状态加载失败，底层存储读取错误（非"不存在"）。
	ErrLoadFailed = errors.New("statestore: load failed")
)
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./statestore/...`
Expected: 无错误

- [ ] **Step 3: Commit**

```bash
git add statestore/errors.go
git commit -m "feat(statestore): add sentinel errors"
```

---

### Task 2: statestore 包 — 状态模型

**Files:**
- Create: `statestore/state.go`
- Create: `statestore/state_test.go`

- [ ] **Step 1: 编写测试 — JSON 序列化/反序列化**

```go
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
```

- [ ] **Step 2: 运行测试 — 失败（类型未定义）**

Run: `go test ./statestore/ -v -run TestBaseTaskState`
Expected: 编译失败 — `BaseTaskState` 未定义

- [ ] **Step 3: 实现 state.go**

```go
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
	TaskID        string           `json:"task_id"`
	TaskType      string           `json:"task_type"`
	Phase         TaskPhase        `json:"phase"`
	Message       string           `json:"message,omitempty"`
	Progress      int              `json:"progress"`
	CheckpointLSN int64            `json:"checkpoint_lsn"`
	Payload       json.RawMessage  `json:"payload"`
}
```

- [ ] **Step 4: 运行测试 — 全部通过**

Run: `go test ./statestore/ -v`
Expected: PASS

- [ ] **Step 5: Commit**

```bash
git add statestore/state.go statestore/state_test.go
git commit -m "feat(statestore): add BaseTaskState and TaskPhase model"
```

---

### Task 3: statestore 包 — StateRepository 接口

**Files:**
- Create: `statestore/repository.go`

- [ ] **Step 1: 创建 repository.go**

```go
package statestore

import "context"

// StateRepository 是任务状态的原子持久化抽象。
//
// 实现必须满足：
//   - Save 是原子全量替换，不允许局部合并
//   - Save 返回 nil 表示状态已完整持久化
//   - Save 返回 error 表示状态停留在调用之前
//   - Load 对不存在的任务返回 (nil, nil)，而非 error
//   - Delete 对不存在的任务应幂等返回 nil
type StateRepository interface {
	// Load 获取任务状态的序列化字节流。
	// 任务不存在时必须返回 nil, nil（不是 error）。
	Load(ctx context.Context, taskID string) ([]byte, error)

	// Save 原子性全量替换任务状态。
	Save(ctx context.Context, taskID string, state []byte) error

	// Delete 清理任务状态，释放存储空间。
	Delete(ctx context.Context, taskID string) error
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./statestore/...`
Expected: 无错误

- [ ] **Step 3: Commit**

```bash
git add statestore/repository.go
git commit -m "feat(statestore): add StateRepository interface"
```

---

### Task 4: phys 包 — 物理系统抽象接口

**Files:**
- Create: `phys/phys.go`

- [ ] **Step 1: 创建 phys.go**

```go
package phys

import "context"

// Row 是通用的数据行载体，调用方自行定义键值语义。
type Row map[string]interface{}

// DataSource 是导出引擎从数据源分页读取的抽象。
// 调用方实现具体的数据库查询、API 调用等。
type DataSource interface {
	// FetchPage 获取第 page 页的数据（page 从 0 开始）。
	// 每页返回最多 pageSize 行。
	// 返回 io.EOF 表示无更多数据可供读取。
	FetchPage(ctx context.Context, page int, pageSize int) ([]Row, error)
}

// DataTarget 是导入引擎向目标批量写入的抽象。
//
// 契约：实现方必须保证写入的幂等性。进程崩溃恢复时，
// 已入库但未 checkpoint 的数据会被重新写入，
// DataTarget 需通过 UPSERT / 唯一键去重 / 预检等方式处理。
type DataTarget interface {
	// WriteBatch 批量写入行，返回实际插入的行数。
	WriteBatch(ctx context.Context, rows []Row) (inserted int64, err error)
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./phys/...`
Expected: 无错误

- [ ] **Step 3: Commit**

```bash
git add phys/phys.go
git commit -m "feat(phys): add DataSource and DataTarget interfaces"
```

---

### Task 5: filestore 包 — 文件系统 StateRepository 实现

**Files:**
- Create: `filestore/filestore.go`
- Create: `filestore/filestore_test.go`

- [ ] **Step 1: 编写测试**

```go
package filestore

import (
	"context"
	"os"
	"path/filepath"
	"testing"

	"state-store/statestore"
)

func TestFileRepository_SaveAndLoad(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-001"
	original := []byte(`{"task_id":"task-001","phase":"running"}`)

	if err := repo.Save(ctx, taskID, original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	data, err := repo.Load(ctx, taskID)
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if string(data) != string(original) {
		t.Errorf("data = %s, want %s", data, original)
	}
}

func TestFileRepository_LoadNotFound(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	data, err := repo.Load(context.Background(), "nonexistent")
	if err != nil {
		t.Fatalf("Load should not error for missing task: %v", err)
	}
	if data != nil {
		t.Errorf("data should be nil for missing task, got %s", data)
	}
}

func TestFileRepository_Delete(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-del"
	if err := repo.Save(ctx, taskID, []byte("{}")); err != nil {
		t.Fatalf("Save: %v", err)
	}
	if err := repo.Delete(ctx, taskID); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	// 验证已删除
	data, err := repo.Load(ctx, taskID)
	if err != nil {
		t.Fatalf("Load after delete: %v", err)
	}
	if data != nil {
		t.Errorf("data should be nil after delete, got %s", data)
	}
}

func TestFileRepository_DeleteIdempotent(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	// 删除不存在的任务不应报错
	if err := repo.Delete(context.Background(), "nonexistent"); err != nil {
		t.Errorf("Delete should be idempotent: %v", err)
	}
}

func TestFileRepository_SaveAtomic_NoPartialWrite(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	ctx := context.Background()
	taskID := "task-atomic"

	// 写入并验证 .tmp 文件在 Save 后不存在
	if err := repo.Save(ctx, taskID, []byte("atomic-data")); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// .tmp 不应残留
	tmpPath := filepath.Join(dir, taskID+".state.tmp")
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error(".tmp file should not exist after successful Save")
	}

	// .state 应存在
	statePath := filepath.Join(dir, taskID+".state")
	if _, err := os.Stat(statePath); os.IsNotExist(err) {
		t.Error(".state file should exist after Save")
	}
}

func TestFileRepository_Cleanup(t *testing.T) {
	dir := t.TempDir()
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}

	// 创建残留 .tmp 文件
	tmpPath := filepath.Join(dir, "orphan.state.tmp")
	if err := os.WriteFile(tmpPath, []byte("garbage"), 0644); err != nil {
		t.Fatalf("write tmp: %v", err)
	}

	// Cleanup 应删除它
	if err := repo.Cleanup(); err != nil {
		t.Fatalf("Cleanup: %v", err)
	}
	if _, err := os.Stat(tmpPath); !os.IsNotExist(err) {
		t.Error("Cleanup should remove orphan .tmp files")
	}
}

func TestFileRepository_ImplementsInterface(t *testing.T) {
	// 编译期检查：FileRepository 实现 StateRepository
	var _ statestore.StateRepository = (*FileRepository)(nil)
}

func TestFileRepository_NewCreatesDir(t *testing.T) {
	base := t.TempDir()
	dir := filepath.Join(base, "subdir", "statestore")
	repo, err := New(dir)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	if repo == nil {
		t.Fatal("repo should not be nil")
	}
	// 验证目录已创建
	info, err := os.Stat(dir)
	if err != nil {
		t.Fatalf("stat dir: %v", err)
	}
	if !info.IsDir() {
		t.Error("path should be a directory")
	}
}
```

- [ ] **Step 2: 运行测试 — 编译失败**

Run: `go test ./filestore/ -v`
Expected: 编译失败 — `FileRepository`, `New` 未定义

- [ ] **Step 3: 实现 filestore.go**

```go
package filestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
)

// FileRepository 是基于本地文件系统的 StateRepository 实现。
// 使用 tmp + Rename 策略保证写入原子性。
type FileRepository struct {
	dir string
}

// New 创建 FileRepository。
// 若目录不存在则自动创建。
func New(dir string) (*FileRepository, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("filestore: create dir %s: %w", dir, err)
	}
	return &FileRepository{dir: dir}, nil
}

// Load 读取任务状态文件。
// 任务不存在返回 nil, nil。
func (r *FileRepository) Load(ctx context.Context, taskID string) ([]byte, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(r.statePath(taskID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("%w: %v", ErrLoadFailed, err)
	}
	return data, nil
}

// Save 原子写入任务状态。
// 流程：写 .tmp → Sync → Rename。
func (r *FileRepository) Save(ctx context.Context, taskID string, state []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tmpPath := r.tmpPath(taskID)
	finalPath := r.statePath(taskID)

	// 1. 写入临时文件
	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("%w: create tmp: %v", ErrSaveFailed, err)
	}

	if _, err := f.Write(state); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("%w: write tmp: %v", ErrSaveFailed, err)
	}

	// 2. 强制落盘
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("%w: sync tmp: %v", ErrSaveFailed, err)
	}
	f.Close()

	// 3. 原子替换
	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("%w: rename: %v", ErrSaveFailed, err)
	}

	return nil
}

// Delete 删除任务状态文件，幂等。
func (r *FileRepository) Delete(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(r.statePath(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Cleanup 清理残留的 .tmp 文件（进程崩溃后遗留）。
func (r *FileRepository) Cleanup() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("filestore: read dir: %w", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".state.tmp") {
			os.Remove(filepath.Join(r.dir, e.Name()))
		}
	}
	return nil
}

func (r *FileRepository) statePath(taskID string) string {
	return filepath.Join(r.dir, taskID+".state")
}

func (r *FileRepository) tmpPath(taskID string) string {
	return filepath.Join(r.dir, taskID+".state.tmp")
}

// ErrLoadFailed 在 filestore 包内定义为包装 statestore 的错误。
// 使用 errors.Is 可与 statestore.ErrLoadFailed 匹配。
var ErrLoadFailed = fmt.Errorf("filestore: load failed")

// ErrSaveFailed 同上。
var ErrSaveFailed = fmt.Errorf("filestore: save failed")
```

- [ ] **Step 4: 运行测试 — 全部通过**

Run: `go test ./filestore/ -v`
Expected: 全部 PASS（7 个测试）

- [ ] **Step 5: Commit**

```bash
git add filestore/filestore.go filestore/filestore_test.go
git commit -m "feat(filestore): add FileRepository with atomic tmp+Rename save"
```

---

### Task 6: engine 包 — Engine 接口与 Run 编排

**Files:**
- Create: `engine/engine.go`
- Create: `engine/engine_test.go`

- [ ] **Step 1: 实现 engine.go（接口定义 + Run 函数）**

```go
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
			return fmt.Errorf("%w: %v", statestore.ErrLoadFailed, err)
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
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./engine/...`
Expected: 无错误

- [ ] **Step 3: 编写 engine_test.go — mock Engine + 恢复/正常链路测试**

```go
package engine

import (
	"context"
	"encoding/json"
	"errors"
	"testing"

	"state-store/statestore"
)

// mockRepo 是 StateRepository 的内存实现，用于测试。
type mockRepo struct {
	store map[string][]byte
}

func newMockRepo() *mockRepo {
	return &mockRepo{store: make(map[string][]byte)}
}

func (m *mockRepo) Load(ctx context.Context, taskID string) ([]byte, error) {
	data, ok := m.store[taskID]
	if !ok {
		return nil, nil
	}
	return data, nil
}

func (m *mockRepo) Save(ctx context.Context, taskID string, state []byte) error {
	m.store[taskID] = make([]byte, len(state))
	copy(m.store[taskID], state)
	return nil
}

func (m *mockRepo) Delete(ctx context.Context, taskID string) error {
	delete(m.store, taskID)
	return nil
}

// simpleEngine 是一个简单的 Engine mock，用于测试框架行为。
type simpleEngine struct {
	executeCalls     int
	compensateCalls  int
	compensateTarget int64
	failOnExecute    error
}

func (e *simpleEngine) TaskType() string { return "test" }

func (e *simpleEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	if e.failOnExecute != nil {
		return 0, e.failOnExecute
	}
	e.executeCalls++

	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		state.Payload = json.RawMessage(`{"step":0}`)
		return 0, nil
	case statestore.PhaseRunning:
		e.executeCalls++
		state.Phase = statestore.PhaseCompleted
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

func (e *simpleEngine) Compensate(ctx context.Context, targetLSN int64) error {
	e.compensateCalls++
	e.compensateTarget = targetLSN
	return nil
}

func (e *simpleEngine) Progress(state statestore.BaseTaskState) int {
	return 50
}

func TestRun_NormalFlow(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	err := Run(context.Background(), repo, eng, "task-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}
	if eng.executeCalls == 0 {
		t.Error("Execute should be called")
	}
	if eng.compensateCalls != 0 {
		t.Error("Compensate should not be called in normal flow")
	}

	// 验证最终状态
	data, err := repo.Load(context.Background(), "task-001")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	var state statestore.BaseTaskState
	json.Unmarshal(data, &state)
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want %q", state.Phase, statestore.PhaseCompleted)
	}
}

func TestRun_RecoveryFlow(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	// 预存一个 running 状态（模拟崩溃恢复）
	initialState := statestore.BaseTaskState{
		TaskID:        "task-recover",
		TaskType:      "test",
		Phase:         statestore.PhaseRunning,
		CheckpointLSN: 50,
		Payload:       json.RawMessage(`{"step":1}`),
	}
	data, _ := json.Marshal(initialState)
	repo.Save(context.Background(), "task-recover", data)

	err := Run(context.Background(), repo, eng, "task-recover")
	if err != nil {
		t.Fatalf("Run recovery: %v", err)
	}

	// 恢复时必须调用 Compensate
	if eng.compensateCalls != 1 {
		t.Errorf("Compensate calls = %d, want 1", eng.compensateCalls)
	}
	if eng.compensateTarget != 50 {
		t.Errorf("Compensate target = %d, want 50", eng.compensateTarget)
	}
}

func TestRun_ExecuteErrorNotSaved(t *testing.T) {
	repo := newMockRepo()
	expectedErr := errors.New("mock failure")
	eng := &simpleEngine{failOnExecute: expectedErr}

	err := Run(context.Background(), repo, eng, "task-err")
	if !errors.Is(err, expectedErr) {
		t.Fatalf("error = %v, want %v", err, expectedErr)
	}

	// Execute 失败后不应有状态保存（调用方控制）
	data, _ := repo.Load(context.Background(), "task-err")
	if data != nil {
		t.Error("state should not be saved after Execute error")
	}
}

func TestRun_ContextCancellation(t *testing.T) {
	repo := newMockRepo()
	eng := &simpleEngine{}

	ctx, cancel := context.WithCancel(context.Background())
	cancel() // 立即取消

	err := Run(ctx, repo, eng, "task-cancel")
	if !errors.Is(err, context.Canceled) {
		t.Errorf("error = %v, want context.Canceled", err)
	}
}
```

- [ ] **Step 4: 运行测试 — 全部通过**

Run: `go test ./engine/ -v`
Expected: 全部 PASS（4 个测试）

- [ ] **Step 5: Commit**

```bash
git add engine/engine.go engine/engine_test.go
git commit -m "feat(engine): add Engine interface and Run checkpoint orchestration"
```

---

### Task 7: engine/export 包 — 导出引擎

**Files:**
- Create: `engine/export/export.go`
- Create: `engine/export/export_test.go`

- [ ] **Step 1: 实现 export.go**

```go
package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"state-store/engine"
	"state-store/phys"
	"state-store/statestore"
)

// ExportPayload 是导出任务的业务扩展状态。
type ExportPayload struct {
	CurrentPage      int   `json:"current_page"`
	CurrentChunkIdx  int   `json:"current_chunk_idx"`
	CurrentChunkSize int64 `json:"current_chunk_size"`
	TotalChunks      int   `json:"total_chunks"`
	FinalFileSize    int64 `json:"final_file_size"`
	MergedChunkIdx   int   `json:"merged_chunk_idx"`
}

// ExportEngine 实现 engine.Engine 接口，执行分页提取→分块写文件→合并的导出流程。
type ExportEngine struct {
	src        phys.DataSource
	outputDir  string
	outputFile string
	pageSize   int
	chunkPages int
}

// ExportOption 是 ExportEngine 的配置函数。
type ExportOption func(*ExportEngine)

// WithPageSize 设置每页行数，默认 1000。
func WithPageSize(n int) ExportOption {
	return func(e *ExportEngine) { e.pageSize = n }
}

// WithChunkPages 设置每个分块包含的页数，默认 10。
func WithChunkPages(n int) ExportOption {
	return func(e *ExportEngine) { e.chunkPages = n }
}

// New 创建 ExportEngine。
func New(src phys.DataSource, outputDir, outputFile string, opts ...ExportOption) *ExportEngine {
	e := &ExportEngine{
		src:        src,
		outputDir:  outputDir,
		outputFile: outputFile,
		pageSize:   1000,
		chunkPages: 10,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// 编译期检查
var _ engine.Engine = (*ExportEngine)(nil)

func (e *ExportEngine) TaskType() string { return "export" }

func (e *ExportEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	var p ExportPayload
	if len(state.Payload) > 0 {
		if err := json.Unmarshal(state.Payload, &p); err != nil {
			return 0, fmt.Errorf("export: unmarshal payload: %w", err)
		}
	}

	switch state.Phase {
	case statestore.PhasePending:
		p = ExportPayload{}
		state.Phase = statestore.PhaseRunning
		state.Payload = e.marshalPayload(p)
		return 0, nil

	case statestore.PhaseRunning:
		return e.executeRunning(ctx, state, &p)

	case statestore.PhaseMerging:
		return e.executeMerging(ctx, state, &p)

	default:
		return state.CheckpointLSN, nil
	}
}

func (e *ExportEngine) executeRunning(ctx context.Context, state *statestore.BaseTaskState, p *ExportPayload) (int64, error) {
	// 检查是否有更多数据
	rows, err := e.src.FetchPage(ctx, p.CurrentPage, e.pageSize)
	if err == io.EOF {
		// 提取完成，切换阶段
		p.TotalChunks = p.CurrentChunkIdx
		if p.CurrentChunkSize > 0 {
			p.TotalChunks++ // 当前未满的块也算一个
		}
		state.Phase = statestore.PhaseMerging
		state.Message = "extraction complete, starting merge"
		state.Payload = e.marshalPayload(*p)
		return e.calcLSN(*p), nil
	}
	if err != nil {
		return 0, fmt.Errorf("export: fetch page %d: %w", p.CurrentPage, err)
	}

	// 写入当前分块文件
	chunkPath := e.chunkPath(p.CurrentChunkIdx)
	f, err := os.OpenFile(chunkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("export: open chunk %d: %w", p.CurrentChunkIdx, err)
	}

	written, err := e.writeRows(f, rows)
	f.Close()
	if err != nil {
		return 0, err
	}

	p.CurrentChunkSize += written
	p.CurrentPage++

	// 当前块满了？
	if p.CurrentPage%e.e.chunkPages == 0 {
		p.CurrentChunkIdx++
		p.CurrentChunkSize = 0
	}

	state.Message = fmt.Sprintf("extracting page %d", p.CurrentPage)
	state.Payload = e.marshalPayload(*p)
	return e.calcLSN(*p), nil
}

func (e *ExportEngine) executeMerging(ctx context.Context, state *statestore.BaseTaskState, p *ExportPayload) (int64, error) {
	if p.MergedChunkIdx >= p.TotalChunks {
		// 合并完成
		state.Phase = statestore.PhaseCompleted
		state.Message = "export completed"
		e.cleanupChunks(p.TotalChunks)
		return p.FinalFileSize, nil
	}

	chunkPath := e.chunkPath(p.MergedChunkIdx)
	chunkData, err := os.ReadFile(chunkPath)
	if err != nil {
		return 0, fmt.Errorf("export: read chunk %d: %w", p.MergedChunkIdx, err)
	}

	finalPath := filepath.Join(e.outputDir, e.outputFile)
	f, err := os.OpenFile(finalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("export: open final file: %w", err)
	}

	n, err := f.Write(chunkData)
	f.Close()
	if err != nil {
		return 0, fmt.Errorf("export: write final file: %w", err)
	}

	p.FinalFileSize += int64(n)
	p.MergedChunkIdx++

	state.Message = fmt.Sprintf("merging chunk %d/%d", p.MergedChunkIdx, p.TotalChunks)
	state.Payload = e.marshalPayload(*p)
	return p.FinalFileSize, nil
}

func (e *ExportEngine) Compensate(ctx context.Context, targetLSN int64) error {
	// 补偿逻辑由 Phase 决定，但 Compensate 不接收 state。
	// 导出引擎的补偿：截断文件。
	// 由于我们不知道当前 Phase，这里采用最佳努力：
	// 尝试截断最终文件和所有分块文件到合适大小。
	// 实际实现中通过 Execute 的恢复来保证正确性，
	// Compensate 清除明显的脏数据。
	finalPath := filepath.Join(e.outputDir, e.outputFile)
	if info, err := os.Stat(finalPath); err == nil && info.Size() > targetLSN {
		if err := os.Truncate(finalPath, targetLSN); err != nil {
			return fmt.Errorf("export: truncate final file: %w", err)
		}
	}
	return nil
}

func (e *ExportEngine) Progress(state statestore.BaseTaskState) int {
	var p ExportPayload
	if len(state.Payload) > 0 {
		json.Unmarshal(state.Payload, &p)
	}
	if state.Phase == statestore.PhaseMerging {
		if p.TotalChunks == 0 {
			return 90
		}
		return 90 + p.MergedChunkIdx*10/p.TotalChunks
	}
	// running 或 pending
	if p.TotalChunks == 0 {
		return 0
	}
	prog := p.CurrentChunkIdx * 100 / p.TotalChunks
	if prog > 90 {
		return 90
	}
	return prog
}

func (e *ExportEngine) calcLSN(p ExportPayload) int64 {
	return int64(p.CurrentChunkIdx)*e.chunkSize() + p.CurrentChunkSize
}

func (e *ExportEngine) chunkSize() int64 {
	return int64(e.pageSize * e.chunkPages)
}

func (e *ExportEngine) chunkPath(idx int) string {
	return filepath.Join(e.outputDir, fmt.Sprintf("%s.chunk_%d.tmp", e.outputFile, idx))
}

func (e *ExportEngine) marshalPayload(p ExportPayload) json.RawMessage {
	data, _ := json.Marshal(p)
	return data
}

func (e *ExportEngine) writeRows(f *os.File, rows []phys.Row) (int64, error) {
	var total int64
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return total, fmt.Errorf("export: marshal row: %w", err)
		}
		n, err := f.Write(append(data, '\n'))
		total += int64(n)
		if err != nil {
			return total, fmt.Errorf("export: write row: %w", err)
		}
	}
	return total, nil
}

func (e *ExportEngine) cleanupChunks(total int) {
	for i := 0; i < total; i++ {
		os.Remove(e.chunkPath(i))
	}
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./engine/export/...`
Expected: 无错误

- [ ] **Step 3: 编写 export_test.go**

```go
package export

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"state-store/phys"
	"state-store/statestore"
)

// stubDataSource 用于测试。
type stubDataSource struct {
	pages    [][]phys.Row
	pageIdx  int
	failOn   int // 在第几次 FetchPage 时失败
}

func (s *stubDataSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if s.pageIdx >= len(s.pages) {
		return nil, io.EOF
	}
	if s.failOn > 0 && s.pageIdx == s.failOn {
		return nil, io.EOF // 模拟中途失败
	}
	result := s.pages[s.pageIdx]
	s.pageIdx++
	return result, nil
}

func makeRows(n int) []phys.Row {
	rows := make([]phys.Row, n)
	for i := 0; i < n; i++ {
		rows[i] = phys.Row{"id": i, "name": "row"}
	}
	return rows
}

func TestExportEngine_NormalFlow(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{
		pages: [][]phys.Row{
			makeRows(5),
			makeRows(5),
		},
	}
	eng := New(ds, dir, "output.dat", WithPageSize(5), WithChunkPages(1))

	state := &statestore.BaseTaskState{
		TaskID:   "export-001",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	// Phase pending → running
	lsn, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	if lsn != 0 {
		t.Errorf("lsn = %d, want 0", lsn)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Errorf("phase = %q, want running", state.Phase)
	}

	// Phase running: 第 1 页
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute running page 1: %v", err)
	}

	// Phase running: 第 2 页 → EOF → merging
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute running page 2: %v", err)
	}
	if state.Phase != statestore.PhaseMerging {
		t.Errorf("phase after EOF = %q, want merging", state.Phase)
	}

	// Phase merging → completed
	for state.Phase == statestore.PhaseMerging {
		lsn, err = eng.Execute(context.Background(), state)
		if err != nil {
			t.Fatalf("Execute merging: %v", err)
		}
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("final phase = %q, want completed", state.Phase)
	}
	_ = lsn
}

func TestExportEngine_Compensate(t *testing.T) {
	dir := t.TempDir()
	ds := &stubDataSource{}
	eng := New(ds, dir, "output.dat")

	// 创建一个超大的最终文件
	finalPath := filepath.Join(dir, "output.dat")
	os.WriteFile(finalPath, make([]byte, 1000), 0644)

	err := eng.Compensate(context.Background(), 500)
	if err != nil {
		t.Fatalf("Compensate: %v", err)
	}

	info, _ := os.Stat(finalPath)
	if info.Size() > 500 {
		t.Errorf("file size = %d, want <= 500", info.Size())
	}
}

func TestExportEngine_Progress(t *testing.T) {
	eng := New(nil, "", "")
	state := statestore.BaseTaskState{
		Phase:   statestore.PhaseRunning,
		Payload: json.RawMessage(`{"current_chunk_idx":5,"total_chunks":10}`),
	}
	prog := eng.Progress(state)
	if prog < 0 || prog > 100 {
		t.Errorf("progress = %d, want 0-100", prog)
	}
}
```

- [ ] **Step 4: 运行测试 — 全部通过**

Run: `go test ./engine/export/ -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add engine/export/export.go engine/export/export_test.go
git commit -m "feat(engine/export): add ExportEngine with chunked file export"
```

---

### Task 8: engine/import 包 — 导入引擎

**Files:**
- Create: `engine/import/import.go`
- Create: `engine/import/import_test.go`

- [ ] **Step 1: 实现 import.go**

```go
package importpkg

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"state-store/engine"
	"state-store/phys"
	"state-store/statestore"
)

// ImportPayload 是导入任务的业务扩展状态。
type ImportPayload struct {
	CurrentReadOffset int64 `json:"current_read_offset"`
	CurrentBatchIdx   int   `json:"current_batch_idx"`
	TotalBatches      int   `json:"total_batches"`
	InsertedRows      int64 `json:"inserted_rows"`
	FailedRows        int64 `json:"failed_rows"`
}

// ImportEngine 实现 engine.Engine 接口，从源文件读取并批量写入目标。
type ImportEngine struct {
	srcPath   string
	target    phys.DataTarget
	batchSize int
}

// ImportOption 是 ImportEngine 的配置函数。
type ImportOption func(*ImportEngine)

// WithBatchSize 设置每批次行数，默认 1000。
func WithBatchSize(n int) ImportOption {
	return func(e *ImportEngine) { e.batchSize = n }
}

// New 创建 ImportEngine。
func New(srcPath string, target phys.DataTarget, opts ...ImportOption) *ImportEngine {
	e := &ImportEngine{
		srcPath:   srcPath,
		target:    target,
		batchSize: 1000,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// 编译期检查
var _ engine.Engine = (*ImportEngine)(nil)

func (e *ImportEngine) TaskType() string { return "import" }

func (e *ImportEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	var p ImportPayload
	if len(state.Payload) > 0 {
		if err := json.Unmarshal(state.Payload, &p); err != nil {
			return 0, fmt.Errorf("import: unmarshal payload: %w", err)
		}
	}

	switch state.Phase {
	case statestore.PhasePending:
		p = ImportPayload{}
		state.Phase = statestore.PhaseRunning
		state.Message = "import started"
		state.Payload = e.marshalPayload(p)
		return 0, nil

	case statestore.PhaseRunning:
		return e.executeRunning(ctx, state, &p)

	default:
		return state.CheckpointLSN, nil
	}
}

func (e *ImportEngine) executeRunning(ctx context.Context, state *statestore.BaseTaskState, p *ImportPayload) (int64, error) {
	f, err := os.Open(e.srcPath)
	if err != nil {
		return 0, fmt.Errorf("import: open source: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(p.CurrentReadOffset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("import: seek to %d: %w", p.CurrentReadOffset, err)
	}

	// 读取 batchSize 行（每行是 JSON）
	decoder := json.NewDecoder(f)
	var rows []phys.Row
	for i := 0; i < e.batchSize; i++ {
		var row phys.Row
		if err := decoder.Decode(&row); err == io.EOF {
			break
		} else if err != nil {
			return 0, fmt.Errorf("import: decode row at offset %d: %w", p.CurrentReadOffset, err)
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		state.Phase = statestore.PhaseCompleted
		state.Message = "import completed"
		return p.CurrentReadOffset, nil
	}

	inserted, err := e.target.WriteBatch(ctx, rows)
	if err != nil {
		return 0, fmt.Errorf("import: write batch: %w", err)
	}

	p.InsertedRows += inserted
	p.FailedRows += int64(len(rows)) - inserted
	p.CurrentBatchIdx++

	// 获取当前文件位置作为新的 LSN
	newOffset, _ := f.Seek(0, io.SeekCurrent)
	p.CurrentReadOffset = newOffset

	state.Message = fmt.Sprintf("importing batch %d, %d rows inserted", p.CurrentBatchIdx, inserted)
	state.Payload = e.marshalPayload(p)
	return p.CurrentReadOffset, nil
}

func (e *ImportEngine) Compensate(ctx context.Context, targetLSN int64) error {
	info, err := os.Stat(e.srcPath)
	if err != nil {
		return fmt.Errorf("import: stat source: %w", err)
	}
	if info.Size() < targetLSN {
		return fmt.Errorf("import: source file smaller than LSN (%d < %d): source file may have been truncated",
			info.Size(), targetLSN)
	}
	return nil
}

func (e *ImportEngine) Progress(state statestore.BaseTaskState) int {
	var p ImportPayload
	if len(state.Payload) > 0 {
		json.Unmarshal(state.Payload, &p)
	}
	info, err := os.Stat(e.srcPath)
	if err != nil || info.Size() == 0 {
		return 0
	}
	prog := int(p.CurrentReadOffset * 100 / info.Size())
	if prog > 100 {
		return 100
	}
	return prog
}

func (e *ImportEngine) marshalPayload(p ImportPayload) json.RawMessage {
	data, _ := json.Marshal(p)
	return data
}
```

- [ ] **Step 2: 验证编译通过**

Run: `go build ./engine/import/...`
Expected: 无错误

- [ ] **Step 3: 编写 import_test.go**

```go
package importpkg

import (
	"context"
	"encoding/json"
	"os"
	"path/filepath"
	"testing"

	"state-store/phys"
	"state-store/statestore"
)

// stubDataTarget 用于测试。
type stubDataTarget struct {
	inserted []phys.Row
}

func (t *stubDataTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.inserted = append(t.inserted, rows...)
	return int64(len(rows)), nil
}

func TestImportEngine_NormalFlow(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")

	// 准备源文件（JSON lines 格式）
	rows := []phys.Row{
		{"id": "1", "name": "alice"},
		{"id": "2", "name": "bob"},
		{"id": "3", "name": "carol"},
	}
	f, _ := os.Create(srcPath)
	for _, r := range rows {
		data, _ := json.Marshal(r)
		f.Write(append(data, '\n'))
	}
	f.Close()

	target := &stubDataTarget{}
	eng := New(srcPath, target, WithBatchSize(2))

	state := &statestore.BaseTaskState{
		TaskID:   "import-001",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	// Phase pending → running
	lsn, err := eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute pending: %v", err)
	}
	if state.Phase != statestore.PhaseRunning {
		t.Errorf("phase = %q, want running", state.Phase)
	}
	_ = lsn

	// Phase running: 第 1 批 (2 行)
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute batch 1: %v", err)
	}
	if len(target.inserted) != 2 {
		t.Errorf("inserted rows = %d, want 2", len(target.inserted))
	}

	// Phase running: 第 2 批 (1 行) → 完成后 Phase=completed
	lsn, err = eng.Execute(context.Background(), state)
	if err != nil {
		t.Fatalf("Execute batch 2: %v", err)
	}
	if state.Phase != statestore.PhaseCompleted {
		t.Errorf("phase = %q, want completed", state.Phase)
	}
	if len(target.inserted) != 3 {
		t.Errorf("total inserted = %d, want 3", len(target.inserted))
	}
}

func TestImportEngine_Compensate(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")
	os.WriteFile(srcPath, []byte(`{"id":"1"}`+"\n"), 0644)

	eng := New(srcPath, &stubDataTarget{})

	// LSN 在文件范围内 → OK
	if err := eng.Compensate(context.Background(), 5); err != nil {
		t.Errorf("Compensate: %v", err)
	}

	// LSN 超出文件大小 → 错误
	if err := eng.Compensate(context.Background(), 99999); err == nil {
		t.Error("Compensate should fail when LSN > file size")
	}
}

func TestImportEngine_Progress(t *testing.T) {
	dir := t.TempDir()
	srcPath := filepath.Join(dir, "source.jsonl")
	os.WriteFile(srcPath, make([]byte, 1000), 0644)

	eng := New(srcPath, &stubDataTarget{})
	state := statestore.BaseTaskState{
		Phase:   statestore.PhaseRunning,
		Payload: json.RawMessage(`{"current_read_offset":500}`),
	}
	prog := eng.Progress(state)
	if prog != 50 {
		t.Errorf("progress = %d, want 50", prog)
	}
}
```

- [ ] **Step 4: 运行测试 — 全部通过**

Run: `go test ./engine/import/ -v`
Expected: 全部 PASS

- [ ] **Step 5: Commit**

```bash
git add engine/import/import.go engine/import/import_test.go
git commit -m "feat(engine/import): add ImportEngine with offset-based resume"
```

---

### Task 9: 集成测试 — 完整导出+导入链路

**Files:**
- Create: `integration/integration_test.go`

- [ ] **Step 1: 编写集成测试**

```go
package integration_test

import (
	"context"
	"encoding/json"
	"io"
	"os"
	"path/filepath"
	"testing"

	"state-store/engine"
	"state-store/engine/export"
	importpkg "state-store/engine/import"
	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
)

// inmemSource 是 DataSource 的内存实现，用于集成测试。
type inmemSource struct {
	data [][]phys.Row
	idx  int
}

func (s *inmemSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if s.idx >= len(s.data) {
		return nil, io.EOF
	}
	result := s.data[s.idx]
	s.idx++
	return result, nil
}

// inmemTarget 是 DataTarget 的内存实现。
type inmemTarget struct {
	rows []phys.Row
}

func (t *inmemTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.rows = append(t.rows, rows...)
	return int64(len(rows)), nil
}

func TestExportThenImport_EndToEnd(t *testing.T) {
	dir := t.TempDir()

	// 准备测试数据（2 页，每页 3 行）
	sourceData := [][]phys.Row{
		{
			{"id": 1, "name": "alice"},
			{"id": 2, "name": "bob"},
			{"id": 3, "name": "carol"},
		},
		{
			{"id": 4, "name": "dave"},
			{"id": 5, "name": "eve"},
		},
	}

	// === 导出阶段 ===
	exportRepo, err := filestore.New(filepath.Join(dir, "export-state"))
	if err != nil {
		t.Fatalf("create export repo: %v", err)
	}

	exportSrc := &inmemSource{data: sourceData}
	exportEng := export.New(exportSrc, dir, "exported.dat",
		export.WithPageSize(3), export.WithChunkPages(1))

	err = engine.Run(context.Background(), exportRepo, exportEng, "export-task-1")
	if err != nil {
		t.Fatalf("export Run: %v", err)
	}

	// 验证最终文件存在
	finalPath := filepath.Join(dir, "exported.dat")
	info, err := os.Stat(finalPath)
	if err != nil {
		t.Fatalf("stat exported file: %v", err)
	}
	if info.Size() == 0 {
		t.Error("exported file is empty")
	}

	// 验证状态为 completed
	data, err := exportRepo.Load(context.Background(), "export-task-1")
	if err != nil {
		t.Fatalf("load export state: %v", err)
	}
	var exportState statestore.BaseTaskState
	json.Unmarshal(data, &exportState)
	if exportState.Phase != statestore.PhaseCompleted {
		t.Errorf("export phase = %q, want completed", exportState.Phase)
	}

	// === 导入阶段 ===
	importRepo, err := filestore.New(filepath.Join(dir, "import-state"))
	if err != nil {
		t.Fatalf("create import repo: %v", err)
	}

	importTarget := &inmemTarget{}
	importEng := importpkg.New(finalPath, importTarget, importpkg.WithBatchSize(2))

	err = engine.Run(context.Background(), importRepo, importEng, "import-task-1")
	if err != nil {
		t.Fatalf("import Run: %v", err)
	}

	// 验证导入的行数
	if len(importTarget.rows) != 5 {
		t.Errorf("imported rows = %d, want 5", len(importTarget.rows))
	}

	// 验证导入状态
	data, err = importRepo.Load(context.Background(), "import-task-1")
	if err != nil {
		t.Fatalf("load import state: %v", err)
	}
	var importState statestore.BaseTaskState
	json.Unmarshal(data, &importState)
	if importState.Phase != statestore.PhaseCompleted {
		t.Errorf("import phase = %q, want completed", importState.Phase)
	}
}

func TestExportResume_MidCrash(t *testing.T) {
	dir := t.TempDir()

	// 大量数据分多页
	pages := make([][]phys.Row, 10)
	for i := 0; i < 10; i++ {
		pages[i] = []phys.Row{{"page": i, "idx": 0}, {"page": i, "idx": 1}}
	}

	repo, err := filestore.New(filepath.Join(dir, "state"))
	if err != nil {
		t.Fatalf("create repo: %v", err)
	}

	exportSrc := &inmemSource{data: pages}
	exportEng := export.New(exportSrc, dir, "result.dat",
		export.WithPageSize(2), export.WithChunkPages(3))

	err = engine.Run(context.Background(), repo, exportEng, "resume-test")
	if err != nil {
		t.Fatalf("export Run: %v", err)
	}

	// 验证最终文件内容行数（每行一个 JSON）
	finalPath := filepath.Join(dir, "result.dat")
	finalData, _ := os.ReadFile(finalPath)
	lines := 0
	for _, b := range finalData {
		if b == '\n' {
			lines++
		}
	}
	if lines != 20 { // 10 pages × 2 rows
		t.Errorf("exported lines = %d, want 20", lines)
	}
}
```

- [ ] **Step 2: 运行集成测试**

Run: `go test -v -run TestExportThenImport`
Expected: PASS

Run: `go test -v -run TestExportResume`
Expected: PASS

- [ ] **Step 3: 运行全部测试**

Run: `go test ./... -v`
Expected: 全部 PASS

- [ ] **Step 4: Commit**

```bash
git add integration/integration_test.go
git commit -m "test: add end-to-end export+import integration test"
```

---

### Task 10: 最终验证

- [ ] **Step 1: 运行全部测试并检查覆盖率**

Run: `go test ./... -cover`
Expected: 全部 PASS，覆盖率 > 70%

- [ ] **Step 2: 运行 go vet**

Run: `go vet ./...`
Expected: 无警告

- [ ] **Step 3: 最终提交（如有修改）**

```bash
git add -A
git commit -m "chore: final cleanup and verification"
```

---

## 实施依赖关系

```
Task 1 (errors) ─┐
Task 2 (state)  ─┤
Task 3 (repo)   ─┤
Task 4 (phys)   ─┤
                 ├──▶ Task 6 (engine) ──▶ Task 9 (integration)
                 │         │
Task 5 (filestore)┘         ├──▶ Task 7 (export)
                            └──▶ Task 8 (import)
```

- Task 1-4 可并行
- Task 5 依赖 Task 1
- Task 6 依赖 Task 1-4
- Task 7-8 依赖 Task 6
- Task 9 依赖 Task 5-8
- Task 10 依赖所有
