# Universal Task StateStore — 设计文档

## 元信息

- **日期**: 2026-06-15
- **项目**: `state-store` (Go module)
- **阶段**: 设计完成，待实施

---

## 1. 范围与目标

### 1.1 范围

构建一个完整的 Go 库，提供：

- **`StateRepository` 接口** — 任务状态的原子持久化抽象
- **文件系统存储实现** — 基于 tmp + Rename 的原子写入
- **`Engine` 框架** — Checkpoint 协议编排与恢复链路
- **内建导出引擎** — 分页提取 → 分块写入 → 合并
- **内建导入引擎** — 读取源文件 → 批量写入目标

### 1.2 非范围

- Redis / RDBMS 存储实现（后续版本）
- HTTP/gRPC 服务层
- 任务调度器
- 并发管理（goroutine 池等）
- 内建重试策略

### 1.3 核心约束

| 约束 | 决策 |
|------|------|
| 架构形态 | 纯 Go 库（SDK），非独立服务 |
| 并发模型 | 同步阻塞 API，调用方管理 goroutine 和生命周期 |
| 错误处理 | 引擎返回 error，不自动保存状态，不内建重试 |
| 物理系统适配 | 库定义抽象接口（DataSource/DataTarget），调用方实现 |
| Payload 扩展性 | 完全自由 `json.RawMessage`，库不解析自定义载荷 |

---

## 2. 包结构

```
state-store/
├── statestore/           # 根包：StateRepository 接口 + BaseTaskState 模型 + TaskPhase 常量
│   ├── repository.go     # StateRepository interface
│   ├── state.go          # BaseTaskState, TaskPhase
│   └── errors.go         # Sentinel errors
├── phys/                 # 物理系统抽象接口
│   └── phys.go           # DataSource, DataTarget, Row
├── filestore/            # 文件系统 StateRepository 实现
│   └── filestore.go      # FileRepository
├── engine/               # 引擎框架：Engine 接口 + Checkpoint 协议编排 + 恢复编排
│   └── engine.go         # Engine interface, Run()
├── engine/export/        # 导出引擎
│   └── export.go         # ExportEngine (实现 engine.Engine)
└── engine/import/        # 导入引擎
    └── import.go         # ImportEngine (实现 engine.Engine)
```

依赖方向（自上而下）：调用方 → engine/export, engine/import → engine → statestore, phys → (无外部依赖)

---

## 3. 核心接口

### 3.1 StateRepository（statestore 包）

```go
type StateRepository interface {
    Load(ctx context.Context, taskID string) ([]byte, error)
    // 任务不存在 → nil, nil（不是 error）

    Save(ctx context.Context, taskID string, state []byte) error
    // 原子全量替换。返回 nil → 已完整持久化。返回 error → 状态未变。

    Delete(ctx context.Context, taskID string) error
}
```

### 3.2 Engine（engine 包）

```go
type Engine interface {
    // TaskType 返回此引擎处理的任务类型标识（"export" / "import"）。
    TaskType() string

    // Execute 执行一步业务逻辑。
    // state 为当前状态（指针，引擎可修改 Phase/Message/Payload）。
    // Phase=pending 时，引擎应在 Execute 内初始化 Payload 并转为 running。
    // 返回 newLSN 为本次执行后的安全偏移量。
    Execute(ctx context.Context, state *statestore.BaseTaskState) (newLSN int64, err error)

    // Compensate 补偿物理系统，使其对齐到 targetLSN。
    // 仅在恢复路径调用（Load 后发现 Phase 为 running/merging）。
    Compensate(ctx context.Context, targetLSN int64) error

    // Progress 根据 state 计算 0-100 的进度。
    Progress(state statestore.BaseTaskState) int
}
```

### 3.3 DataSource / DataTarget（phys 包）

```go
type Row map[string]interface{}

type DataSource interface {
    FetchPage(ctx context.Context, page int, pageSize int) ([]Row, error)
    // 返回 io.EOF 表示无更多数据
}

type DataTarget interface {
    WriteBatch(ctx context.Context, rows []Row) (inserted int64, err error)
    // 契约：实现方必须保证写入幂等性（UPSERT / 唯一键去重 / 预检）
}
```

---

## 4. 状态模型

### 4.1 BaseTaskState

```go
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

### 4.2 Phase 状态机

```
pending ──Run()──▶ running ──引擎判断──▶ merging ──合并完成──▶ completed
                       │                    │
                       ├──Execute error──▶ failed
                       ├──Execute done──▶ completed
                       └──ctx cancel──▶ canceled
```

- 引擎在 `Execute` 内部负责判断和设置 Phase（running→merging, running→completed）
- 框架负责：pending→running, running→failed（Execute 返回 error）, *→canceled（ctx 取消）

### 4.3 引擎 Payload

**ExportPayload**（`engine/export` 包）:
```go
type ExportPayload struct {
    CurrentPage      int   `json:"current_page"`
    CurrentChunkIdx  int   `json:"current_chunk_idx"`
    CurrentChunkSize int64 `json:"current_chunk_size"`
    TotalChunks      int   `json:"total_chunks"`
    FinalFileSize    int64 `json:"final_file_size"`
    MergedChunkIdx   int   `json:"merged_chunk_idx"`
}
```

**ImportPayload**（`engine/import` 包）:
```go
type ImportPayload struct {
    CurrentReadOffset int64 `json:"current_read_offset"`
    CurrentBatchIdx   int   `json:"current_batch_idx"`
    TotalBatches      int   `json:"total_batches"`
    InsertedRows      int64 `json:"inserted_rows"`
    FailedRows        int64 `json:"failed_rows"`
}
```

---

## 5. 引擎框架

### 5.1 engine.Run() 主循环

**初始状态约定**：`Run()` 内部自动处理初始化与恢复。Load 返回 nil 时自动构建 Phase=pending 的初始状态，Load 返回已有状态时走恢复链路。

```go
func Run(ctx context.Context, repo StateRepository, eng Engine, taskID string) error {
    // 1. 加载或初始化状态
    data, err := repo.Load(ctx, taskID)
    if err != nil {
        return err
    }
    var state *statestore.BaseTaskState
    if data == nil {
        // 首次运行：自动初始化
        state = &statestore.BaseTaskState{
            TaskID:   taskID,
            TaskType: eng.TaskType(),
            Phase:    statestore.PhasePending,
        }
    } else {
        // 恢复
        json.Unmarshal(data, &state)
    }

    // 2. 恢复：物理系统向 Store 对齐
    if state.Phase == PhaseRunning || state.Phase == PhaseMerging {
        if err := eng.Compensate(ctx, state.CheckpointLSN); err != nil {
            return err
        }
    }

    // 3. 主循环：Execute → Save，每次迭代一个 checkpoint
    for state.Phase == PhasePending || state.Phase == PhaseRunning || state.Phase == PhaseMerging {
        newLSN, err := eng.Execute(ctx, state)
        if err != nil {
            return err
        }

        state.CheckpointLSN = newLSN
        state.Progress = eng.Progress(*state)
        if err := repo.Save(ctx, state.TaskID, marshal(state)); err != nil {
            return fmt.Errorf("checkpoint save: %w", err)
        }

        if state.Phase == PhaseCompleted || state.Phase == PhaseFailed {
            break
        }
    }
    return nil
}
```

调用方示例：
```go
// 首次运行或恢复 — 调用方无需区分
err := engine.Run(ctx, repo, exportEng, "export-001")
```

Engine 接口需要额外暴露 `TaskType() string` 方法供框架初始化时使用。

### 5.2 Checkpoint 协议映射

| Spec 步骤 | 框架实现 | 保证 |
|-----------|----------|------|
| 1. 执行动作 | `eng.Execute()` — 引擎在物理系统产生副作用 | 引擎保证 newLSN ≤ 物理系统实际状态 |
| 2. 持久化动作 | 引擎内部自行决定（fsync / COMMIT） | 调用方通过实现控制持久化级别 |
| 3. 提交 Checkpoint | `repo.Save()` — 原子写入 | Save 成功 → 新 LSN 持久化 |

### 5.3 恢复链路

```
Run() 被调用
  → Load state（Store 即真相）
  → 检测 Phase 为 running/merging
  → 调用 Compensate(CheckpointLSN)：物理系统截断/对齐
  → 进入 Execute 循环：从断点继续
```

---

## 6. 文件系统存储（filestore）

### 6.1 Save 原子写入

1. 写入 `<taskID>.state.tmp`（完整序列化字节流）
2. `tmpFile.Sync()` — 强制落盘
3. `os.Rename(tmpPath, finalPath)` — 原子替换
4. （可选）`dir.Sync()` — 目录元数据持久化

原子性保证：POSIX `rename(2)` 是原子操作。中途崩溃只可能残留 `.tmp` 文件，绝不出现 `.state` 半写。

### 6.2 文件布局

```
/var/lib/statestore/
├── task-export-001.state
├── task-import-002.state
├── task-export-003.state.tmp    ← 崩溃残留，可清理
└── ...
```

### 6.3 Cleanup

提供 `Cleanup()` 方法扫描删除残留 `.tmp` 文件，调用方在应用启动时调用。

---

## 7. 导出引擎（engine/export）

### 7.1 构造

```go
func New(src phys.DataSource, outputDir, outputFile string, opts ...ExportOption) *ExportEngine
```

选项：`WithPageSize(n)`（默认 1000）、`WithChunkPages(n)`（默认 10）

### 7.2 分块文件命名

```
<outputFile>.chunk_<N>.tmp
```

示例：`result.dat.chunk_0.tmp`、`result.dat.chunk_1.tmp`

同一目录不同导出任务通过 outputFile 前缀隔离，互不冲突。

### 7.3 Execute

- **Phase pending**: 初始化 ExportPayload（CurrentPage=0, CurrentChunkIdx=0 等），设置 Phase = running，返回 newLSN=0
- **Phase running**: `FetchPage()` → 写入 `chunk_N.tmp` → 满 chunkPages 页后切换块 → newLSN = 累计字节总数
- 遇到 `io.EOF` → 设置 Phase = merging
- **Phase merging**: 依次将 `chunk_N.tmp` 追加到最终文件 → 合并完成 → Phase = completed → 清理分块文件

### 7.4 Compensate

- running: 截断 `chunk_<CurrentChunkIdx>.tmp` 到 `CurrentChunkSize`
- merging: 截断 `<outputFile>` 到 `FinalFileSize`

### 7.5 CheckpointLSN 语义

已安全写入磁盘的总字节数。running 阶段 = 已完成块 + 当前块安全字节；merging 阶段 = 最终文件安全字节。

### 7.6 Progress

- running: `min(90, CurrentChunkIdx * 100 / TotalChunks)`
- merging: `90 + MergedChunkIdx * 10 / TotalChunks`

---

## 8. 导入引擎（engine/import）

### 8.1 构造

```go
func New(srcPath string, target phys.DataTarget, opts ...ImportOption) *ImportEngine
```

选项：`WithBatchSize(n)`（默认 1000）

### 8.2 Execute

- **Phase pending**: 初始化 ImportPayload（CurrentReadOffset=0, CurrentBatchIdx=0 等），设置 Phase = running，返回 newLSN=0
- **Phase running**: 从 `CurrentReadOffset` 定位源文件 → 读取 batchSize 行 → `target.WriteBatch()` → newLSN = 文件当前读取位置

### 8.3 Compensate

验证源文件存在且大小 ≥ `CheckpointLSN`。导入的补偿本质上是 Execute 的读取位置跳过已确认数据 + DataTarget 幂等写入。

### 8.4 DataTarget 幂等契约

导入恢复的正确性依赖 DataTarget 实现方保证幂等写入。实现方可使用 `INSERT ... ON CONFLICT DO NOTHING`、唯一键预检等方式。

### 8.5 CheckpointLSN 语义

源文件中已安全解析并写入目标的字节偏移量。

### 8.6 Progress

`CurrentReadOffset * 100 / SourceFileSize`

---

## 9. 错误处理

### 9.1 Sentinel Errors

```go
// statestore 包
var (
    ErrSaveFailed = errors.New("statestore: save failed")
    ErrLoadFailed = errors.New("statestore: load failed")
)

// engine 包
var (
    ErrCompensateFailed = errors.New("engine: compensate failed")
    ErrInvalidState     = errors.New("engine: invalid state")
)
```

### 9.2 调用方处理模式

```go
err := engine.Run(ctx, repo, exportEng, taskID)
if err != nil {
    // 1. 检查 ctx.Err() 区分取消和真实错误
    // 2. 决定重试 / Resume / 放弃
    // 3. 可手动 repo.Load() 查看最后保存的状态
}
```

无内建重试，调用方控制。

---

## 10. 测试策略

| 层级 | 测试内容 | 方式 |
|------|----------|------|
| filestore | Save 原子性（崩溃模拟）、Load nil,nil、Delete 幂等 | 单元测试 + 临时目录 |
| engine 框架 | 恢复链路（Compensate 被调用）、正常链路（Execute→Save）、ctx 取消 | mock Engine 实现 |
| export 引擎 | 正常导出、io.EOF 切换 merging、Compensate 截断正确性、Progress | mock DataSource + filestore |
| import 引擎 | 正常导入、断点恢复（LSN 定位）、DataTarget 幂等验证 | mock DataTarget + filestore |
| 集成 | 完整导出+导入链路 | 临时目录 + mock 物理接口 |

---

## 附录：与原始 Spec 的映射

| 原始 Spec 章节 | 设计中的位置 |
|----------------|-------------|
| 1. 概述 | §1 范围与目标 |
| 2. 核心设计理念 | §5 引擎框架（Store 即真相、LSN、两步提交） |
| 3. 接口契约 | §3.1 StateRepository |
| 4. 状态模型 | §4 状态模型 |
| 5. 一致性要求 | §6.1 原子写入 |
| 6. 介质实现 | §6 文件系统存储 |
| 7. 容错恢复 | §5.2/5.3 + §7.4 + §8.3 |
