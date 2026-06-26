# state-store

一个 Go 异步任务状态持久化与断点恢复框架，零外部依赖。

## 概述

`state-store` 为长时间运行的异步任务提供 **checkpoint / resume** 能力。它将任务状态检查点（checkpoint）协议抽象为通用框架，让引擎只需关注业务逻辑，框架自动处理初始化、状态持久化和崩溃恢复编排。

适用场景：数据导出/导入、ETL 流水线、批量数据处理等需要中断恢复的长时间任务。

## 目录

- [安装](#安装)
- [快速开始](#快速开始)
- [架构](#架构)
- [核心概念](#核心概念)
  - [任务阶段](#任务阶段taskphase)
  - [检查点 LSN](#检查点-lsn)
  - [崩溃恢复流程](#崩溃恢复流程)
- [内置引擎](#内置引擎)
  - [Export 引擎](#export-引擎-engineexport)
  - [Import 引擎](#import-引擎-engineimport)
- [实现自定义引擎](#实现自定义引擎)
- [持久化后端](#持久化后端)
  - [FileRepository](#filerepository-filestore)
  - [自定义后端](#自定义后端)
- [物理层抽象](#物理层抽象)
  - [DataSource](#datasource导出数据源)
  - [DataTarget](#datatarget导入目标)
- [测试](#测试)
- [许可](#许可)

## 安装

```bash
go get state-store
```

要求 Go 1.26+。

## 快速开始

```go
package main

import (
    "context"
    "state-store/engine"
    "state-store/engine/export"
    "state-store/filestore"
)

func main() {
    ctx := context.Background()

    // 1. 创建状态仓库（持久化 checkpoint）
    repo, _ := filestore.New("./task-state")

    // 2. 创建导出引擎
    eng := export.New(
        myDataSource,            // 实现 phys.DataSource 的数据源
        "./output",              // 输出目录
        "result.jsonl",          // 输出文件名
        export.WithPageSize(1000),
        export.WithChunkPages(10),
    )

    // 3. 运行任务（自动崩溃恢复 + 断点续传）
    if err := engine.Run(ctx, repo, eng, "export-001"); err != nil {
        panic(err)
    }

    // 4. 清理分块临时文件
    eng.(*export.Engine).Cleanup()
}
```

运行 `go run ./cmd/demo/` 查看完整的导出 → 导入 → 崩溃恢复演示。

## 架构

```
┌─────────────────────────────────────────────┐
│                 engine.Run()                 │
│   Load → Compensate → Execute → Save 循环    │
├─────────────────────────────────────────────┤
│              engine.Engine 接口               │
│     TaskType / Execute / Compensate / Progress│
├──────────────┬──────────────────────────────┤
│  export 引擎  │         import 引擎           │
│  分页→分块→合并 │     逐行读取→批量写入       │
├──────────────┴──────────────────────────────┤
│               phys 抽象层                     │
│      DataSource / DataTarget                 │
├─────────────────────────────────────────────┤
│              statestore 领域层                 │
│   BaseTaskState / StateRepository / 错误      │
├─────────────────────────────────────────────┤
│          filestore — 文件持久化实现             │
│           tmp → Sync → Rename                │
└─────────────────────────────────────────────┘
```

## 核心概念

### 任务阶段（TaskPhase）

```
pending → running → merging → verifying → completed
                ↘              ↘          ↗
                 failed ← ← ← ← ← ← ← ←
                 canceled
```

| 阶段 | 含义 |
|------|------|
| `pending` | 等待执行，需初始化 Payload |
| `running` | 执行中，每步保存 checkpoint |
| `merging` | 中间产物整合（导出专用） |
| `verifying` | 结果校验（预留） |
| `completed` | 成功完成 |
| `failed` | 执行失败 |
| `canceled` | 被取消 |

### 检查点 LSN

LSN（Log Sequence Number）是引擎定义的逻辑偏移量，用于标识任务已安全完成的进度：

- **导出引擎**：已写入最终文件的累计字节数
- **导入引擎**：源文件的字节偏移量

框架不解释 LSN 的语义，只负责在恢复时将其传递给 `Compensate`。

### 崩溃恢复流程

1. `Run()` 调用 `Load()` 加载状态
2. 若 Phase 为 `running` 或 `merging`（上次未正常结束），调用 `Compensate(LSN)` 对齐物理系统
3. 进入 Execute → Save 循环，从断点继续
4. 到达终态（completed/failed）后退出

## 内置引擎

### Export 引擎 (`engine/export`)

从 `phys.DataSource` 分页读取数据，写入分块文件，最终合并为单个输出文件。

```
DataSource.FetchPage(page) → chunk_0.tmp
DataSource.FetchPage(page+1) → ...
                             → chunk_N.tmp
                                         ↘
                              合并 → output.jsonl
```

配置选项：
- `WithPageSize(n)` — 每页行数，默认 1000
- `WithChunkPages(n)` — 每个分块包含的页数，默认 10

### Import 引擎 (`engine/import`)

逐行读取 JSONL 文件，批量写入 `phys.DataTarget`。使用 `bufio.Scanner` 精确追踪字节偏移，支持断点续传。

配置选项：
- `WithBatchSize(n)` — 每批次行数，默认 1000

## 实现自定义引擎

实现 `engine.Engine` 接口的四个方法即可：

```go
type MyEngine struct{}

func (e *MyEngine) TaskType() string { return "my-task" }

func (e *MyEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
    // 根据 state.Phase 决定行为：
    //   - pending: 初始化 Payload，转为 running
    //   - running: 执行一步业务逻辑，更新 Payload
    // 返回新的 LSN
}

func (e *MyEngine) Compensate(ctx context.Context, targetLSN int64) error {
    // 将物理系统对齐到 targetLSN
    // 仅恢复时调用（Phase=running/merging）
}

func (e *MyEngine) Progress(state statestore.BaseTaskState) int {
    // 返回 0-100 的进度百分比
}
```

**关键契约：**
- **时序约定**：`Execute` 先执行物理副作用，框架后保存 checkpoint。崩溃恢复时物理系统可能领先于 checkpoint，`Compensate` 只需截断/回退，无需前滚。
- `Execute` 必须是幂等步骤——同一步可能因崩溃而重新执行
- `Compensate` 应回滚/截断超出 LSN 的部分，而非追加
- Payload 使用 `json.RawMessage`，由引擎自行序列化/反序列化
- **副作用约束**：`Execute` 的物理副作用必须可补偿（可截断或可幂等重放）。不可逆操作（发邮件、扣款、发送消息队列等）应在调度层使用 outbox / saga 模式处理

## 持久化后端

### FileRepository (`filestore`)

基于本地文件系统，使用 **写 .tmp → Sync → Rename** 保证写入原子性。状态文件以 `.state` 为后缀，存放在指定目录下。`New()` 自动清理上次崩溃残留的 `.tmp` 文件。

```go
repo, _ := filestore.New("/var/state-store/tasks")
```

### 自定义后端

实现 `statestore.StateRepository` 接口即可接入数据库、对象存储等其他后端：

```go
type StateRepository interface {
    Load(ctx context.Context, taskID string) ([]byte, error)
    Save(ctx context.Context, taskID string, state []byte) error
    Delete(ctx context.Context, taskID string) error
}
```

约定：
- `Load` 对不存在的任务返回 `(nil, nil)`，不返回 error
- `Save` 为原子全量替换，不允许部分合并
- `Delete` 幂等，删除不存在的任务不报错

## 运行时配置

### 重试机制

`Run()` 支持通过 `WithRetry` 选项启用 Execute 步骤的自动重试，适用于网络抖动等瞬态错误：

```go
engine.Run(ctx, repo, eng, "task-001",
    engine.WithRetry(3, 5*time.Second), // 最多重试 3 次，间隔 5 秒
)
```

重试仅适用于 `Execute` 步骤；`Load` / `Save` / `Compensate` 错误不重试。

## 物理层抽象

### DataSource（导出数据源）

```go
type DataSource interface {
    FetchPage(ctx context.Context, page int, pageSize int) ([]Row, error)
}
```

返回 `io.EOF` 表示无更多数据。

### DataTarget（导入目标）

```go
type DataTarget interface {
    WriteBatch(ctx context.Context, rows []Row) (int64, error)
}
```

**必须保证写入幂等性**——崩溃恢复时已入库但未 checkpoint 的数据会被重新写入。建议通过 UPSERT、唯一键去重等方式处理。

## 测试

```bash
go test ./...              # 全部测试
go test -v ./integration/  # 端到端集成测试（含崩溃恢复场景）
```

## 许可

MIT
