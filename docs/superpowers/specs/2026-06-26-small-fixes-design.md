# 小修复设计文档

日期：2026-06-26
范围：7 项小修复——文档化、自动清理、测试探针、死代码清除、死胡同修正、换行符兼容、错误链

## 背景

上轮代码审查识别出 10 个可补强点。其中 3 个归入"大项"（chunk 恢复缺陷、文件锁、不可逆操作兜底），另起设计。本文档覆盖剩余 7 个"小修复"——变更范围确定、不涉及架构决策。

所有改动相互独立，可并行落地。

---

## 修复清单

### ① LSN 时序约定文档化

**文件**：`engine/engine.go`
**类型**：GoDoc 增补

在 `Engine` 接口的 `Execute` 方法注释中追加契约说明，明确"先物理后 checkpoint"的时序约定。

**当前代码**（第 19-21 行）：
```go
// Execute 执行一步业务逻辑并返回新的安全偏移量（LSN）。
// state 为指针，引擎可修改 Phase / Message / Payload。
// Phase=pending 时引擎应初始化 Payload 并转为 running。
Execute(ctx context.Context, state *statestore.BaseTaskState) (newLSN int64, err error)
```

**改为**：
```go
// Execute 执行一步业务逻辑并返回新的安全偏移量（LSN）。
// state 为指针，引擎可修改 Phase / Message / Payload。
// Phase=pending 时引擎应初始化 Payload 并转为 running。
//
// 时序契约：Execute 先执行物理副作用（写文件、写数据库等），再返回新 LSN。
// 框架在 Execute 返回后保存 checkpoint。因此崩溃恢复时，物理系统可能领先于
// checkpoint——副作用已发生但 LSN 未记录。Compensate 只需将物理系统回退/截断
// 到 LSN，无需前滚。
Execute(ctx context.Context, state *statestore.BaseTaskState) (newLSN int64, err error)
```

**理由**：当前只在 README 隐式提及，接口层没有明确约束。自定义引擎实现者容易犯"先 checkpoint 后物理"的错误，导致恢复时数据丢失而非重复，Compensate 语义完全反转。

---

### ③ `.tmp` 孤儿文件自动清理

**文件**：`filestore/filestore.go`
**类型**：函数级改动

在 `Load()` 方法开头增加一行 `os.Remove(r.tmpPath(taskID))`，在读取状态前自动清理可能存在的崩溃孤儿 `.tmp`。

**当前代码**（第 26-38 行）：
```go
func (r *FileRepository) Load(ctx context.Context, taskID string) ([]byte, error) {
    if err := ctx.Err(); err != nil {
        return nil, err
    }
    data, err := os.ReadFile(r.statePath(taskID))
    // ...
}
```

**改为**：
```go
func (r *FileRepository) Load(ctx context.Context, taskID string) ([]byte, error) {
    // 自动清理上次崩溃可能残留的 .tmp（不存在则忽略）
    os.Remove(r.tmpPath(taskID))

    if err := ctx.Err(); err != nil {
        return nil, err
    }
    data, err := os.ReadFile(r.statePath(taskID))
    // ...
}
```

**理由**：
- `Save()` 流程是 tmp → Sync → Rename。任何残留 `.tmp` 一定是崩溃孤儿（Sync 后进程挂掉、Rename 未执行）。此时 `.state` 要么不存在、要么是旧版本，`.tmp` 是半写垃圾。
- `Load()` 是每次 `Run()` 的第一个操作，在此清理时机最早、零额外调用。
- `os.Remove` 对不存在的文件返回错误，直接忽略即可。
- 不改变公开 API，现有的 `Cleanup()` 保留（仍可用于批量清理目录级孤儿）。

---

### ⑤ 幂等探针测试

**文件**：`engine/export/export_test.go`、`engine/import/import_test.go`
**类型**：新增测试函数

各新增一个测试，模拟"Execute 成功但 Save 未发生"后重新 Execute 的场景，验证引擎的幂等性假设在实际行为中成立。

#### Export 侧：`TestEngine_IdempotentExecute_RunningPhase`

```
给定：DataSource 有 3 页数据，pageSize=3, chunkPages=2
步骤：
  1. 执行 Pending→Running
  2. 执行 2 步 running（处理 page 0 和 page 1，形成 1 个 chunk）
  3. 记录此时的 Payload 和 CheckpointLSN
  4. 将 state 的 Payload 和 CheckpointLSN 回退到第 1 步后的值
     （模拟：第 2 步的 Execute 完成了但 Save 未发生）
  5. 用回退后的 state 重新 Execute 第 2 步
  6. 继续 Execute 到 completed
验证：最终文件中行数 = 数据源总行数（无重复、无缺失）
```

#### Import 侧：`TestEngine_IdempotentExecute_RunningPhase`

```
给定：源文件 5 行，batchSize=2
步骤：
  1. 执行 Pending→Running
  2. 执行第 1 批（2 行），记录 Payload
  3. 将 state 的 Payload 回退到第 1 步后的值
     （模拟：第 1 批 Execute 完成但 Save 未发生）
  4. 用回退后的 state 重新 Execute
验证：
  - 总共插入行数 = 5（无重复、无缺失）
```

**理由**：这是方向 ⑤ 的最小落地——不增加框架机制，仅用测试覆盖已知的脆弱窗口。如果引擎的幂等性假设不成立，这些测试会首先暴露问题。

---

### 2.2 删除死代码 `cleanupChunks`

**文件**：`engine/export/export.go`
**类型**：删除

删除第 239-242 行的私有方法：
```go
func (e *Engine) cleanupChunks(total int) {
    for i := 0; i < total; i++ {
        os.Remove(e.chunkPath(i))
    }
}
```

**理由**：公开的 `Cleanup()` 用目录扫描实现（第 245-253 行），此方法从未被调用。保留会误导读者以为存在两套清理逻辑。

---

### 2.3 `PhaseVerifying` 死胡同

**文件**：`engine/engine.go`
**类型**：单行改动

在 `Run()` 的循环条件中增加 `PhaseVerifying`。

**当前代码**（第 65-67 行）：
```go
for state.Phase == statestore.PhasePending ||
    state.Phase == statestore.PhaseRunning ||
    state.Phase == statestore.PhaseMerging {
```

**改为**：
```go
for state.Phase == statestore.PhasePending ||
    state.Phase == statestore.PhaseRunning ||
    state.Phase == statestore.PhaseMerging ||
    state.Phase == statestore.PhaseVerifying {
```

**理由**：循环条件缺少 `verifying` 意味着引擎设置此 phase 后循环直接退出且不报错，状态永久卡在 `verifying`。加入后引擎可以正常使用验证阶段——`Execute` 返回时设置 `completed`/`failed` 即可退出循环。

---

### 2.4 import 引擎换行符偏移修正

**文件**：`engine/import/import.go`
**类型**：函数级改动（~15 行，局部于 `executeRunning`）

#### 问题根因

`bufio.Scanner` 默认 `ScanLines` 内部调用 `dropCR` 剥离 `\r`：

```
输入 "abc\r\n" → Scanner.Bytes()="abc"(len=3), 实际消费 5 字节
输入 "abc\n"   → Scanner.Bytes()="abc"(len=3), 实际消费 4 字节
```

当前 `offset += len(line) + 1` 对 `\r\n` 行少算 1 字节/行，积累后断点恢复位置错误。**且 `dropCR` 已剥离 `\r`，无法从 `Bytes()` 返回值检测原始换行符。**

#### 方案：`bufio.Reader.ReadBytes('\n')` 替代 `bufio.Scanner`

`ReadBytes('\n')` 返回的数据**包含分隔符 `\n`**，`len(line)` 精确等于实际消费字节数。

**替换范围**（第 93-117 行整体重构）：

```go
// 改前（bufio.Scanner）
scanner := bufio.NewScanner(f)
var rows []phys.Row
offset := p.CurrentReadOffset
eof := false

for i := 0; i < e.batchSize; i++ {
    if !scanner.Scan() {
        if err := scanner.Err(); err != nil {
            return 0, fmt.Errorf("import: scan at offset %d: %w", offset, err)
        }
        eof = true
        break
    }
    line := scanner.Bytes()
    offset += int64(len(line) + 1) // BUG: \r\n 下少算 1 字节

    var row phys.Row
    if err := json.Unmarshal(line, &row); err != nil {
        return 0, fmt.Errorf("import: decode row at offset %d: %w", offset, err)
    }
    rows = append(rows, row)
}
```

```go
// 改后（bufio.Reader.ReadBytes）
reader := bufio.NewReader(f)
var rows []phys.Row
offset := p.CurrentReadOffset
eof := false

for i := 0; i < e.batchSize; i++ {
    rawLine, err := reader.ReadBytes('\n')
    if err == io.EOF {
        if len(rawLine) == 0 {
            break
        }
        eof = true
    } else if err != nil {
        return 0, fmt.Errorf("import: read at offset %d: %w", offset, err)
    }
    offset += int64(len(rawLine)) // 精确：包含 \n 或 \r\n，最后一行无换行符时为实际长度
    line := bytes.TrimRight(rawLine, "\r\n")

    var row phys.Row
    if err := json.Unmarshal(line, &row); err != nil {
        return 0, fmt.Errorf("import: decode row at offset %d: %w", offset, err)
    }
    rows = append(rows, row)
}
```

**imports 变更**：增加 `"bytes"` 和 `"io"`（`io.EOF` 已存在于 phys 间接依赖但需显式导入），去掉 `"bufio"` 的 `Scanner` 使用（仍保留 `bufio` 因为使用 `NewReader`）。

**行为差异**：
- `ReadBytes('\n')` 最后一行无换行符时返回 `(data, io.EOF)`，`len(rawLine)` = 实际数据长度，`TrimRight` 无操作
- 空文件：`ReadBytes` 返回 `([], io.EOF)`，`len(rawLine)==0` → break
- 默认 buffer 大小由 `bufio.NewReader` 管理（默认 4096），对大行（>4KB JSON）也可通过增大 buffer 兼容

**理由**：`ReadBytes` 直接返回包含分隔符的原始字节，偏移计算变为 `offset += len(rawLine)`——对任意换行风格（`\n`、`\r\n`、无换行尾行）天然正确，无需判断换行符类型。

---

### 2.5 预定义错误投入使用

**文件**：`filestore/filestore.go`
**类型**：替换 `fmt.Errorf` 为 `%w` 包装

**`Save` 中**（第 51 行）：
```go
// 改前
return fmt.Errorf("filestore: save %s: create tmp: %v", taskID, err)
// 改后
return fmt.Errorf("filestore: save %s: create tmp: %w", taskID, statestore.ErrSaveFailed)
```

**`Save` 中**（第 57 行 write 失败）同上。

**`Save` 中**（第 69 行 rename 失败）同上。

**`Load` 中**（第 35 行，非 `IsNotExist` 路径）：
```go
// 改前
return nil, fmt.Errorf("filestore: load %s: %v", taskID, err)
// 改后
return nil, fmt.Errorf("filestore: load %s: %w", taskID, statestore.ErrLoadFailed)
```

**理由**：调用方可用 `errors.Is(err, statestore.ErrSaveFailed)` 区分"保存失败"和"其他错误"，不丢失底层原因（`%w` 保留原始 err 链）。`statestore/errors.go` 的预定义错误目前零引用，此改动让它们兑现设计意图。

---

## 不改动项（显式排除）

| 编号 | 内容 | 决策 |
|---|---|---|
| ② | 文件锁 | 不做。单进程定位，多实例互斥由上层调度负责 |
| ④ | 不可逆操作兜底 | 大项，另起设计 |
| 2.1 | 导出 chunk 恢复重复 | 大项，另起设计 |

---

## 测试策略

- 现有测试全量回归（`go test ./...`）
- 修复 ⑤ 本身就是新增测试
- 修复 2.4 需要在测试中构造 `\r\n` 源文件验证
- 其余修复不需要新增测试（纯重构/文档/死代码删除）
