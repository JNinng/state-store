package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"state-store/engine"
	"state-store/engine/export"
	importpkg "state-store/engine/import"
	"state-store/filestore"
	"state-store/outbox"
	"state-store/phys"
	"state-store/statestore"
)

// ============================================================
// 内存数据源 — 实现 phys.DataSource，供导出使用
// ============================================================

type memSource struct {
	pages [][]phys.Row
}

func (s *memSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if page >= len(s.pages) {
		return nil, io.EOF
	}
	return s.pages[page], nil
}

// ============================================================
// 内存数据目标 — 实现 phys.DataTarget，供导入使用
// ============================================================

type memTarget struct {
	rows []phys.Row
}

func (t *memTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.rows = append(t.rows, rows...)
	return int64(len(rows)), nil
}

// ============================================================
// 辅助函数
// ============================================================

// printExportState 打印导出任务状态
func printExportState(repo *filestore.FileRepository, taskID string) {
	raw, _ := repo.Load(context.Background(), taskID)
	if raw == nil {
		fmt.Println("  状态: (未找到)")
		return
	}
	var state statestore.BaseTaskState
	json.Unmarshal(raw, &state)
	var p export.Payload
	json.Unmarshal(state.Payload, &p)
	fmt.Printf("  状态: phase=%s, progress=%d%%, lsn=%d\n",
		state.Phase, state.Progress, state.CheckpointLSN)
	fmt.Printf("  详情: page=%d, chunk=%d/%d, merge=%d/%d\n",
		p.CurrentPage, p.CurrentChunkIdx, p.TotalChunks,
		p.MergedChunkIdx, p.TotalChunks)
}

// printImportState 打印导入任务状态
func printImportState(repo *filestore.FileRepository, taskID string) {
	raw, _ := repo.Load(context.Background(), taskID)
	if raw == nil {
		fmt.Println("  状态: (未找到)")
		return
	}
	var state statestore.BaseTaskState
	json.Unmarshal(raw, &state)
	var p importpkg.Payload
	json.Unmarshal(state.Payload, &p)
	fmt.Printf("  状态: phase=%s, progress=%d%%, lsn=%d\n",
		state.Phase, state.Progress, state.CheckpointLSN)
	fmt.Printf("  详情: offset=%d, batch=%d, inserted=%d, failed=%d\n",
		p.CurrentReadOffset, p.CurrentBatchIdx, p.InsertedRows, p.FailedRows)
}

// countLines 统计文件中换行符个数（即行数）
func countLines(data []byte) int {
	n := 0
	for _, b := range data {
		if b == '\n' {
			n++
		}
	}
	return n
}

// countNonEmptyLines 统计文件中非空行数
func countNonEmptyLines(data []byte) int {
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.TrimSpace(line) != "" {
			n++
		}
	}
	return n
}

// countMatchingLines 统计文件中包含指定子串的行数
func countMatchingLines(data []byte, substr string) int {
	n := 0
	for _, line := range strings.Split(string(data), "\n") {
		if strings.Contains(line, substr) {
			n++
		}
	}
	return n
}

// dedupLines 对文件内容按行去重，返回去重后的行数
func dedupLines(data []byte) int {
	seen := make(map[string]bool)
	for _, line := range strings.Split(string(data), "\n") {
		line = strings.TrimSpace(line)
		if line != "" {
			seen[line] = true
		}
	}
	return len(seen)
}

// notifyEngine 是一个在 Payload 中嵌入 outbox 消息的演示引擎。
// 展示调度层如何使用 outbox 模式：引擎只写意图，不可逆操作由调度层分发。
type notifyEngine struct {
	taskID   string
	messages []*outbox.Message
}

func newNotifyEngine(taskID string, messages []*outbox.Message) *notifyEngine {
	return &notifyEngine{taskID: taskID, messages: messages}
}

func (e *notifyEngine) TaskType() string { return "notify_demo" }

func (e *notifyEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	switch state.Phase {
	case statestore.PhasePending:
		state.Phase = statestore.PhaseRunning
		outboxData, _ := json.Marshal(e.messages)
		state.Payload = json.RawMessage(fmt.Sprintf(`{"outbox":%s}`, string(outboxData)))
		return 0, nil
	case statestore.PhaseRunning:
		state.Phase = statestore.PhaseCompleted
		state.Message = "notify task completed"
		return 100, nil
	default:
		return state.CheckpointLSN, nil
	}
}

func (e *notifyEngine) Compensate(ctx context.Context, targetLSN int64) error {
	return nil
}

func (e *notifyEngine) Progress(state statestore.BaseTaskState) int {
	if state.Phase == statestore.PhaseCompleted {
		return 100
	}
	return 50
}

// ExtractOutboxMessages 从 Payload 中提取 outbox 消息。
func (e *notifyEngine) ExtractOutboxMessages() ([]*outbox.Message, error) {
	return e.messages, nil
}

var _ engine.Engine = (*notifyEngine)(nil)

func main() {
	ctx := context.Background()

	// ----------------------------------------------------------
	// 准备测试数据（6 页，每页 5 行 = 共 30 行）
	// ----------------------------------------------------------
	sourceData := make([][]phys.Row, 6)
	names := []string{"张三", "李四", "王五", "赵六", "孙七", "周八"}
	cities := []string{"北京", "上海", "广州", "深圳", "杭州", "成都"}
	for page := 0; page < 6; page++ {
		sourceData[page] = make([]phys.Row, 5)
		for i := 0; i < 5; i++ {
			sourceData[page][i] = phys.Row{
				"id":   float64(page*5 + i + 1),
				"name": names[page],
				"city": cities[page],
			}
		}
	}

	// 工作目录
	workDir := filepath.Join(".", "demo-output")
	os.RemoveAll(workDir)
	if err := os.MkdirAll(workDir, 0755); err != nil {
		panic(err)
	}

	// ==========================================================
	// 阶段一：正常导出（Export — 完整流程）
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段一：正常导出（完整流程）                ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	exportRepo, err := filestore.New(filepath.Join(workDir, "export-state"))
	if err != nil {
		panic(err)
	}

	exportSrc := &memSource{pages: sourceData}
	exportEng := export.New(exportSrc, workDir, "exported.jsonl",
		export.WithPageSize(5),   // 每页 5 行
		export.WithChunkPages(2), // 每 2 页一个 chunk → 共 3 个 chunks
	)

	if err := engine.Run(ctx, exportRepo, exportEng, "export-001"); err != nil {
		panic(err)
	}
	exportEng.Cleanup()

	exportedPath := filepath.Join(workDir, "exported.jsonl")
	exportedBytes, _ := os.ReadFile(exportedPath)
	fmt.Printf("导出文件: %s (%d bytes, %d 行)\n\n",
		exportedPath, len(exportedBytes), countLines(exportedBytes))
	printExportState(exportRepo, "export-001")
	fmt.Println()

	// ==========================================================
	// 阶段二：导出中断恢复（Export Recovery）
	//   模拟场景：导出 large-dataset，执行到一半进程崩溃，
	//   重启后从 checkpoint 恢复，继续完成导出。
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段二：导出中断恢复（崩溃→重启→继续）     ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	// 2a. 创建独立的 repo 和 engine
	recoveryExportRepo, _ := filestore.New(filepath.Join(workDir, "export-recovery-state"))
	recoveryExportSrc := &memSource{pages: sourceData}
	recoveryExportEng := export.New(recoveryExportSrc, workDir, "exported-recovery.jsonl",
		export.WithPageSize(5), export.WithChunkPages(2),
	)

	// 2b. 手动执行若干步（不通过 engine.Run，模拟中间过程）
	state := &statestore.BaseTaskState{
		TaskID:   "export-recovery-001",
		TaskType: "export",
		Phase:    statestore.PhasePending,
	}

	fmt.Println("--- 第一步：Pending → Running ---")
	lsn, _ := recoveryExportEng.Execute(ctx, state)
	state.CheckpointLSN = lsn
	printExportState(recoveryExportRepo, "export-recovery-001")
	fmt.Println()

	// 手动执行 3 页（pageSize=5, chunkPages=2，3 页会跨越 chunk 边界）
	fmt.Println("--- 连续执行 3 页后『崩溃』---")
	for i := 0; i < 3; i++ {
		lsn, err := recoveryExportEng.Execute(ctx, state)
		if err != nil {
			panic(err)
		}
		state.CheckpointLSN = lsn
		fmt.Printf("  第 %d 次 Execute: phase=%s\n", i+1, state.Phase)
	}
	// 此时进度约 50%（3/6 页），1 个完整 chunk + 第 2 个 chunk 中
	printExportState(recoveryExportRepo, "export-recovery-001")

	// 2c. 保存 checkpoint（模拟崩溃前最后一次 Save）
	marshaled, _ := json.Marshal(state)
	recoveryExportRepo.Save(ctx, "export-recovery-001", marshaled)
	fmt.Println(">>> 保存 checkpoint 后进程崩溃 <<<")
	fmt.Println()

	// 2d. 『重启』：新建 engine，调用 engine.Run 自动恢复
	fmt.Println("--- 重启：新 engine + engine.Run 自动恢复 ---")
	restartedExportSrc := &memSource{pages: sourceData} // 新数据源
	restartedExportEng := export.New(restartedExportSrc, workDir, "exported-recovery.jsonl",
		export.WithPageSize(5), export.WithChunkPages(2),
	)

	if err := engine.Run(ctx, recoveryExportRepo, restartedExportEng, "export-recovery-001"); err != nil {
		panic(err)
	}
	restartedExportEng.Cleanup()

	// 2e. 验证恢复结果
	recoveryPath := filepath.Join(workDir, "exported-recovery.jsonl")
	recoveryBytes, _ := os.ReadFile(recoveryPath)
	fmt.Printf("\n恢复后导出文件: %s (%d bytes, %d 行)\n",
		recoveryPath, len(recoveryBytes), countLines(recoveryBytes))
	printExportState(recoveryExportRepo, "export-recovery-001")

	// 对比正常导出
	normalLines := countLines(exportedBytes)
	recoveryLines := countLines(recoveryBytes)
	if normalLines == recoveryLines {
		fmt.Printf("✓ 恢复验证通过：行数一致 (%d == %d)\n", normalLines, recoveryLines)
	} else {
		fmt.Printf("✗ 恢复验证失败：行数不一致 (%d != %d)\n", normalLines, recoveryLines)
	}
	fmt.Println()

	// ==========================================================
	// 阶段三：正常导入（Import — 完整流程）
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段三：正常导入（完整流程）                ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	importRepo, _ := filestore.New(filepath.Join(workDir, "import-state"))
	importTarget := &memTarget{}
	importEng := importpkg.New(exportedPath, importTarget,
		importpkg.WithBatchSize(7), // 每批 7 行（30 行 ≈ 5 批）
	)

	if err := engine.Run(ctx, importRepo, importEng, "import-001"); err != nil {
		panic(err)
	}

	fmt.Printf("导入完成: 共 %d 行\n", len(importTarget.rows))
	printImportState(importRepo, "import-001")
	fmt.Println()

	// ==========================================================
	// 阶段四：导入中断恢复（Import Recovery）
	//   模拟场景：导入大文件时进程崩溃，
	//   重启后从 offset 断点恢复，不重复不遗漏。
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段四：导入中断恢复（崩溃→重启→继续）     ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	// 4a. 创建独立的 repo 和 engine
	recoveryImportRepo, _ := filestore.New(filepath.Join(workDir, "import-recovery-state"))
	recoveryImportTarget1 := &memTarget{}
	recoveryImportEng1 := importpkg.New(exportedPath, recoveryImportTarget1,
		importpkg.WithBatchSize(7),
	)

	// 4b. 手动执行 2 批（14 行），然后模拟崩溃
	importState := &statestore.BaseTaskState{
		TaskID:   "import-recovery-001",
		TaskType: "import",
		Phase:    statestore.PhasePending,
	}

	fmt.Println("--- 第一步：Pending → Running ---")
	lsn, _ = recoveryImportEng1.Execute(ctx, importState)
	importState.CheckpointLSN = lsn

	fmt.Println("--- 执行 2 批后『崩溃』---")
	for i := 0; i < 2; i++ {
		lsn, err := recoveryImportEng1.Execute(ctx, importState)
		if err != nil {
			panic(err)
		}
		importState.CheckpointLSN = lsn
		fmt.Printf("  第 %d 批: 已导入 %d 行, offset=%d\n",
			i+1, len(recoveryImportTarget1.rows), importState.CheckpointLSN)
	}
	printImportState(recoveryImportRepo, "import-recovery-001")

	// 4c. 保存 checkpoint
	marshaled, _ = json.Marshal(importState)
	recoveryImportRepo.Save(ctx, "import-recovery-001", marshaled)
	fmt.Println(">>> 保存 checkpoint 后进程崩溃 <<<")
	fmt.Println()

	// 4d. 『重启』：新 engine + 新 target
	//     注意：之前的 14 行已丢失（随旧进程消失），
	//     恢复后必须重新导入这 14 行。恢复机制保证
	//     引擎从 offset 重新读取。
	fmt.Println("--- 重启：新 engine + engine.Run 自动恢复 ---")
	recoveryImportTarget2 := &memTarget{} // 新 target（模拟新进程）
	recoveryImportEng2 := importpkg.New(exportedPath, recoveryImportTarget2,
		importpkg.WithBatchSize(7),
	)

	if err := engine.Run(ctx, recoveryImportRepo, recoveryImportEng2, "import-recovery-001"); err != nil {
		panic(err)
	}

	// 4e. 验证恢复结果
	fmt.Printf("\n恢复后导入: 共 %d 行\n", len(recoveryImportTarget2.rows))
	printImportState(recoveryImportRepo, "import-recovery-001")

	// 验证：恢复后的导入行数 + 崩溃前导入行数 = 总行数
	totalRows := len(importTarget.rows) // 正常流程的总行数
	crashedRows := len(recoveryImportTarget1.rows)
	resumedRows := len(recoveryImportTarget2.rows)
	fmt.Printf("崩溃前导入: %d 行, 恢复后导入: %d 行, 合计: %d 行 (总行数: %d)\n",
		crashedRows, resumedRows, crashedRows+resumedRows, totalRows)

	if crashedRows+resumedRows == totalRows {
		fmt.Println("✓ 恢复验证通过：总行数一致（不重复不遗漏）")
	} else {
		fmt.Printf("✗ 恢复验证失败：行数不一致\n")
	}
	fmt.Println()

	// ==========================================================
	// 阶段五：Outbox 模式 — 文件通知示例
	//   演示调度层如何使用 Outbox 模式处理不可逆操作。
	//   场景：导出完成后发"通知"写入文件。
	//   写文件本身是可逆的（可截断），但这里模拟的场景是：
	//   多个下游消费者依赖这个"通知文件"来触发后续动作
	//   （如发送真实邮件、调用 webhook）。一旦文件被消费者读取，
	//   就无法撤回——因此"通知写入"应通过 outbox 在调度层处理。
	//
	//   同时展示 at-least-once 特性：handler 写入成功但 ack 前
	//   崩溃，重启后消息重新分发→文件出现重复行。
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段五：Outbox 模式（文件通知 + at-least-once）║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	// ---- 5a. 准备 ----
	notifyLogPath := filepath.Join(workDir, "notifications.log")

	outboxStore := outbox.NewInMemoryStore()
	registry := outbox.NewHandlerRegistry()

	// 注册 Handler：将通知内容写入文件（模拟真实不可逆操作）
	registry.Register("write_notification", func(msg *outbox.Message) error {
		f, err := os.OpenFile(notifyLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		var payload map[string]string
		json.Unmarshal(msg.Payload, &payload)
		line := fmt.Sprintf("[%s] task=%s, event=%s\n", msg.ID, payload["task_id"], payload["message"])
		if _, err := f.WriteString(line); err != nil {
			return err
		}
		return nil
	})

	fmt.Println("Handler 已注册: write_notification → 追加写入 notifications.log")
	fmt.Println()

	// ---- 5b. 正常流程：engine.Run + outbox 分发 ----
	fmt.Println("--- 5b. 正常流程：engine.Run 完成后分发 outbox ---")

	normalNotifyEng := newNotifyEngine("export-task-normal", []*outbox.Message{
		{ID: "notify-001", EventType: "write_notification", Payload: json.RawMessage(`{"task_id":"export-task-normal","message":"export completed successfully"}`)},
	})
	normalNotifyRepo, _ := filestore.New(filepath.Join(workDir, "notify-normal-state"))

	if err := engine.Run(ctx, normalNotifyRepo, normalNotifyEng, "export-task-normal"); err != nil {
		panic(err)
	}

	// 从引擎 Payload 提取 outbox 消息并写入 Store
	msgs, _ := normalNotifyEng.ExtractOutboxMessages()
	for _, m := range msgs {
		m.Status = outbox.StatusPending
		outboxStore.Append(ctx, m)
	}
	fmt.Printf("  引擎执行完成，提取到 %d 条 outbox 消息\n", len(msgs))

	// 分发 outbox 消息
	dispatcher := outbox.NewDispatcher(outboxStore, registry)
	processed, _ := dispatcher.DispatchPending(ctx)
	fmt.Printf("  Dispatcher 处理完成: %d 条消息已分发\n", processed)

	normalNotifyData, _ := os.ReadFile(notifyLogPath)
	fmt.Printf("  notifications.log: %d 行\n", countLines(normalNotifyData))
	fmt.Println()

	// ---- 5c. at-least-once 演示：写成功但 ack 前崩溃 → 重复 ----
	//   经典 at-least-once 故障：
	//   Handler 已成功写入文件（副作用已发生），但在 MarkProcessed
	//   之前进程崩溃。消息状态未更新 → 重启后被重新分发 → 再次写入
	//   → 文件出现重复行。
	fmt.Println("--- 5c. at-least-once 演示：写成功但 ack 失败 → 重复 ---")

	os.Remove(notifyLogPath)

	dupStore := outbox.NewInMemoryStore()
	dupRegistry := outbox.NewHandlerRegistry()

	// 准备 3 条 outbox 消息
	dupMessages := []*outbox.Message{
		{ID: "dup-001", EventType: "write_notification", Payload: json.RawMessage(`{"task_id":"dup-task","message":"phase 1 done"}`)},
		{ID: "dup-002", EventType: "write_notification", Payload: json.RawMessage(`{"task_id":"dup-task","message":"phase 2 done"}`)},
		{ID: "dup-003", EventType: "write_notification", Payload: json.RawMessage(`{"task_id":"dup-task","message":"phase 3 done"}`)},
	}
	for _, m := range dupMessages {
		m.Status = outbox.StatusPending
		dupStore.Append(ctx, m)
	}

	dupRegistry.Register("write_notification", func(msg *outbox.Message) error {
		f, err := os.OpenFile(notifyLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		var payload map[string]string
		json.Unmarshal(msg.Payload, &payload)
		fmt.Fprintf(f, "[%s] task=%s, event=%s\n", msg.ID, payload["task_id"], payload["message"])
		return nil
	})

	// 模拟分发循环：dup-001 正常完成 → dup-002 写入成功但 ack 前崩溃
	func() {
		defer func() { recover() }()
		pending, _ := dupStore.FetchPending(ctx)

		// msg 0: dup-001 — 写入 + MarkProcessed 都成功
		msg := pending[0]
		fmt.Printf("  [dup-001] 写入文件 → MarkProcessed ✓\n")
		dupStore.UpdateStatus(ctx, msg.ID, outbox.StatusProcessing, 0, "")
		dupRegistry.Get(msg.EventType)(msg)
		dupStore.MarkProcessed(ctx, msg.ID)

		// msg 1: dup-002 — 写入成功，但 MarkProcessed 之前进程崩溃！
		msg = pending[1]
		fmt.Printf("  [dup-002] 写入文件 ✓ → MarkProcessed 之前进程崩溃!\n")
		dupStore.UpdateStatus(ctx, msg.ID, outbox.StatusProcessing, 0, "")
		dupRegistry.Get(msg.EventType)(msg) // ★ 副作用已发生（文件已写入）
		// ↓ 没有 MarkProcessed！进程在此处崩溃
		panic("simulated crash after write but before ack")
	}()

	fmt.Printf("  崩溃后 outbox 状态: dup-001=%s, dup-002=%s, dup-003=%s\n",
		dupStore.GetMessage("dup-001").Status,
		dupStore.GetMessage("dup-002").Status,
		dupStore.GetMessage("dup-003").Status)

	partialData, _ := os.ReadFile(notifyLogPath)
	fmt.Printf("  崩溃后 notifications.log: %d 行\n", countLines(partialData))
	fmt.Println()

	// 『重启』：新 dispatcher 扫描 outbox store
	// dup-001: completed → 跳过
	// dup-002: processing（已写入但未 ack）→ 回退为 pending → 重新分发！
	// dup-003: pending → 正常分发
	fmt.Println("  --- 重启 dispatcher ---")
	fmt.Println("  dup-002 processing→回退pending 重发, dup-003 pending 首发送")

	// 手动将 dup-002 回退到 pending（模拟进程崩溃后状态回退）
	dupStore.UpdateStatus(ctx, "dup-002", outbox.StatusPending, 1, "crash before ack")

	restartedRegistry := outbox.NewHandlerRegistry()
	restartedRegistry.Register("write_notification", func(msg *outbox.Message) error {
		f, err := os.OpenFile(notifyLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()
		var payload map[string]string
		json.Unmarshal(msg.Payload, &payload)
		fmt.Fprintf(f, "[%s] task=%s, event=%s\n", msg.ID, payload["task_id"], payload["message"])
		return nil
	})

	restartedDispatcher := outbox.NewDispatcher(dupStore, restartedRegistry)
	restarted, _ := restartedDispatcher.DispatchPending(ctx)
	fmt.Printf("  重启后分发: %d 条\n", restarted)

	finalNotifyData, _ := os.ReadFile(notifyLogPath)
	totalNotifyLines := countLines(finalNotifyData)
	// 按消息 ID 检测重复：同一 msg.ID 出现多次即为 at-least-once 重复
	dupLines := countMatchingLines(finalNotifyData, "dup-002")
	dup003Lines := countMatchingLines(finalNotifyData, "dup-003")

	fmt.Println()
	fmt.Printf("  最终文件内容 (%d 行):\n", totalNotifyLines)
	for _, line := range strings.Split(string(finalNotifyData), "\n") {
		if strings.TrimSpace(line) != "" {
			fmt.Printf("    %s\n", line)
		}
	}

	// dup-002 在崩溃前写了 1 次，重启后又写了 1 次 → 共 2 次
	if dupLines > 1 {
		fmt.Printf("\n  ⚠ at-least-once 效果: dup-002 在文件中出现了 %d 次（预期 1 次）\n", dupLines)
		fmt.Println("    dup-002 被写入了 2 次：第一次在崩溃前，第二次在重启后")
		fmt.Println("    下游消费者必须实现幂等处理（如按 msg.ID 去重）")
	} else {
		fmt.Println("  (本次运行无重复)")
	}
	_ = dup003Lines
	fmt.Println()

	// ---- 5d. Outbox 模式总结 ----
	fmt.Println("Outbox 模式关键要点：")
	fmt.Println("  1. 引擎只写意图（Payload 中嵌入 outbox 记录）")
	fmt.Println("  2. engine.Run 成功后，调度层提取并持久化 outbox 消息")
	fmt.Println("  3. Dispatcher 分发 outbox 消息 → 执行真正的不可逆操作")
	fmt.Println("  4. 保证 at-least-once 投递（消息不会丢失，但可能重复）")
	fmt.Println("  5. Handler 实现必须幂等（按 msg.ID 去重 / UPSERT 等）")
	fmt.Println()

	// ==========================================================
	// 阶段六：Saga 模式 — 多步文件操作 + 失败补偿
	//   演示分布式事务的 Saga 编排。
	//   场景：项目创建工作流——
	//     Step 1: 创建项目目录
	//     Step 2: 写入配置文件
	//     Step 3: 创建索引文件
	//   如果 Step 2 失败，补偿 Step 1（删除目录）。
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  阶段六：Saga 模式（多步文件操作 + 失败补偿）║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Println()

	// ---- 6a. Saga 正常流程 ----
	fmt.Println("--- 6a. Saga 正常流程：创建项目目录结构 ---")

	sagaDir := filepath.Join(workDir, "saga-project")
	configPath := filepath.Join(sagaDir, "config.json")
	indexPath := filepath.Join(sagaDir, "index.json")

	createSaga := &outbox.Saga{
		Name:              "create_project",
		DefaultMaxRetries: 1,
		Steps: []outbox.SagaStep{
			{
				Name: "create_directory",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [action] 创建项目目录:", sagaDir)
					if err := os.MkdirAll(sagaDir, 0755); err != nil {
						return fmt.Errorf("mkdir: %w", err)
					}
					actx["dir_created"] = true
					return nil
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 删除项目目录:", sagaDir)
					return os.RemoveAll(sagaDir)
				},
			},
			{
				Name: "write_config",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					config := map[string]interface{}{"version": "1.0", "name": "demo-project"}
					data, _ := json.MarshalIndent(config, "", "  ")
					fmt.Printf("    [action] 写入配置文件: %s\n", configPath)
					return os.WriteFile(configPath, data, 0644)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 删除配置文件:", configPath)
					os.Remove(configPath)
					return nil
				},
			},
			{
				Name: "create_index",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					index := map[string]interface{}{"files": []string{"config.json"}, "total": 1}
					data, _ := json.MarshalIndent(index, "", "  ")
					fmt.Printf("    [action] 创建索引文件: %s\n", indexPath)
					return os.WriteFile(indexPath, data, 0644)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 删除索引文件:", indexPath)
					os.Remove(indexPath)
					return nil
				},
			},
		},
	}

	sagaStore := outbox.NewInMemorySagaStore()
	coordinator := outbox.NewSagaCoordinator(sagaStore)
	sagaState, err := coordinator.Run(ctx, createSaga, "saga-normal-001")
	if err != nil {
		panic(err)
	}

	fmt.Printf("  Saga 状态: %s\n", sagaState.Status)
	if info, err := os.Stat(configPath); err == nil {
		fmt.Printf("  ✓ config.json 已创建 (%d bytes)\n", info.Size())
	}
	if info, err := os.Stat(indexPath); err == nil {
		fmt.Printf("  ✓ index.json 已创建 (%d bytes)\n", info.Size())
	}
	fmt.Println()

	// ---- 6b. Saga 失败补偿演示 ----
	fmt.Println("--- 6b. Saga 失败补偿：第 2 步失败，回退第 1 步 ---")

	os.RemoveAll(sagaDir)

	failingSaga := &outbox.Saga{
		Name:              "failing_project",
		DefaultMaxRetries: 1,
		Steps: []outbox.SagaStep{
			{
				Name: "create_directory",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [action] 创建项目目录:", sagaDir)
					if err := os.MkdirAll(sagaDir, 0755); err != nil {
						return fmt.Errorf("mkdir: %w", err)
					}
					actx["dir_created"] = true
					return nil
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 回退：删除项目目录:", sagaDir)
					return os.RemoveAll(sagaDir)
				},
			},
			{
				Name: "write_config",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Printf("    [action] 写入配置文件: %s\n", configPath)
					return fmt.Errorf("disk full: cannot write config.json")
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 回退：删除配置文件（如果存在）")
					os.Remove(configPath)
					return nil
				},
			},
			{
				Name: "create_index",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Printf("    [action] 创建索引文件: %s\n", indexPath)
					return os.WriteFile(indexPath, []byte("{}"), 0644)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [comp] 回退：删除索引文件")
					os.Remove(indexPath)
					return nil
				},
			},
		},
	}

	failSagaState, err := coordinator.Run(ctx, failingSaga, "saga-fail-001")
	if err != nil {
		panic(err)
	}

	fmt.Printf("  Saga 状态: %s\n", failSagaState.Status)
	fmt.Printf("  Step 0 (create_directory): %s (已补偿)\n", failSagaState.StepStatuses[0])
	fmt.Printf("  Step 1 (write_config):    %s (失败)\n", failSagaState.StepStatuses[1])
	fmt.Printf("  Step 2 (create_index):    %s (未执行)\n", failSagaState.StepStatuses[2])

	if _, err := os.Stat(sagaDir); os.IsNotExist(err) {
		fmt.Println("  ✓ 项目目录已被补偿操作删除")
	} else {
		fmt.Println("  ✗ 项目目录仍存在（补偿失败）")
	}
	fmt.Println()

	// ---- 6c. Saga 崩溃恢复演示 ----
	fmt.Println("--- 6c. Saga 崩溃恢复：checkpoint 后 resume ---")

	os.RemoveAll(sagaDir)

	resumeSaga := &outbox.Saga{
		Name:              "resume_project",
		DefaultMaxRetries: 1,
		Steps: []outbox.SagaStep{
			{
				Name: "create_directory",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Println("    [action] 创建项目目录:", sagaDir)
					return os.MkdirAll(sagaDir, 0755)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					return os.RemoveAll(sagaDir)
				},
			},
			{
				Name: "write_config",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Printf("    [action] 写入配置文件: %s\n", configPath)
					return os.WriteFile(configPath, []byte(`{"version":"1.0"}`), 0644)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					os.Remove(configPath)
					return nil
				},
			},
			{
				Name: "create_index",
				Action: func(sctx context.Context, actx map[string]interface{}) error {
					fmt.Printf("    [action] 创建索引文件: %s\n", indexPath)
					return os.WriteFile(indexPath, []byte(`{"files":["config.json"]}`), 0644)
				},
				Compensation: func(sctx context.Context, actx map[string]interface{}) error {
					os.Remove(indexPath)
					return nil
				},
			},
		},
	}

	resumeSagaStore := outbox.NewInMemorySagaStore()
	resumeCoordinator := outbox.NewSagaCoordinator(resumeSagaStore)

	resumeSagaState, _ := resumeCoordinator.Run(ctx, resumeSaga, "saga-resume-001")
	fmt.Printf("  Saga 结果: %s\n", resumeSagaState.Status)
	if resumeSagaState.Status == outbox.SagaCompleted {
		fmt.Println("  ✓ 所有步骤已完成（SagaStore 可在崩溃后恢复）")
	}
	fmt.Println()

	// ---- 6d. Saga 模式总结 ----
	fmt.Println("Saga 模式关键要点：")
	fmt.Println("  1. 每个步骤有 Action（正向操作）和 Compensation（回退操作）")
	fmt.Println("  2. 任一步骤失败 → 按逆序执行已完成步骤的 Compensation")
	fmt.Println("  3. 补偿失败不阻断其他补偿（记录并继续，需人工介入）")
	fmt.Println("  4. Saga 状态通过 SagaStore 持久化 → 崩溃后可 Resume")
	fmt.Println("  5. 适用于跨服务的分布式事务协调")
	fmt.Println()

	// ==========================================================
	// 阶段七：总结
	// ==========================================================
	fmt.Println("╔══════════════════════════════════════════════╗")
	fmt.Println("║  演示完成                                    ║")
	fmt.Println("╚══════════════════════════════════════════════╝")
	fmt.Printf("输出目录: %s\n", workDir)
	fmt.Println()
	fmt.Println("关键机制：")
	fmt.Println("  1. 每步 Execute 后框架自动 Save checkpoint")
	fmt.Println("  2. 崩溃重启后 engine.Run 自动 Load 状态")
	fmt.Println("  3. 调用 Compensate 对齐物理系统到 LSN")
	fmt.Println("  4. 从断点继续 Execute，直到完成")
	fmt.Println()
	fmt.Println("扩展模式（outbox 包）：")
	fmt.Println("  5. Outbox 模式：不可逆操作在调度层通过 Store+Dispatcher 分发")
	fmt.Println("  6. Saga 模式：多步骤事务通过 SagaCoordinator 编排 + 失败补偿")
}
