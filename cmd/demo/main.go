package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"

	"state-store/engine"
	"state-store/engine/export"
	importpkg "state-store/engine/import"
	"state-store/filestore"
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
	// 阶段五：总结
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
}
