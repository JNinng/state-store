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
	idx   int
}

func (s *memSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	if s.idx >= len(s.pages) {
		return nil, io.EOF
	}
	result := s.pages[s.idx]
	s.idx++
	return result, nil
}

// ============================================================
// 内存数据目标 — 实现 phys.DataTarget，供导入使用
// 导入的数据存入声明的 rows 切片中
// ============================================================

type memTarget struct {
	rows []phys.Row // ★ 声明的变量，导入结果存入这里
}

func (t *memTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.rows = append(t.rows, rows...)
	return int64(len(rows)), nil
}

func main() {
	ctx := context.Background()

	// ----------------------------------------------------------
	// 1. 准备测试数据（3 页，每页若干行）
	// ----------------------------------------------------------
	sourceData := [][]phys.Row{
		{
			{"id": float64(1), "name": "张三", "city": "北京"},
			{"id": float64(2), "name": "李四", "city": "上海"},
			{"id": float64(3), "name": "王五", "city": "广州"},
		},
		{
			{"id": float64(4), "name": "赵六", "city": "深圳"},
			{"id": float64(5), "name": "孙七", "city": "杭州"},
		},
		{
			{"id": float64(6), "name": "周八", "city": "成都"},
		},
	}

	// 工作目录设在当前运行目录下，运行前先清理，执行后保留以便查看
	workDir := filepath.Join(".", "demo-output")
	os.RemoveAll(workDir) // 运行前清理
	if err := os.MkdirAll(workDir, 0755); err != nil {
		panic(err)
	}

	// ==========================================================
	// 阶段一：导出（Export）
	// ==========================================================
	fmt.Println("========== 阶段一：导出数据 ==========")

	// 1a. 创建 StateRepository（文件持久化）
	exportRepo, err := filestore.New(filepath.Join(workDir, "export-state"))
	if err != nil {
		panic(err)
	}

	// 1b. 创建数据源和导出引擎
	exportSrc := &memSource{pages: sourceData}
	exportEng := export.New(exportSrc, workDir, "exported.jsonl",
		export.WithPageSize(3),   // 每页 3 行
		export.WithChunkPages(1), // 每 1 页一个分块（便于演示 checkpoint）
	)

	// 1c. 运行导出
	if err := engine.Run(ctx, exportRepo, exportEng, "export-001"); err != nil {
		panic(err)
	}

	// 1d. 清理临时分块文件
	exportEng.Cleanup()

	// 1e. 读取并打印导出文件内容
	exportedPath := filepath.Join(workDir, "exported.jsonl")
	exportedBytes, _ := os.ReadFile(exportedPath)
	fmt.Printf("导出文件: %s\n", exportedPath)
	fmt.Printf("文件大小: %d bytes\n\n", len(exportedBytes))
	fmt.Println("--- 导出的原始 JSONL 内容 ---")
	fmt.Println(string(exportedBytes))

	// 1f. 查询导出任务最终状态
	rawState, _ := exportRepo.Load(ctx, "export-001")
	var exportState statestore.BaseTaskState
	json.Unmarshal(rawState, &exportState)
	fmt.Printf("导出状态: phase=%s, progress=%d%%, lsn=%d\n\n",
		exportState.Phase, exportState.Progress, exportState.CheckpointLSN)

	// ==========================================================
	// 阶段二：导入（Import）—— 导入到声明的变量中
	// ==========================================================
	fmt.Println("========== 阶段二：导入数据 ==========")

	// 2a. 创建 StateRepository
	importRepo, err := filestore.New(filepath.Join(workDir, "import-state"))
	if err != nil {
		panic(err)
	}

	// 2b. ★ 声明变量：用于接收导入结果
	var importedRows []phys.Row
	importTarget := &memTarget{} // rows 字段即为我们声明的变量

	// 2c. 创建导入引擎
	importEng := importpkg.New(exportedPath, importTarget,
		importpkg.WithBatchSize(2), // 每批 2 行（便于演示 checkpoint）
	)

	// 2d. 运行导入
	if err := engine.Run(ctx, importRepo, importEng, "import-001"); err != nil {
		panic(err)
	}

	// 2e. ★ 将导入结果赋值给声明的变量
	importedRows = importTarget.rows

	// 2f. 查询导入任务最终状态
	rawState2, _ := importRepo.Load(ctx, "import-001")
	var importState statestore.BaseTaskState
	json.Unmarshal(rawState2, &importState)
	var importPayload importpkg.ImportPayload
	json.Unmarshal(importState.Payload, &importPayload)

	fmt.Printf("导入状态: phase=%s, progress=%d%%, lsn=%d\n",
		importState.Phase, importState.Progress, importState.CheckpointLSN)
	fmt.Printf("导入统计: 成功=%d 行, 失败=%d 行\n\n",
		importPayload.InsertedRows, importPayload.FailedRows)

	// ==========================================================
	// 阶段三：打印导入到变量中的数据
	// ==========================================================
	fmt.Println("========== 阶段三：打印导入结果 ==========")
	fmt.Printf("共导入 %d 行数据到声明的变量 importedRows 中:\n\n", len(importedRows))

	for i, row := range importedRows {
		pretty, _ := json.MarshalIndent(row, "", "  ")
		fmt.Printf("[第 %d 行] %s\n", i+1, string(pretty))
	}

	fmt.Println("\n========== 演示完成 ==========")
	fmt.Printf("输出目录（已保留，可手动查看）: %s\n", workDir)
}
