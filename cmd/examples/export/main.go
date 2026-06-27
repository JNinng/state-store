// Package main 演示如何使用 export.Engine 将分页数据导出为 JSONL 文件。
//
// 运行: go run ./cmd/examples/export/
//
// 本示例模拟"订单系统将数据库订单导出为 JSONL 文件"的场景。
//
// 你需要实现的接口:
//   - phys.DataSource: 提供分页查询能力 (FetchPage)
//
// 框架自动处理:
//   - checkpoint 保存与恢复
//   - chunk 分块写入与合并
//   - 崩溃后的 Compensate（文件截断对齐）
//
// 涵盖两个场景:
//   1. 正常导出: 200 条订单完整导出为 JSONL
//   2. 崩溃恢复: 导出中途进程 panic，recover 后创建新 engine 从断点继续

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
	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
)

// ---- 实现 phys.DataSource（你的数据层） ----

// orderDB 模拟订单数据库的分页查询。
// 实际项目中替换为你的 MySQL/PostgreSQL SELECT ... LIMIT ... OFFSET 逻辑。
type orderDB struct {
	pages [][]phys.Row
}

func (db *orderDB) FetchPage(_ context.Context, page, pageSize int) ([]phys.Row, error) {
	if page >= len(db.pages) {
		return nil, io.EOF // 没有更多数据 → 框架自动进入 merging 阶段
	}
	return db.pages[page], nil
}

// crashableSource 包装 phys.DataSource，在指定次数的 FetchPage 调用后触发 panic。
// 用来模拟进程在导出过程中意外崩溃。
type crashableSource struct {
	inner     phys.DataSource
	crashPage int // 第 N 次 FetchPage 调用时 panic（0 = 不崩溃）
	callCount int
}

func (s *crashableSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	s.callCount++
	if s.crashPage > 0 && s.callCount == s.crashPage {
		panic(fmt.Sprintf("进程在第 %d 次分页请求时意外终止（模拟）", s.callCount))
	}
	return s.inner.FetchPage(ctx, page, pageSize)
}

// ---- 辅助函数 ----

// makeOrders 构造模拟订单数据。
func makeOrders(total, pageSize int) [][]phys.Row {
	numPages := total / pageSize
	pages := make([][]phys.Row, numPages)
	statuses := []string{"pending", "paid", "shipped", "cancelled"}
	for p := 0; p < numPages; p++ {
		pages[p] = make([]phys.Row, pageSize)
		for i := 0; i < pageSize; i++ {
			id := p*pageSize + i + 1
			pages[p][i] = phys.Row{
				"order_id": float64(id),
				"user_id":  float64(1000 + id%50),
				"amount":   float64(9900+id*100) / 100.0,
				"status":   statuses[id%len(statuses)],
			}
		}
	}
	return pages
}

// printExportState 打印导出任务的 checkpoint 状态。
func printExportState(repo *filestore.FileRepository, taskID string) {
	raw, _ := repo.Load(context.Background(), taskID)
	if raw == nil {
		return
	}
	var state statestore.BaseTaskState
	json.Unmarshal(raw, &state)
	var p export.Payload
	json.Unmarshal(state.Payload, &p)
	fmt.Printf("  checkpoint: phase=%s  progress=%d%%  page=%d  chunk=%d/%d  lsn=%d\n",
		state.Phase, state.Progress, p.CurrentPage, p.CurrentChunkIdx, p.TotalChunks, state.CheckpointLSN)
}

func main() {
	ctx := context.Background()

	// 准备数据: 200 条订单，每页 10 条，共 20 页
	pages := makeOrders(200, 10)

	workDir := filepath.Join(".", "example-output", "export")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// ================================================================
	// 场景一: 正常导出
	// ================================================================
	fmt.Println("=== 正常导出: 200 条订单 → orders.jsonl ===")
	fmt.Println()

	repo, err := filestore.New(filepath.Join(workDir, "state"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 repo 失败: %v\n", err)
		os.Exit(1)
	}

	eng := export.New(
		&orderDB{pages},          // 你的数据源
		workDir,                  // 工作目录（存放中间 chunk 文件）
		"orders.jsonl",           // 最终输出文件名
		export.WithPageSize(10),  // 每页行数
		export.WithChunkPages(4), // 每 4 页写一个中间 chunk
	)

	// engine.Run 是核心循环:
	//   Load state → Compensate(if recovering) → Execute → Save → 循环
	if err := engine.Run(ctx, repo, eng, "task-export-001"); err != nil {
		fmt.Fprintf(os.Stderr, "导出失败: %v\n", err)
		os.Exit(1)
	}
	eng.Cleanup() // 删除中间 chunk 文件

	outputPath := filepath.Join(workDir, "orders.jsonl")
	data, _ := os.ReadFile(outputPath)
	fmt.Printf("输出文件: %s\n", outputPath)
	fmt.Printf("文件大小: %d bytes (共 200 行)\n", len(data))
	printExportState(repo, "task-export-001")
	fmt.Println()

	// ================================================================
	// 场景二: 崩溃恢复
	//
	// 导出大量数据时（百万级以上），进程可能因 OOM、机器重启等
	// 意外终止。这里用 crashableSource 在第 8 次分页请求时触发
	// panic 来模拟，然后立即 recover 并创建新 engine 继续。
	//
	// 框架保证:
	//   1. 崩溃前每步 Execute 后自动 Save checkpoint
	//   2. 重启后 engine.Run Load checkpoint → Compensate 对齐文件
	//   3. 从断点继续 Execute，最终文件不重复不遗漏
	// ================================================================
	fmt.Println("=== 崩溃恢复: 导出到第 8 页时崩溃，recover 后重启 ===")
	fmt.Println()

	recoveryDir := filepath.Join(workDir, "recovery")
	os.MkdirAll(recoveryDir, 0755)

	recoveryRepo, _ := filestore.New(filepath.Join(recoveryDir, "state"))

	// 第一次运行: 使用 crashableSource，第 8 次 FetchPage 时 panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("!!! 捕获 panic: %v\n", r)
				fmt.Println("    前 7 页的 checkpoint 已由框架自动保存")
				fmt.Println()
			}
		}()

		crashSrc := &crashableSource{
			inner:     &orderDB{pages},
			crashPage: 8, // 第 8 次分页请求时崩溃
		}
		eng := export.New(crashSrc, recoveryDir, "orders.jsonl",
			export.WithPageSize(10), export.WithChunkPages(4))

		// engine.Run 内部在每步 Execute 后自动 Save checkpoint。
		// 崩溃时前 7 页的状态已被持久化。
		_ = engine.Run(ctx, recoveryRepo, eng, "task-export-002")
	}()

	// 第二次运行: recover 后"重启"，创建全新的 engine 实例
	fmt.Println("进程重启，创建新 engine（使用正常数据源）...")

	restartEng := export.New(&orderDB{pages}, recoveryDir, "orders.jsonl",
		export.WithPageSize(10), export.WithChunkPages(4))

	// engine.Run 自动 Load checkpoint → Compensate 截断文件 → 从断点继续 Execute
	if err := engine.Run(ctx, recoveryRepo, restartEng, "task-export-002"); err != nil {
		fmt.Fprintf(os.Stderr, "恢复失败: %v\n", err)
		os.Exit(1)
	}
	restartEng.Cleanup()

	data, _ = os.ReadFile(filepath.Join(recoveryDir, "orders.jsonl"))
	fmt.Printf("恢复完成: 最终输出 %d bytes\n", len(data))
	printExportState(recoveryRepo, "task-export-002")
	fmt.Println()

	fmt.Println("关键机制:")
	fmt.Println("  Execute 每步返回 LSN，框架在循环内自动 Save checkpoint")
	fmt.Println("  panic → recover → 新建 engine → engine.Run Load → Compensate → 继续")
	fmt.Println("  Compensate 将文件截断到 checkpoint LSN，消除崩溃时的不完整写入")
	fmt.Println("  DataSource 无状态要求 —— 只需支持重复分页查询")
}
