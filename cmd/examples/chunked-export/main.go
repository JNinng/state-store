// Package main 演示 export.Engine 的分批分片导出与合并机制。
//
// 运行: go run ./cmd/examples/chunked-export/
//
// 导出引擎内部流程:
//   1. running 阶段: FetchPage 逐页获取数据，按 chunkPages 数攒批写 .chunk_N.tmp
//   2. io.EOF → merging 阶段: 按序拼接所有 .chunk_N.tmp 为最终文件
//   3. Cleanup(): 删除中间 .tmp 文件
//
// 本示例刻意在 Cleanup 前展示工作目录，让你看到中间分片文件。
// 同时演示分片中途崩溃时 Compensate 如何将正在写的 chunk 截断对齐。

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
	"state-store/task"
	"state-store/task/export"
)

// ---- 实现 phys.DataSource ----

// orderDB 模拟订单数据库的分页查询。
type orderDB struct {
	pages [][]phys.Row
}

func (db *orderDB) FetchPage(_ context.Context, page, pageSize int) ([]phys.Row, error) {
	if page >= len(db.pages) {
		return nil, io.EOF
	}
	return db.pages[page], nil
}

// panicSource 包装 phys.DataSource，在指定次数的 FetchPage 调用后触发 panic。
type panicSource struct {
	inner     phys.DataSource
	crashPage int
	callCount *int
}

func (s *panicSource) FetchPage(ctx context.Context, page, pageSize int) ([]phys.Row, error) {
	*s.callCount++
	if s.crashPage > 0 && *s.callCount == s.crashPage {
		panic(fmt.Sprintf("进程在第 %d 次分页请求时意外终止（模拟）", *s.callCount))
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

// listDir 列出目录中的文件，按名称排序。
func listDir(dir string) []os.DirEntry {
	entries, _ := os.ReadDir(dir)
	sort.Slice(entries, func(i, j int) bool {
		return entries[i].Name() < entries[j].Name()
	})
	return entries
}

// printPayload 打印导出引擎 Payload 的关键字段。
func printPayload(repo *filestore.FileRepository, taskID string) {
	raw, _ := repo.Load(context.Background(), taskID)
	if raw == nil {
		return
	}
	var state statestore.BaseTaskState
	json.Unmarshal(raw, &state)
	var p export.Payload
	json.Unmarshal(state.Payload, &p)
	fmt.Printf("  phase=%-10s  page=%-3d  chunk=%-3d  totalChunks=%-3d  lsn=%d\n",
		state.Phase, p.CurrentPage, p.CurrentChunkIdx, p.TotalChunks, state.CheckpointLSN)
}

func main() {
	ctx := context.Background()

	// 500 条订单, pageSize=10 → 50 页, chunkPages=5 → 10 个 chunk
	totalRows := 500
	pageSize := 10
	chunkPages := 5
	pages := makeOrders(totalRows, pageSize)

	workDir := filepath.Join(".", "example-output", "chunked-export")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// ================================================================
	// 场景一: 正常分片导出，展示中间 chunk 文件 + Cleanup
	// ================================================================
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║  分批分片导出: 500 行 × 10行/页 × 5页/chunk = 10 chunk   ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()

	repo, err := filestore.New(filepath.Join(workDir, "state"))
	if err != nil {
		fmt.Fprintf(os.Stderr, "创建 repo 失败: %v\n", err)
		os.Exit(1)
	}

	eng := export.New(
		&orderDB{pages},
		workDir,
		"orders.jsonl",
		export.WithPageSize(pageSize),
		export.WithChunkPages(chunkPages),
	)

	// engine.Run 内部流程:
	//   running 阶段 — FetchPage → 行写入当前 chunk → 攒够 chunkPages 后 flush chunk_N.tmp
	//   io.EOF → 转 merging 阶段 — 按序拼接所有 .chunk_N.tmp → orders.jsonl
	if err := task.Run(ctx, repo, eng, "task-chunked-001"); err != nil {
		fmt.Fprintf(os.Stderr, "导出失败: %v\n", err)
		os.Exit(1)
	}

	// 导出完成后不立即 Cleanup — 先查看中间 chunk 文件
	fmt.Println("导出后的工作目录（Cleanup 前，chunk 文件均在）:")
	fmt.Println("  文件名                                  大小      说明")
	fmt.Println("  ──────────────────────────────────────────────────────────")
	for _, f := range listDir(workDir) {
		info, _ := f.Info()
		kind := ""
		switch {
		case strings.HasSuffix(f.Name(), ".tmp"):
			kind = "← 分片文件"
		case f.Name() == "orders.jsonl":
			kind = "← 合并后最终输出"
		case strings.HasSuffix(f.Name(), ".state"):
			kind = "← checkpoint 状态"
		}
		fmt.Printf("  %-35s %7d   %s\n", f.Name(), info.Size(), kind)
	}

	var tmpCount int
	var tmpSize int64
	for _, f := range listDir(workDir) {
		if strings.HasSuffix(f.Name(), ".tmp") {
			tmpCount++
			info, _ := f.Info()
			tmpSize += info.Size()
		}
	}
	fmt.Printf("\n  中间文件: %d 个 .tmp, 合计 %d bytes\n", tmpCount, tmpSize)

	finalPath := filepath.Join(workDir, "orders.jsonl")
	finalData, _ := os.ReadFile(finalPath)
	finalLines := strings.Count(string(finalData), "\n")
	fmt.Printf("  最终文件: orders.jsonl = %d bytes, %d 行\n", len(finalData), finalLines)
	printPayload(repo, "task-chunked-001")
	fmt.Println()

	// ----- 清理 chunk 文件 -----
	fmt.Println("调用 eng.Cleanup() → 删除所有中间 .chunk_N.tmp ...")
	eng.Cleanup()

	fmt.Println("\n清理后的工作目录:")
	for _, f := range listDir(workDir) {
		info, _ := f.Info()
		fmt.Printf("  %-35s %7d\n", f.Name(), info.Size())
	}
	fmt.Println()

	// ================================================================
	// 场景二: 分片中途崩溃 → Compensate 截断 → 恢复后继续
	//
	// 使用 panicSource 在第 12 次 FetchPage 时触发 panic。
	// chunkPages=5 → 页 0-4 为 chunk_0, 页 5-9 为 chunk_1,
	// 页 10-14 为 chunk_2 → 第 12 次 FetchPage 正在写 chunk_2。
	// Compensate 将 chunk_2 截断到 checkpoint LSN，恢复后重写。
	// ================================================================
	fmt.Println("╔══════════════════════════════════════════════════════════╗")
	fmt.Println("║  崩溃恢复: 第 3 个 chunk 写入中途崩溃，resume 后继续     ║")
	fmt.Println("╚══════════════════════════════════════════════════════════╝")
	fmt.Println()

	recoveryDir := filepath.Join(workDir, "recovery")
	os.MkdirAll(recoveryDir, 0755)
	recoveryRepo, _ := filestore.New(filepath.Join(recoveryDir, "state"))

	crashPage := 12
	fmt.Printf("panicSource 设置在第 %d 次 FetchPage 时 panic\n", crashPage)
	fmt.Println("（chunk_0 = 页 0-4, chunk_1 = 页 5-9, chunk_2 = 页 10-14 → 崩溃在 chunk_2）")
	fmt.Println()

	// 第一次运行: panic → recover 捕获
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("!!! 捕获 panic: %v\n", r)
				fmt.Println("    前 11 次 FetchPage 的 checkpoint 已被框架自动保存")
				fmt.Println("    Compensate 将 chunk_2 截断到 checkpoint LSN")
				fmt.Println()
			}
		}()

		crashSrc := &panicSource{
			inner:     &orderDB{pages},
			crashPage: crashPage,
			callCount: new(int),
		}
		eng := export.New(crashSrc, recoveryDir, "orders.jsonl",
			export.WithPageSize(pageSize), export.WithChunkPages(chunkPages))
		_ = task.Run(ctx, recoveryRepo, eng, "task-chunked-002")
	}()

	// 崩溃后查看工作目录
	fmt.Println("崩溃后工作目录中的文件:")
	for _, f := range listDir(recoveryDir) {
		info, _ := f.Info()
		fmt.Printf("  %-35s %7d\n", f.Name(), info.Size())
	}
	fmt.Println("  ↑ chunk_0.tmp, chunk_1.tmp 完整，无 chunk_2.tmp（被截断或未 flush）")
	fmt.Println()

	// 第二次运行: 进程重启，恢复
	fmt.Println("进程重启，创建新 engine ...")
	restartEng := export.New(&orderDB{pages}, recoveryDir, "orders.jsonl",
		export.WithPageSize(pageSize), export.WithChunkPages(chunkPages))

	if err := task.Run(ctx, recoveryRepo, restartEng, "task-chunked-002"); err != nil {
		fmt.Fprintf(os.Stderr, "恢复失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Println("恢复完成后的工作目录（所有 chunk 已重新生成并合并）:")
	for _, f := range listDir(recoveryDir) {
		info, _ := f.Info()
		kind := ""
		if strings.HasSuffix(f.Name(), ".tmp") {
			kind = "← chunk"
		} else if f.Name() == "orders.jsonl" {
			kind = "← 最终文件"
		}
		fmt.Printf("  %-35s %7d   %s\n", f.Name(), info.Size(), kind)
	}
	restartEng.Cleanup()

	finalData, _ = os.ReadFile(filepath.Join(recoveryDir, "orders.jsonl"))
	finalLines = strings.Count(string(finalData), "\n")
	fmt.Printf("\n恢复后最终文件: %d bytes, %d 行\n\n", len(finalData), finalLines)

	// ================================================================
	// 流程总结
	// ================================================================
	fmt.Println("───────────────────────────────────────────────────────")
	fmt.Println("分批分片导出合并流程总结:")
	fmt.Println()
	fmt.Println("  ┌─ running（分批分片）──────────────────────────────┐")
	fmt.Println("  │ FetchPage(0) → 写 chunk_0.tmp                     │")
	fmt.Println("  │ FetchPage(1) → 写 chunk_0.tmp                     │")
	fmt.Println("  │ FetchPage(4) → chunk_0 写满，flush 并关闭         │")
	fmt.Println("  │ FetchPage(5) → 写 chunk_1.tmp（新 chunk）          │")
	fmt.Println("  │ ...                                               │")
	fmt.Println("  │ FetchPage(49) → io.EOF → 转 merging               │")
	fmt.Println("  └──────────────────────────────────────────────────┘")
	fmt.Println("  ┌─ merging（合并）─────────────────────────────────┐")
	fmt.Println("  │ 按序拼接: chunk_0 + chunk_1 + ... + chunk_9       │")
	fmt.Println("  │ → orders.jsonl                                   │")
	fmt.Println("  └──────────────────────────────────────────────────┘")
	fmt.Println("  ┌─ 崩溃恢复 ───────────────────────────────────────┐")
	fmt.Println("  │ chunk_2 写入中途崩溃 → Compensate 截断 chunk_2    │")
	fmt.Println("  │ 重启 → 从 break 的 page 重新 Fetch → 重写 chunk_2  │")
	fmt.Println("  │ chunk_3..9 正常写完 → merging → 最终文件正确      │")
	fmt.Println("  └──────────────────────────────────────────────────┘")
	fmt.Println("  Cleanup() → 删除所有 .chunk_*.tmp")
}
