// Package main 演示使用 WithRowMarshaler 将数据导出为 CSV 文件。
//
// 运行: go run ./cmd/examples/csv-export/
//
// JSONL（默认）适用于机器间数据交换，CSV 适用于人工查看 / Excel 打开。
// 通过自定义 RowMarshaler，export.Engine 可以输出任意文本格式。
//
// 要点:
//   - RowMarshaler 负责将 phys.Row 转为一行 CSV 文本（引擎会自动追加 \n）
//   - 使用 encoding/csv 处理字段转义（引号、逗号、换行符等）
//   - CSV 表头在导出完成后写入（因引擎按行序列化，无法提前插入表头）
//
// 生产注意: 如果需要对 CSV 文件做崩溃恢复，表头字节数需计入 LSN 偏移。

package main

import (
	"bytes"
	"context"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"state-store/filestore"
	"state-store/phys"
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

// csvField 将 phys.Row 的值转为 CSV 安全的字符串。
func csvField(v interface{}) string {
	if v == nil {
		return ""
	}
	return fmt.Sprint(v)
}

// ---- 主流程 ----

func main() {
	ctx := context.Background()

	// 200 条订单，pageSize=10 → 20 页, chunkPages=5 → 4 个 chunk
	pages := makeOrders(200, 10)

	workDir := filepath.Join(".", "example-output", "csv-export")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// ---- CSV 列定义 —— 数据的 Schema 契约 ----
	columns := []string{"order_id", "user_id", "amount", "status", "remark"}

	// ---- 构建导出引擎 ----
	repo, _ := filestore.New(filepath.Join(workDir, "state"))

	eng := export.New(
		&orderDB{pages},
		workDir,
		"orders.csv",
		export.WithPageSize(10),
		export.WithChunkPages(5),

		// ★ 核心: 用 RowMarshaler 将 Row → CSV 文本行
		export.WithRowMarshaler(func(row phys.Row) ([]byte, error) {
			var buf bytes.Buffer
			w := csv.NewWriter(&buf)
			w.UseCRLF = false // 引擎会补 \n，避免重复换行

			// 按列定义顺序取值，保证列对齐
			fields := make([]string, len(columns))
			for i, col := range columns {
				fields[i] = csvField(row[col])
			}
			w.Write(fields)
			w.Flush()
			if err := w.Error(); err != nil {
				return nil, err
			}

			// csv.Writer.Write 会在末尾追加 \n，去掉它（引擎会加）
			return bytes.TrimRight(buf.Bytes(), "\n"), nil
		}),
	)

	// ---- 导出数据行 ----
	fmt.Println("=== CSV 导出: 200 条订单 → orders.csv ===")
	fmt.Println()

	if err := task.Run(ctx, repo, eng, "task-csv-001"); err != nil {
		fmt.Fprintf(os.Stderr, "导出失败: %v\n", err)
		os.Exit(1)
	}
	eng.Cleanup()

	// ---- 写入 CSV 表头 ----
	// 引擎按行序列化，表头独立于数据行，因此在导出完成后插入文件头部。
	// 如果数据量极大，可改为：先写 header → 引擎以 append 模式追加数据。
	csvPath := filepath.Join(workDir, "orders.csv")
	data, _ := os.ReadFile(csvPath)
	headerLine := strings.Join(columns, ",") + "\n"
	os.WriteFile(csvPath, append([]byte(headerLine), data...), 0644)

	// ---- 展示结果 ----
	content, _ := os.ReadFile(csvPath)
	lines := strings.Split(string(content), "\n")
	dataLines := 0
	for _, l := range lines {
		if strings.TrimSpace(l) != "" {
			dataLines++
		}
	}

	fmt.Printf("输出文件: %s\n", csvPath)
	fmt.Printf("文件大小: %d bytes\n", len(content))
	fmt.Printf("行数: 1 表头 + %d 数据行\n\n", dataLines-1)

	fmt.Println("前 6 行内容:")
	for i := 0; i < 6 && i < len(lines); i++ {
		if strings.TrimSpace(lines[i]) != "" {
			fmt.Printf("  %s\n", lines[i])
		}
	}
	fmt.Println("  ...")
	fmt.Println()

	// ---- 对比 JSONL 大小 ----
	jsonlPath := filepath.Join(workDir, "orders.jsonl")
	jsonlEng := export.New(&orderDB{pages}, workDir, "orders.jsonl",
		export.WithPageSize(10), export.WithChunkPages(5))
	task.Run(ctx, repo, jsonlEng, "task-jsonl-001")
	jsonlEng.Cleanup()

	jsonlData, _ := os.ReadFile(jsonlPath)
	csvData, _ := os.ReadFile(csvPath)
	fmt.Printf("格式对比: JSONL %d bytes  →  CSV %d bytes (节省 %d%%)\n",
		len(jsonlData), len(csvData),
		(len(jsonlData)-len(csvData))*100/len(jsonlData))
	fmt.Println()

	fmt.Println("WithRowMarshaler 关键点:")
	fmt.Println("  返回的 []byte 是一行文本（不含换行），引擎自动追加 \\n")
	fmt.Println("  使用 encoding/csv.Writer 保证逗号、引号、换行正确转义")
	fmt.Println("  列顺序在 RowMarshaler 闭包中定义，可与 phys.Row key 解耦")
	fmt.Println("  CSV 表头独立于数据行，需在导出后单独写入")
}

// ---- 辅助: 构造模拟数据 ----

// makeOrders 构造模拟订单数据。
// 最后一个 "remark" 字段包含逗号和换行，用于验证 CSV 转义。
func makeOrders(total, pageSize int) [][]phys.Row {
	numPages := total / pageSize
	pages := make([][]phys.Row, numPages)
	statuses := []string{"pending", "paid", "shipped", "cancelled"}

	id := 0
	for p := 0; p < numPages; p++ {
		pages[p] = make([]phys.Row, pageSize)
		for i := 0; i < pageSize; i++ {
			id++
			remark := ""
			// 故意在某些行插入包含逗号和引号的数据，验证 CSV 转义
			if id%50 == 0 {
				remark = "special, with \"comma\""
			}
			pages[p][i] = phys.Row{
				"order_id": float64(id),
				"user_id":  float64(1000 + id%50),
				"amount":   float64(9900+id*100) / 100.0,
				"status":   statuses[id%len(statuses)],
				"remark":   remark,
			}
		}
	}
	return pages
}
