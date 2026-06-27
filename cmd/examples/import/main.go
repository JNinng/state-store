// Package main 演示如何使用 import.Engine 将 JSONL 文件导入到数据目标。
//
// 运行: go run ./cmd/examples/import/
//
// 本示例模拟"将 JSONL 文件批量导入用户数据库"的场景。
//
// 你需要实现的接口:
//   - phys.DataTarget: 提供批量写入能力 (WriteBatch)
//
// 关键约束:
//   - WriteBatch 必须实现幂等（如 UPSERT），因为崩溃恢复会重放最后一个批次
//
// 框架自动处理:
//   - 文件读取指针追踪（byte offset）
//   - checkpoint 保存与恢复
//   - 崩溃后的 Compensate（源文件完整性检查）
//
// 涵盖两个场景:
//   1. 正常导入: 150 条用户记录完整导入
//   2. 崩溃恢复: 导入中途进程 panic，recover 后创建新 engine 从断点继续

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"state-store/engine"
	importpkg "state-store/engine/import"
	"state-store/filestore"
	"state-store/phys"
	"state-store/statestore"
)

// ---- 实现 phys.DataTarget（你的数据层） ----

// userDB 模拟用户数据库。
// WriteBatch 使用 user_id 做主键去重，实现幂等写入。
// 实际项目中替换为 INSERT ... ON DUPLICATE KEY UPDATE 或 INSERT ... ON CONFLICT。
type userDB struct {
	users map[float64]phys.Row // user_id → row
}

func (db *userDB) WriteBatch(_ context.Context, rows []phys.Row) (int64, error) {
	for _, row := range rows {
		id := row["user_id"].(float64)
		db.users[id] = row // 相同 user_id 覆盖 → 幂等
	}
	return int64(len(rows)), nil
}

// crashableTarget 包装 phys.DataTarget，在指定次数的 WriteBatch 调用后触发 panic。
// 用来模拟进程在导入过程中意外崩溃。
type crashableTarget struct {
	inner      phys.DataTarget
	crashBatch int // 第 N 次 WriteBatch 调用时 panic（0 = 不崩溃）
	callCount  int
}

func (t *crashableTarget) WriteBatch(ctx context.Context, rows []phys.Row) (int64, error) {
	t.callCount++
	if t.crashBatch > 0 && t.callCount == t.crashBatch {
		panic(fmt.Sprintf("进程在第 %d 次批量写入时意外终止（模拟）", t.callCount))
	}
	return t.inner.WriteBatch(ctx, rows)
}

// ---- 辅助函数 ----

// generateUsersFile 生成 JSONL 源文件。
func generateUsersFile(path string, count int) {
	f, err := os.Create(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	cities := []string{"Beijing", "Shanghai", "Guangzhou", "Shenzhen", "Hangzhou", "Chengdu"}
	for i := 1; i <= count; i++ {
		row := phys.Row{
			"user_id": float64(i),
			"name":    fmt.Sprintf("user_%d", i),
			"email":   fmt.Sprintf("user_%d@example.com", i),
			"city":    cities[i%len(cities)],
		}
		data, _ := json.Marshal(row)
		f.Write(data)
		f.Write([]byte("\n"))
	}
}

// printImportState 打印导入任务的 checkpoint 状态。
func printImportState(repo *filestore.FileRepository, taskID string) {
	raw, _ := repo.Load(context.Background(), taskID)
	if raw == nil {
		return
	}
	var state statestore.BaseTaskState
	json.Unmarshal(raw, &state)
	var p importpkg.Payload
	json.Unmarshal(state.Payload, &p)
	fmt.Printf("  checkpoint: phase=%s  progress=%d%%  offset=%d  batch=%d  inserted=%d\n",
		state.Phase, state.Progress, p.CurrentReadOffset, p.CurrentBatchIdx, p.InsertedRows)
}

func main() {
	ctx := context.Background()

	workDir := filepath.Join(".", "example-output", "import")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// 生成 JSONL 源文件
	srcPath := filepath.Join(workDir, "users.jsonl")
	generateUsersFile(srcPath, 150)
	fmt.Printf("源文件: %s\n", srcPath)
	fmt.Println()

	// ================================================================
	// 场景一: 正常导入
	// ================================================================
	fmt.Println("=== 正常导入: users.jsonl (150 行) → 用户数据库 ===")
	fmt.Println()

	repo, _ := filestore.New(filepath.Join(workDir, "state"))

	target := &userDB{users: make(map[float64]phys.Row)}
	eng := importpkg.New(
		srcPath,                     // JSONL 源文件路径
		target,                      // 你的数据目标
		importpkg.WithBatchSize(20), // 每批写入 20 行
	)

	if err := engine.Run(ctx, repo, eng, "task-import-001"); err != nil {
		fmt.Fprintf(os.Stderr, "导入失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("导入完成: %d 个用户已入库\n", len(target.users))
	printImportState(repo, "task-import-001")
	fmt.Println()

	// ================================================================
	// 场景二: 崩溃恢复
	//
	// 导入大文件时（GB 级），进程可能因各种原因意外终止。
	// 这里用 crashableTarget 在第 4 次 WriteBatch 时触发 panic
	// 来模拟，然后立即 recover 并创建新 engine 继续。
	//
	// 框架保证:
	//   1. 每批写入后自动 Save checkpoint（记录 byte offset）
	//   2. crashableTarget 崩溃时前 3 批已被完整写入并保存
	//   3. 重启后 Compensate 验证源文件完整性
	//   4. 从 checkpoint offset 继续读取 → 第 4 批被重放（幂等保证正确性）
	// ================================================================
	fmt.Println("=== 崩溃恢复: 导入到第 4 批时崩溃，recover 后重启 ===")
	fmt.Println()

	recoveryRepo, _ := filestore.New(filepath.Join(workDir, "import-recovery-state"))

	// 第一次运行: 使用 crashableTarget，第 4 次 WriteBatch 时 panic
	func() {
		defer func() {
			if r := recover(); r != nil {
				fmt.Printf("!!! 捕获 panic: %v\n", r)
				fmt.Println("    前 3 批的 checkpoint 已由框架自动保存")
				fmt.Println()
			}
		}()

		crashTarget := &crashableTarget{
			inner:      &userDB{users: make(map[float64]phys.Row)},
			crashBatch: 4, // 第 4 次批量写入时崩溃
		}
		eng := importpkg.New(srcPath, crashTarget, importpkg.WithBatchSize(20))

		// engine.Run 在每批写入后自动 Save checkpoint。
		// 崩溃时前 3 批的 offset 已被持久化。
		_ = engine.Run(ctx, recoveryRepo, eng, "task-import-002")
	}()

	// 第二次运行: recover 后"重启"，创建全新的 engine + DataTarget
	fmt.Println("进程重启，创建新 engine 和新 DataTarget...")

	target2 := &userDB{users: make(map[float64]phys.Row)}
	eng2 := importpkg.New(srcPath, target2, importpkg.WithBatchSize(20))

	// engine.Run 自动 Load checkpoint → Compensate 验证源文件 → 从断点继续
	if err := engine.Run(ctx, recoveryRepo, eng2, "task-import-002"); err != nil {
		fmt.Fprintf(os.Stderr, "恢复失败: %v\n", err)
		os.Exit(1)
	}

	fmt.Printf("恢复完成: %d 个用户已入库\n", len(target2.users))
	printImportState(recoveryRepo, "task-import-002")
	fmt.Println()

	fmt.Println("关键机制:")
	fmt.Println("  LSN = 源文件 byte offset，精确追踪读取位置")
	fmt.Println("  panic → recover → 新建 engine → engine.Run Load → Compensate → 继续")
	fmt.Println("  Compensate 验证源文件完整性（未被截断）")
	fmt.Println("  崩溃前最后一批会被重放 → DataTarget.WriteBatch 必须幂等")
	fmt.Println("  使用 bufio.Scanner（非 json.Decoder）避免内部缓冲语义差异")
}
