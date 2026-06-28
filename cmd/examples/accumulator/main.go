// Package main 演示如何通过嵌入 outbox.Accumulator 在自定义引擎中
// 增量生产 outbox 消息，实现生产级的"边执行边产生事件"模式。
//
// 运行: go run ./cmd/examples/accumulator/
//
// 场景: 报表生成引擎——分页生成报表，每完成一页产出一条进度通知，
// 最后产生一条汇总通知。所有消息在 checkpoint 保护下逐步积累。
package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	"state-store/filestore"
	"state-store/statestore"
	"state-store/task"
	"state-store/task/outbox"
	outboxfilestore "state-store/task/outbox/filestore"
)

// ---- 自定义引擎: 嵌入 Accumulator 实现增量消息生产 ----

// reportEngine 分页生成报表，通过嵌入 outbox.Accumulator 在每步
// Execute 中增量追加 outbox 消息。消息随 Payload 被框架自动
// checkpoint，崩溃后可通过 UnmarshalPayload 恢复。
type reportEngine struct {
	outbox.Accumulator // ← 嵌入: 获得 Add / MarshalPayload / UnmarshalPayload
	pages              [][]string
	currentPage        int
}

func newReportEngine() *reportEngine {
	return &reportEngine{
		pages: [][]string{
			{"北京: 1200 万", "上海: 2400 万", "广州: 1800 万"},
			{"深圳: 1700 万", "杭州: 1200 万", "成都: 2100 万"},
			{"武汉: 1100 万", "南京: 900 万", "重庆: 3200 万"},
		},
	}
}

func (e *reportEngine) TaskType() string { return "report_generator" }

func (e *reportEngine) Execute(_ context.Context, state *statestore.BaseTaskState) (int64, error) {
	switch state.Phase {
	case statestore.PhasePending:
		fmt.Println("  [phase] pending → running: 开始生成报表")
		state.Phase = statestore.PhaseRunning
		// 恢复时从 Payload 加载已累计的消息
		if e.Len() == 0 && len(state.Payload) > 0 {
			e.UnmarshalPayload(state.Payload)
			fmt.Printf("  [recover] 从 Payload 恢复了 %d 条消息\n", e.Len())
		}
		return 0, nil

	case statestore.PhaseRunning:
		if e.currentPage >= len(e.pages) {
			state.Phase = statestore.PhaseMerging
			fmt.Println("  [phase] running → merging: 所有页面生成完毕")
			return int64(e.currentPage), nil
		}

		page := e.pages[e.currentPage]
		e.currentPage++

		// 模拟生成一页报表
		fmt.Printf("  [generate] 第 %d 页: %s\n", e.currentPage, strings.Join(page, ", "))

		// ★ 增量追加 outbox 消息——不替换已有消息
		e.Add(&outbox.Message{
			ID:        fmt.Sprintf("progress-page-%d", e.currentPage),
			EventType: "report_progress",
			Payload: json.RawMessage(fmt.Sprintf(
				`{"page":%d,"rows":%d,"msg":"第 %d 页生成完成"}`,
				e.currentPage, len(page), e.currentPage)),
		})

		// ★ 将累积的消息写入 Payload（框架在 Execute 返回后自动 checkpoint）
		state.Payload = e.MarshalPayload()
		state.Message = fmt.Sprintf("已生成 %d/%d 页", e.currentPage, len(e.pages))

		fmt.Printf("  [outbox] 累计 %d 条消息已写入 Payload\n", e.Len())
		return int64(e.currentPage), nil

	case statestore.PhaseMerging:
		// 模拟合并所有页面为最终报表
		fmt.Println("  [merge] 合并所有页面为最终报表...")
		totalRows := 0
		for _, page := range e.pages {
			totalRows += len(page)
		}

		// 最后追加一条完成通知
		e.Add(&outbox.Message{
			ID:        "report-complete",
			EventType: "report_completed",
			Payload: json.RawMessage(fmt.Sprintf(
				`{"pages":%d,"total_rows":%d,"msg":"报表生成完毕"}`,
				len(e.pages), totalRows)),
		})

		state.Payload = e.MarshalPayload()
		state.Phase = statestore.PhaseCompleted
		state.Message = fmt.Sprintf("报表完成: %d 页, %d 行", len(e.pages), totalRows)
		fmt.Printf("  [outbox] 累计 %d 条消息（含完成通知）\n", e.Len())
		return int64(e.currentPage + 1), nil

	default:
		return state.CheckpointLSN, nil
	}
}

func (e *reportEngine) Compensate(_ context.Context, targetLSN int64) error {
	// 将 currentPage 回退到目标 LSN 对应页
	e.currentPage = int(targetLSN)
	fmt.Printf("  [compensate] 回退到页面 %d\n", e.currentPage)
	return nil
}

func (e *reportEngine) Progress(state statestore.BaseTaskState) int {
	if state.Phase == statestore.PhaseCompleted {
		return 100
	}
	return e.currentPage * 100 / (len(e.pages) + 1) // +1 for merging
}

var _ task.Engine = (*reportEngine)(nil)

// ---- main ----

func main() {
	ctx := context.Background()
	workDir := filepath.Join(".", "example-output", "accumulator")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	fmt.Println("=== Accumulator 模式: 嵌入 engine 增量生产 outbox 消息 ===")
	fmt.Println()

	// ---- 1. 准备调度层 ----
	notifyLogPath := filepath.Join(workDir, "notifications.log")

	registry := outbox.NewHandlerRegistry()
	registry.Register("report_progress", func(msg *outbox.Message) error {
		var p struct {
			Page int    `json:"page"`
			Msg  string `json:"msg"`
		}
		json.Unmarshal(msg.Payload, &p)
		writeLog(notifyLogPath, "[进度] msg=%s page=%d msg=%s", msg.ID, p.Page, p.Msg)
		return nil
	})
	registry.Register("report_completed", func(msg *outbox.Message) error {
		var p struct {
			Pages     int    `json:"pages"`
			TotalRows int    `json:"total_rows"`
			Msg       string `json:"msg"`
		}
		json.Unmarshal(msg.Payload, &p)
		writeLog(notifyLogPath, "[完成] msg=%s pages=%d rows=%d msg=%s",
			msg.ID, p.Pages, p.TotalRows, p.Msg)
		return nil
	})
	fmt.Println("Handler 已注册: report_progress, report_completed")

	// ---- 2. 运行引擎 ----
	// FileStore: 持久化 outbox 消息（崩溃安全）
	repo, _ := filestore.New(filepath.Join(workDir, "state"))
	outboxStore, _ := outboxfilestore.New(filepath.Join(workDir, "outbox"))

	eng := newReportEngine()

	fmt.Println("\n--- 执行引擎 ---")
	finalState, err := outbox.RunWithOutbox(ctx, repo, eng, "report-001", outboxStore)
	if err != nil {
		fmt.Fprintf(os.Stderr, "RunWithOutbox: %v\n", err)
		os.Exit(1)
	}
	fmt.Printf("\n最终状态: phase=%s message=%s progress=%d\n",
		finalState.Phase, finalState.Message, finalState.Progress)

	// 展示 Payload 中的消息（从 Accumulator 角度）
	fmt.Printf("Accumulator 共收集 %d 条消息:\n", eng.Len())
	for _, m := range eng.Messages() {
		fmt.Printf("  [%s] type=%s\n", m.ID, m.EventType)
	}

	// 展示 FileStore 持久化文件
	fmt.Println("\nFileStore 文件:")
	entries, _ := os.ReadDir(filepath.Join(workDir, "outbox"))
	for _, e := range entries {
		fmt.Printf("  %s\n", e.Name())
	}

	// ---- 3. 后台分发 ----
	fmt.Println("\n--- 后台 Dispatcher 持续分发 ---")
	dispatcher := outbox.NewDispatcher(outboxStore, registry,
		outbox.WithPollInterval(100*time.Millisecond))

	// 立即分发一次（批量）
	dispatcher.DispatchPending(ctx)

	// 读取通知日志
	data, _ := os.ReadFile(notifyLogPath)
	fmt.Print(string(data))

	// ---- 关键要点 ----
	fmt.Println("---")
	fmt.Println("Accumulator 关键要点:")
	fmt.Println("  嵌入 outbox.Accumulator → 无需手动管理消息列表")
	fmt.Println("  Execute 中增量 Add → MarshalPayload → 框架自动 checkpoint")
	fmt.Println("  UnmarshalPayload → 崩溃恢复时从 Payload 重建内存状态")
	fmt.Println("  RunWithOutbox → 终端一次性提取所有消息到 FileStore")
	fmt.Println("  Dispatcher.Start → 后台持续轮询，实时分发")
	fmt.Println("  多个 engine 可共享同一个 OutboxStore → 生产者-消费者解耦")
}

func writeLog(path string, format string, args ...interface{}) {
	line := fmt.Sprintf(format, args...) + "\n"
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if f != nil {
		defer f.Close()
		fmt.Fprint(f, line)
	}
}
