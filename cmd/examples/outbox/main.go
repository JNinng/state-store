// Package main 演示 Outbox 和 Saga 两种调度层模式。
//
// 运行: go run ./cmd/examples/outbox/
//
// task.Engine.Execute 有严格的副作用约束 —— 不可逆操作（发邮件、扣款、
// 发消息队列）不能在 Execute 中直接执行。outbox 包提供了调度层的解决方案。
//
// 本示例涵盖两个模式:
//
//  1. Outbox 模式: 引擎写"意图记录"到 Payload，调度层在 engine.Run 后
//     通过 Dispatcher 执行真正的不可逆操作（at-least-once 投递）。
//
//  2. Saga 模式: 多步骤分布式事务，每个步骤有正向 Action 和逆向
//     Compensation。失败时按逆序补偿已完成步骤。

package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"state-store/filestore"
	"state-store/saga"
	"state-store/task"
	"state-store/task/outbox"
)

// ---- Outbox 模式: 使用通用 outbox 引擎 ----

func main() {
	ctx := context.Background()

	workDir := filepath.Join(".", "example-output", "outbox")
	os.RemoveAll(workDir)
	os.MkdirAll(workDir, 0755)

	// ================================================================
	// 模式一: Outbox（不可逆操作的安全分发）
	//
	// 典型场景: 导出完成后需要发送邮件通知、调用 webhook、
	// 写入消息队列等。这些操作一旦执行就无法撤回。
	//
	// 模式:
	//   1. 引擎在 Payload 中记录 "我想发一封邮件"
	//   2. engine.Run 成功后，调度层提取并持久化到 OutboxStore
	//   3. Dispatcher 执行真正的发邮件操作
	//
	// 保证: at-least-once 投递（消息不会丢，但可能重复）
	//       因此 Handler 必须实现幂等
	// ================================================================
	fmt.Println("=== 模式一: Outbox — 引擎写意图，调度层执行 ===")
	fmt.Println()

	notifyLogPath := filepath.Join(workDir, "notifications.log")

	// 1. 注册 Handler — 处理具体的通知操作
	registry := outbox.NewHandlerRegistry()
	registry.Register("send_notification", func(msg *outbox.Message) error {
		// 模拟发送通知: 写入文件（实际项目替换为邮件/短信/webhook）
		f, err := os.OpenFile(notifyLogPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
		if err != nil {
			return err
		}
		defer f.Close()

		var payload map[string]string
		json.Unmarshal(msg.Payload, &payload)
		line := fmt.Sprintf("[%s] task=%s  event=%s\n",
			msg.ID, payload["task_id"], payload["message"])
		f.WriteString(line)
		return nil
	})
	fmt.Println("Handler 已注册: send_notification → notifications.log")

	// 2. 运行引擎 — 引擎负责可逆工作，只记录意图
	notifyEng := outbox.NewEngine("notify_demo", []*outbox.Message{
		{
			ID:        "msg-001",
			EventType: "send_notification",
			Payload:   json.RawMessage(`{"task_id":"task-export-normal","message":"export completed, 200 rows"}`),
		},
	})

	notifyRepo, _ := filestore.New(filepath.Join(workDir, "notify-state"))
	if err := task.Run(ctx, notifyRepo, notifyEng, "task-export-normal"); err != nil {
		fmt.Fprintf(os.Stderr, "引擎执行失败: %v\n", err)
		os.Exit(1)
	}
	fmt.Println("引擎执行完成（意图已记录在 Payload 中）")

	// 3. 提取 outbox 消息并写入 Store
	outboxStore := outbox.NewInMemoryStore()
	for _, m := range notifyEng.Messages() {
		m.Status = outbox.StatusPending
		outboxStore.Append(ctx, m)
	}
	fmt.Printf("提取到 %d 条 outbox 消息\n", len(notifyEng.Messages()))

	// 4. 分发 — 执行真正的不可逆操作
	dispatcher := outbox.NewDispatcher(outboxStore, registry)
	processed, _ := dispatcher.DispatchPending(ctx)
	fmt.Printf("Dispatcher 已分发 %d 条消息\n", processed)

	data, _ := os.ReadFile(notifyLogPath)
	fmt.Printf("\nnotifications.log 内容:\n%s", string(data))

	// at-least-once 说明
	fmt.Println("---")
	fmt.Println("Outbox 关键要点:")
	fmt.Println("  引擎 Execute 只写意图，不执行不可逆操作")
	fmt.Println("  engine.Run 成功后调度层提取并分发 Outbox 消息")
	fmt.Println("  Handler 崩溃重试 → at-least-once → Handler 必须幂等")
	fmt.Println("  幂等方式: 按 msg.ID 去重 / UPSERT / 唯一约束")
	fmt.Println()

	// ================================================================
	// 模式二: Saga（多步骤分布式事务）
	//
	// 典型场景: 创建项目 → 写配置文件 → 创建索引。
	// 如果"写配置文件"失败，需要回退"创建项目"（删除目录）。
	//
	// Saga 编排:
	//   1. 按顺序执行步骤的 Action
	//   2. 如果某步骤失败，逆序执行已完成步骤的 Compensation
	//   3. 补偿失败不阻断其他补偿（记录错误继续）
	//   4. Saga 状态可持久化 → 支持崩溃恢复
	// ================================================================
	fmt.Println("=== 模式二: Saga — 多步骤事务 + 失败补偿 ===")
	fmt.Println()

	sagaDir := filepath.Join(workDir, "saga-project")
	configPath := filepath.Join(sagaDir, "config.json")
	indexPath := filepath.Join(sagaDir, "index.json")

	// 场景 A: 正常流程
	fmt.Println("--- 正常流程: 三步全部成功 ---")

	createProjectSaga := &saga.Saga{
		Name:              "create_project",
		DefaultMaxRetries: 1,
		Steps: []saga.SagaStep{
			{
				Name: "create_directory",
				Action: func(_ context.Context, actx map[string]interface{}) error {
					fmt.Println("  [action] 创建项目目录:", sagaDir)
					return os.MkdirAll(sagaDir, 0755)
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  删除项目目录:", sagaDir)
					return os.RemoveAll(sagaDir)
				},
			},
			{
				Name: "write_config",
				Action: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [action] 写入配置文件:", configPath)
					return os.WriteFile(configPath, []byte(`{"version":"1.0"}`), 0644)
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  删除配置文件:", configPath)
					os.Remove(configPath)
					return nil
				},
			},
			{
				Name: "create_index",
				Action: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [action] 创建索引文件:", indexPath)
					return os.WriteFile(indexPath, []byte(`{"total":1}`), 0644)
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  删除索引文件:", indexPath)
					os.Remove(indexPath)
					return nil
				},
			},
		},
	}

	sagaStore := saga.NewInMemorySagaStore()
	coordinator := saga.NewSagaCoordinator(sagaStore)

	state, err := coordinator.Run(ctx, createProjectSaga, "saga-001")
	if err != nil {
		fmt.Fprintf(os.Stderr, "Saga 执行错误: %v\n", err)
	}
	fmt.Printf("结果: %s\n\n", state.Status)

	os.RemoveAll(sagaDir) // 清理，准备失败场景

	// 场景 B: 失败补偿
	fmt.Println("--- 失败补偿: 第 2 步失败 → 回退第 1 步 ---")

	failingSaga := &saga.Saga{
		Name:              "failing_project",
		DefaultMaxRetries: 1,
		Steps: []saga.SagaStep{
			{
				Name: "create_directory",
				Action: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [action] 创建项目目录:", sagaDir)
					return os.MkdirAll(sagaDir, 0755)
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  回退: 删除目录", sagaDir)
					return os.RemoveAll(sagaDir)
				},
			},
			{
				Name: "write_config",
				Action: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Printf("  [action] 写入配置文件: %s\n", configPath)
					return fmt.Errorf("disk full: cannot write config.json")
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  回退: 删除配置文件")
					os.Remove(configPath)
					return nil
				},
			},
			{
				Name: "create_index",
				Action: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [action] 创建索引文件（不会执行到这一步）")
					return nil
				},
				Compensation: func(_ context.Context, _ map[string]interface{}) error {
					fmt.Println("  [comp]  回退: 删除索引文件")
					return nil
				},
			},
		},
	}

	failState, _ := coordinator.Run(ctx, failingSaga, "saga-002")
	fmt.Printf("结果: %s\n", failState.Status)

	// 打印各步骤状态
	for i, ss := range failState.StepStatuses {
		fmt.Printf("  Step %d (%s): %s\n", i, failingSaga.Steps[i].Name, ss)
	}

	// 验证: 目录应该被补偿操作删除了
	if _, err := os.Stat(sagaDir); os.IsNotExist(err) {
		fmt.Println("  ✓ 目录已被补偿操作删除，系统回到初始状态")
	}
	fmt.Println()

	// 场景 C: 崩溃恢复
	fmt.Println("--- 崩溃恢复: SagaStore 持久化 + Resume ---")

	resumeSagaStore := saga.NewInMemorySagaStore()
	resumeCoordinator := saga.NewSagaCoordinator(resumeSagaStore)

	// 先完整执行一次 Saga
	resumeState, _ := resumeCoordinator.Run(ctx, createProjectSaga, "saga-resume-001")
	fmt.Printf("Saga 完成: %s\n", resumeState.Status)
	if _, err := os.Stat(configPath); err == nil {
		fmt.Println("  ✓ 项目文件已创建")
	}

	// 检查 SagaStore 中保存的状态
	savedState, _ := resumeSagaStore.Load(ctx, "saga-resume-001")
	if savedState != nil {
		fmt.Printf("  ✓ Saga 状态已持久化 (status=%s)\n", savedState.Status)
	}
	fmt.Println()

	fmt.Println("Saga 关键要点:")
	fmt.Println("  每个步骤有 Action（正向）和 Compensation（逆向回退）")
	fmt.Println("  步骤失败 → 逆序执行已完成步骤的 Compensation")
	fmt.Println("  补偿失败不阻断其他补偿（记录错误，继续执行）")
	fmt.Println("  Saga 状态通过 SagaStore 持久化 → 崩溃后可从 SagaStore Resume")
	fmt.Println("  适用于跨服务的分布式事务协调（每个步骤调用一个服务）")
}
