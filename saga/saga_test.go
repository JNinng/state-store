package saga

import (
	"context"
	"errors"
	"testing"
)

// ============================================================
// Saga 测试
// ============================================================

func TestSagaCoordinator_NormalFlow(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var stepOrder []string

	saga := &Saga{
		Name: "test_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_forward")
					actx["step1_result"] = "done"
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_compensate")
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_forward")
					if actx["step1_result"] != "done" {
						t.Error("step_2 should have access to step_1 result")
					}
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_compensate")
					return nil
				},
			},
			{
				Name: "step_3",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_forward")
					return nil
				},
				Compensation: nil, // 幂等步骤，无需补偿
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if len(stepOrder) != 3 {
		t.Errorf("stepOrder length = %d, want 3", len(stepOrder))
	}
	if stepOrder[0] != "1_forward" || stepOrder[1] != "2_forward" || stepOrder[2] != "3_forward" {
		t.Errorf("unexpected order: %v", stepOrder)
	}

	// 验证没有步骤被补偿
	for i, ss := range state.StepStatuses {
		if ss != StepCompleted {
			t.Errorf("step %d status = %q, want completed", i, ss)
		}
	}
}

func TestSagaCoordinator_CompensationOnFailure(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var stepOrder []string

	saga := &Saga{
		Name: "compensate_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_forward")
					actx["balance"] = 100
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "1_compensate")
					// 退款
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_forward")
					return errors.New("step_2 failed: insufficient inventory")
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "2_compensate")
					return nil
				},
			},
			{
				Name: "step_3",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_forward")
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					stepOrder = append(stepOrder, "3_compensate")
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-comp-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaFailed {
		t.Errorf("status = %q, want failed", state.Status)
	}

	// 应该执行：1_forward → 2_forward(fail) → 1_compensate（逆序）
	expected := []string{"1_forward", "2_forward", "1_compensate"}
	if len(stepOrder) != 3 {
		t.Fatalf("stepOrder length = %d, want 3: %v", len(stepOrder), stepOrder)
	}
	for i, want := range expected {
		if stepOrder[i] != want {
			t.Errorf("stepOrder[%d] = %q, want %q", i, stepOrder[i], want)
		}
	}

	// step_1 应被补偿
	if state.StepStatuses[0] != StepCompensated {
		t.Errorf("step_0 status = %q, want compensated", state.StepStatuses[0])
	}
	// step_2 应标记失败（未补偿，因为它本身失败了）
	if state.StepStatuses[1] != StepFailed {
		t.Errorf("step_1 status = %q, want failed", state.StepStatuses[1])
	}
	// step_3 仍为 pending（从未执行）
	if state.StepStatuses[2] != StepPending {
		t.Errorf("step_2 status = %q, want pending", state.StepStatuses[2])
	}
}

func TestSagaCoordinator_RetryOnFailure(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	var attempts int
	saga := &Saga{
		Name:              "retry_saga",
		DefaultMaxRetries: 2,
		Steps: []SagaStep{
			{
				Name:       "flaky_step",
				MaxRetries: 3,
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					attempts++
					if attempts < 3 {
						return errors.New("temporary error")
					}
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "saga-retry-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if attempts != 3 {
		t.Errorf("attempts = %d, want 3", attempts)
	}
}

func TestSagaCoordinator_ResumeCompletedSaga(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx := context.Background()

	saga := &Saga{
		Name: "idempotent_saga",
		Steps: []SagaStep{
			{
				Name: "only_step",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	// 先完整执行
	_, err := coordinator.Run(ctx, saga, "resume-test")
	if err != nil {
		t.Fatalf("first Run: %v", err)
	}

	// Resume 已完成 saga——应直接返回完成状态
	state, err := coordinator.Resume(ctx, saga, "resume-test")
	if err != nil {
		t.Fatalf("Resume: %v", err)
	}
	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
}

func TestSagaCoordinator_ContextCancellation(t *testing.T) {
	store := NewInMemorySagaStore()
	coordinator := NewSagaCoordinator(store)
	ctx, cancel := context.WithCancel(context.Background())

	saga := &Saga{
		Name: "cancel_saga",
		Steps: []SagaStep{
			{
				Name: "step_1",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					cancel() // 第一步就取消 ctx
					return nil
				},
			},
			{
				Name: "step_2",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	state, err := coordinator.Run(ctx, saga, "cancel-001")
	if err == nil {
		// 上下文取消后，继续执行会被拦截
		t.Log("context cancelled after step 1")
	}
	_ = state
}

func TestRunSaga_Convenience(t *testing.T) {
	ctx := context.Background()
	saga := &Saga{
		Name: "convenience",
		Steps: []SagaStep{
			{
				Name: "simple",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					return nil
				},
			},
		},
	}

	state, err := RunSaga(ctx, saga, "conv-001")
	if err != nil {
		t.Fatalf("RunSaga: %v", err)
	}
	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
}

func TestInMemorySagaStore(t *testing.T) {
	store := NewInMemorySagaStore()
	ctx := context.Background()

	// Load 不存在的 saga
	state, err := store.Load(ctx, "nonexistent")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if state != nil {
		t.Error("Load should return nil for non-existent saga")
	}

	// Save
	original := &SagaState{
		SagaID:   "test",
		SagaName: "test_saga",
		Status:   SagaRunning,
		ActionCtx: map[string]interface{}{
			"key": "value",
		},
	}
	if err := store.Save(ctx, original); err != nil {
		t.Fatalf("Save: %v", err)
	}

	// 修改原始——不影响已保存的
	original.ActionCtx["key"] = "modified"

	// Load 验证隔离
	loaded, err := store.Load(ctx, "test")
	if err != nil {
		t.Fatalf("Load: %v", err)
	}
	if loaded.ActionCtx["key"] != "value" {
		t.Errorf("ActionCtx not isolated: %v", loaded.ActionCtx)
	}

	// Delete
	if err := store.Delete(ctx, "test"); err != nil {
		t.Fatalf("Delete: %v", err)
	}
	after, _ := store.Load(ctx, "test")
	if after != nil {
		t.Error("after Delete, Load should return nil")
	}
}

// TestSchedulingLayer_SagaPattern 演示调度层如何使用 Saga 模式。
//
// 场景：订单结账流程——扣减库存 → 扣款 → 发确认邮件。
// 如果中间某步失败，需补偿已完成的步骤。
func TestSchedulingLayer_SagaPattern(t *testing.T) {
	ctx := context.Background()

	// 模拟外部系统状态
	inventory := map[string]int{"item-1": 10}
	balance := 100
	var emailsSent []string

	saga := &Saga{
		Name:              "order_checkout",
		DefaultMaxRetries: 1,
		Steps: []SagaStep{
			{
				Name: "reserve_inventory",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					itemID := "item-1"
					if inventory[itemID] <= 0 {
						return errors.New("out of stock")
					}
					inventory[itemID]--
					actx["reserved_item"] = itemID
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					// 释放库存
					itemID := actx["reserved_item"].(string)
					inventory[itemID]++
					return nil
				},
			},
			{
				Name: "deduct_balance",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					amount := 50
					if balance < amount {
						return errors.New("insufficient balance")
					}
					balance -= amount
					actx["deducted_amount"] = amount
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					// 退款
					amount := actx["deducted_amount"].(int)
					balance += amount
					return nil
				},
			},
			{
				Name: "send_confirmation",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					// 发邮件是不可逆操作，但 Saga 中它放在最后一步
					// （因为前面步骤都可补偿）
					emailsSent = append(emailsSent, "user@example.com")
					return nil
				},
				Compensation: nil, // 邮件一旦发出无法补偿（可另发"取消通知"）
			},
		},
	}

	// 正常执行
	coordinator := NewSagaCoordinator(NewInMemorySagaStore())
	state, err := coordinator.Run(ctx, saga, "checkout-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaCompleted {
		t.Errorf("status = %q, want completed", state.Status)
	}
	if inventory["item-1"] != 9 {
		t.Errorf("inventory = %d, want 9", inventory["item-1"])
	}
	if balance != 50 {
		t.Errorf("balance = %d, want 50", balance)
	}
	if len(emailsSent) != 1 {
		t.Errorf("emails = %d, want 1", len(emailsSent))
	}
}

// TestSchedulingLayer_SagaCompensationInAction 演示 Saga 失败时的补偿行为。
func TestSchedulingLayer_SagaCompensationInAction(t *testing.T) {
	ctx := context.Background()

	inventory := map[string]int{"item-1": 10}
	balance := 0 // 余额不足，将在第 2 步失败

	saga := &Saga{
		Name: "failed_checkout",
		Steps: []SagaStep{
			{
				Name: "reserve_inventory",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					inventory["item-1"]--
					actx["reserved_item"] = "item-1"
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					inventory["item-1"]++
					return nil
				},
			},
			{
				Name: "deduct_balance",
				Action: func(ctx context.Context, actx map[string]interface{}) error {
					if balance < 50 {
						return errors.New("insufficient balance")
					}
					balance -= 50
					return nil
				},
				Compensation: func(ctx context.Context, actx map[string]interface{}) error {
					balance += 50
					return nil
				},
			},
		},
	}

	coordinator := NewSagaCoordinator(NewInMemorySagaStore())
	state, err := coordinator.Run(ctx, saga, "failed-checkout-001")
	if err != nil {
		t.Fatalf("Run: %v", err)
	}

	if state.Status != SagaFailed {
		t.Errorf("status = %q, want failed", state.Status)
	}

	// 库存应该恢复（补偿生效）
	if inventory["item-1"] != 10 {
		t.Errorf("inventory = %d, want 10 (compensation should restore)", inventory["item-1"])
	}

	// step_0 应被补偿
	if state.StepStatuses[0] != StepCompensated {
		t.Errorf("step_0 = %q, want compensated", state.StepStatuses[0])
	}
	// step_1 应标记失败
	if state.StepStatuses[1] != StepFailed {
		t.Errorf("step_1 = %q, want failed", state.StepStatuses[1])
	}
}

// 确保 InMemorySagaStore 实现了 SagaStore 接口
var _ SagaStore = (*InMemorySagaStore)(nil)
