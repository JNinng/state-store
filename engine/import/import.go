package importpkg

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"

	"state-store/engine"
	"state-store/phys"
	"state-store/statestore"
)

// ImportPayload 是导入任务的业务扩展状态。
type ImportPayload struct {
	CurrentReadOffset int64 `json:"current_read_offset"`
	CurrentBatchIdx   int   `json:"current_batch_idx"`
	TotalBatches      int   `json:"total_batches"`
	InsertedRows      int64 `json:"inserted_rows"`
	FailedRows        int64 `json:"failed_rows"`
}

// ImportEngine 实现 engine.Engine 接口，从源文件读取并批量写入目标。
type ImportEngine struct {
	srcPath   string
	target    phys.DataTarget
	batchSize int
}

// ImportOption 是 ImportEngine 的配置函数。
type ImportOption func(*ImportEngine)

// WithBatchSize 设置每批次行数，默认 1000。
func WithBatchSize(n int) ImportOption {
	return func(e *ImportEngine) { e.batchSize = n }
}

// New 创建 ImportEngine。
func New(srcPath string, target phys.DataTarget, opts ...ImportOption) *ImportEngine {
	e := &ImportEngine{
		srcPath:   srcPath,
		target:    target,
		batchSize: 1000,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// 编译期检查
var _ engine.Engine = (*ImportEngine)(nil)

func (e *ImportEngine) TaskType() string { return "import" }

func (e *ImportEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	var p ImportPayload
	if len(state.Payload) > 0 {
		if err := json.Unmarshal(state.Payload, &p); err != nil {
			return 0, fmt.Errorf("import: unmarshal payload: %w", err)
		}
	}

	switch state.Phase {
	case statestore.PhasePending:
		p = ImportPayload{}
		state.Phase = statestore.PhaseRunning
		state.Message = "import started"
		state.Payload = e.marshalPayload(&p)
		return 0, nil

	case statestore.PhaseRunning:
		return e.executeRunning(ctx, state, &p)

	default:
		return state.CheckpointLSN, nil
	}
}

func (e *ImportEngine) executeRunning(ctx context.Context, state *statestore.BaseTaskState, p *ImportPayload) (int64, error) {
	f, err := os.Open(e.srcPath)
	if err != nil {
		return 0, fmt.Errorf("import: open source: %w", err)
	}
	defer f.Close()

	if _, err := f.Seek(p.CurrentReadOffset, io.SeekStart); err != nil {
		return 0, fmt.Errorf("import: seek to %d: %w", p.CurrentReadOffset, err)
	}

	// Use bufio.Scanner to read line-by-line and track byte offset precisely.
	// json.NewDecoder buffers internally, which makes offset tracking unreliable.
	scanner := bufio.NewScanner(f)
	var rows []phys.Row
	offset := p.CurrentReadOffset
	eof := false

	for i := 0; i < e.batchSize; i++ {
		if !scanner.Scan() {
			if err := scanner.Err(); err != nil {
				return 0, fmt.Errorf("import: scan at offset %d: %w", offset, err)
			}
			eof = true
			break
		}
		line := scanner.Bytes()
		// Advance offset past this line (including newline consumed by Scanner)
		offset += int64(len(line) + 1) // Scanner strips the newline, so +1

		var row phys.Row
		if err := json.Unmarshal(line, &row); err != nil {
			return 0, fmt.Errorf("import: decode row at offset %d: %w", offset, err)
		}
		rows = append(rows, row)
	}

	if len(rows) == 0 {
		state.Phase = statestore.PhaseCompleted
		state.Message = "import completed"
		return p.CurrentReadOffset, nil
	}

	inserted, err := e.target.WriteBatch(ctx, rows)
	if err != nil {
		return 0, fmt.Errorf("import: write batch: %w", err)
	}

	p.InsertedRows += inserted
	p.FailedRows += int64(len(rows)) - inserted
	p.CurrentBatchIdx++
	p.CurrentReadOffset = offset

	if eof {
		state.Phase = statestore.PhaseCompleted
		state.Message = "import completed"
	} else {
		state.Message = fmt.Sprintf("importing batch %d, %d rows inserted", p.CurrentBatchIdx, inserted)
	}
	state.Payload = e.marshalPayload(p)
	return p.CurrentReadOffset, nil
}

func (e *ImportEngine) Compensate(ctx context.Context, targetLSN int64) error {
	info, err := os.Stat(e.srcPath)
	if err != nil {
		return fmt.Errorf("import: stat source: %w", err)
	}
	if info.Size() < targetLSN {
		return fmt.Errorf("import: source file smaller than LSN (%d < %d): source file may have been truncated",
			info.Size(), targetLSN)
	}
	return nil
}

func (e *ImportEngine) Progress(state statestore.BaseTaskState) int {
	var p ImportPayload
	if len(state.Payload) > 0 {
		json.Unmarshal(state.Payload, &p)
	}
	info, err := os.Stat(e.srcPath)
	if err != nil || info.Size() == 0 {
		return 0
	}
	prog := int(p.CurrentReadOffset * 100 / info.Size())
	if prog > 100 {
		return 100
	}
	return prog
}

func (e *ImportEngine) marshalPayload(p *ImportPayload) json.RawMessage {
	data, _ := json.Marshal(p)
	return data
}
