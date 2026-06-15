package export

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"

	"state-store/engine"
	"state-store/phys"
	"state-store/statestore"
)

// ExportPayload 是导出任务的业务扩展状态。
type ExportPayload struct {
	CurrentPage      int   `json:"current_page"`
	CurrentChunkIdx  int   `json:"current_chunk_idx"`
	CurrentChunkSize int64 `json:"current_chunk_size"`
	TotalChunks      int   `json:"total_chunks"`
	FinalFileSize    int64 `json:"final_file_size"`
	MergedChunkIdx   int   `json:"merged_chunk_idx"`
}

// ExportEngine 实现 engine.Engine 接口，执行分页提取→分块写文件→合并的导出流程。
type ExportEngine struct {
	src        phys.DataSource
	outputDir  string
	outputFile string
	pageSize   int
	chunkPages int
}

// ExportOption 是 ExportEngine 的配置函数。
type ExportOption func(*ExportEngine)

// WithPageSize 设置每页行数，默认 1000。
func WithPageSize(n int) ExportOption {
	return func(e *ExportEngine) { e.pageSize = n }
}

// WithChunkPages 设置每个分块包含的页数，默认 10。
func WithChunkPages(n int) ExportOption {
	return func(e *ExportEngine) { e.chunkPages = n }
}

// New 创建 ExportEngine。
func New(src phys.DataSource, outputDir, outputFile string, opts ...ExportOption) *ExportEngine {
	e := &ExportEngine{
		src:        src,
		outputDir:  outputDir,
		outputFile: outputFile,
		pageSize:   1000,
		chunkPages: 10,
	}
	for _, o := range opts {
		o(e)
	}
	return e
}

// 编译期检查
var _ engine.Engine = (*ExportEngine)(nil)

func (e *ExportEngine) TaskType() string { return "export" }

func (e *ExportEngine) Execute(ctx context.Context, state *statestore.BaseTaskState) (int64, error) {
	var p ExportPayload
	if len(state.Payload) > 0 {
		if err := json.Unmarshal(state.Payload, &p); err != nil {
			return 0, fmt.Errorf("export: unmarshal payload: %w", err)
		}
	}

	switch state.Phase {
	case statestore.PhasePending:
		p = ExportPayload{}
		state.Phase = statestore.PhaseRunning
		state.Payload = e.marshalPayload(p)
		return 0, nil

	case statestore.PhaseRunning:
		return e.executeRunning(ctx, state, &p)

	case statestore.PhaseMerging:
		return e.executeMerging(ctx, state, &p)

	default:
		return state.CheckpointLSN, nil
	}
}

func (e *ExportEngine) executeRunning(ctx context.Context, state *statestore.BaseTaskState, p *ExportPayload) (int64, error) {
	rows, err := e.src.FetchPage(ctx, p.CurrentPage, e.pageSize)
	if err == io.EOF {
		p.TotalChunks = p.CurrentChunkIdx
		if p.CurrentChunkSize > 0 {
			p.TotalChunks++
		}
		state.Phase = statestore.PhaseMerging
		state.Message = "extraction complete, starting merge"
		state.Payload = e.marshalPayload(*p)
		return e.calcLSN(*p), nil
	}
	if err != nil {
		return 0, fmt.Errorf("export: fetch page %d: %w", p.CurrentPage, err)
	}

	chunkPath := e.chunkPath(p.CurrentChunkIdx)
	f, err := os.OpenFile(chunkPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("export: open chunk %d: %w", p.CurrentChunkIdx, err)
	}

	written, err := e.writeRows(f, rows)
	f.Close()
	if err != nil {
		return 0, err
	}

	p.CurrentChunkSize += written
	p.CurrentPage++

	if p.CurrentPage%e.chunkPages == 0 {
		p.CurrentChunkIdx++
		p.CurrentChunkSize = 0
	}

	state.Message = fmt.Sprintf("extracting page %d", p.CurrentPage)
	state.Payload = e.marshalPayload(*p)
	return e.calcLSN(*p), nil
}

func (e *ExportEngine) executeMerging(ctx context.Context, state *statestore.BaseTaskState, p *ExportPayload) (int64, error) {
	if p.MergedChunkIdx >= p.TotalChunks {
		state.Phase = statestore.PhaseCompleted
		state.Message = "export completed"
		// cleanup deferred to caller via Cleanup()
		return p.FinalFileSize, nil
	}

	chunkPath := e.chunkPath(p.MergedChunkIdx)
	chunkData, err := os.ReadFile(chunkPath)
	if err != nil {
		return 0, fmt.Errorf("export: read chunk %d: %w", p.MergedChunkIdx, err)
	}

	finalPath := filepath.Join(e.outputDir, e.outputFile)
	f, err := os.OpenFile(finalPath, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return 0, fmt.Errorf("export: open final file: %w", err)
	}

	n, err := f.Write(chunkData)
	f.Close()
	if err != nil {
		return 0, fmt.Errorf("export: write final file: %w", err)
	}

	p.FinalFileSize += int64(n)
	p.MergedChunkIdx++

	state.Message = fmt.Sprintf("merging chunk %d/%d", p.MergedChunkIdx, p.TotalChunks)
	state.Payload = e.marshalPayload(*p)
	return p.FinalFileSize, nil
}

func (e *ExportEngine) Compensate(ctx context.Context, targetLSN int64) error {
	finalPath := filepath.Join(e.outputDir, e.outputFile)
	if info, err := os.Stat(finalPath); err == nil && info.Size() > targetLSN {
		if err := os.Truncate(finalPath, targetLSN); err != nil {
			return fmt.Errorf("export: truncate final file: %w", err)
		}
	}
	return nil
}

func (e *ExportEngine) Progress(state statestore.BaseTaskState) int {
	if state.Phase == statestore.PhaseCompleted {
		return 100
	}

	var p ExportPayload
	if len(state.Payload) > 0 {
		json.Unmarshal(state.Payload, &p)
	}
	if state.Phase == statestore.PhaseMerging {
		if p.TotalChunks == 0 {
			return 90
		}
		return 90 + p.MergedChunkIdx*10/p.TotalChunks
	}
	if p.TotalChunks == 0 {
		return 0
	}
	prog := p.CurrentChunkIdx * 100 / p.TotalChunks
	if prog > 90 {
		return 90
	}
	return prog
}

func (e *ExportEngine) calcLSN(p ExportPayload) int64 {
	return int64(p.CurrentChunkIdx)*e.chunkSize() + p.CurrentChunkSize
}

func (e *ExportEngine) chunkSize() int64 {
	return int64(e.pageSize * e.chunkPages)
}

func (e *ExportEngine) chunkPath(idx int) string {
	return filepath.Join(e.outputDir, fmt.Sprintf("%s.chunk_%d.tmp", e.outputFile, idx))
}

func (e *ExportEngine) marshalPayload(p ExportPayload) json.RawMessage {
	data, _ := json.Marshal(p)
	return data
}

func (e *ExportEngine) writeRows(f *os.File, rows []phys.Row) (int64, error) {
	var total int64
	for _, row := range rows {
		data, err := json.Marshal(row)
		if err != nil {
			return total, fmt.Errorf("export: marshal row: %w", err)
		}
		n, err := f.Write(append(data, '\n'))
		total += int64(n)
		if err != nil {
			return total, fmt.Errorf("export: write row: %w", err)
		}
	}
	return total, nil
}

func (e *ExportEngine) cleanupChunks(total int) {
	for i := 0; i < total; i++ {
		os.Remove(e.chunkPath(i))
	}
}

// Cleanup 清理导出过程中产生的分块文件。应在 Run() 成功返回后调用。
func (e *ExportEngine) Cleanup() {
	entries, _ := os.ReadDir(e.outputDir)
	prefix := e.outputFile + ".chunk_"
	for _, entry := range entries {
		if strings.HasPrefix(entry.Name(), prefix) && strings.HasSuffix(entry.Name(), ".tmp") {
			os.Remove(filepath.Join(e.outputDir, entry.Name()))
		}
	}
}
