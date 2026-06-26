package filestore

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"state-store/statestore"
	"strings"
)

// FileRepository 是基于本地文件系统的 StateRepository 实现。
// 使用 tmp + Rename 策略保证写入原子性。
type FileRepository struct {
	dir string
}

// New 创建 FileRepository。若目录不存在则自动创建。
func New(dir string) (*FileRepository, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("filestore: create dir %s: %w", dir, err)
	}
	return &FileRepository{dir: dir}, nil
}

// Load 读取任务状态文件。任务不存在返回 nil, nil。
func (r *FileRepository) Load(ctx context.Context, taskID string) ([]byte, error) {
	// 自动清理上次崩溃可能残留的 .tmp（不存在则忽略）
	os.Remove(r.tmpPath(taskID))

	if err := ctx.Err(); err != nil {
		return nil, err
	}
	data, err := os.ReadFile(r.statePath(taskID))
	if os.IsNotExist(err) {
		return nil, nil
	}
	if err != nil {
		return nil, fmt.Errorf("filestore: load %s: %w", taskID, statestore.ErrLoadFailed)
	}
	return data, nil
}

// Save 原子写入任务状态。流程：写 .tmp → Sync → Rename。
func (r *FileRepository) Save(ctx context.Context, taskID string, state []byte) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	tmpPath := r.tmpPath(taskID)
	finalPath := r.statePath(taskID)

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("filestore: save %s: create tmp: %w", taskID, statestore.ErrSaveFailed)
	}

	if _, err := f.Write(state); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("filestore: save %s: write tmp: %w", taskID, statestore.ErrSaveFailed)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("filestore: save %s: sync tmp: %w", taskID, statestore.ErrSaveFailed)
	}
	f.Close()

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("filestore: save %s: rename: %w", taskID, statestore.ErrSaveFailed)
	}

	return nil
}

// Delete 删除任务状态文件，幂等。
func (r *FileRepository) Delete(ctx context.Context, taskID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if err := os.Remove(r.statePath(taskID)); err != nil && !os.IsNotExist(err) {
		return err
	}
	return nil
}

// Cleanup 清理残留的 .tmp 文件（进程崩溃后遗留）。
func (r *FileRepository) Cleanup() error {
	entries, err := os.ReadDir(r.dir)
	if err != nil {
		return fmt.Errorf("filestore: read dir: %w", err)
	}
	for _, e := range entries {
		if strings.HasSuffix(e.Name(), ".state.tmp") {
			os.Remove(filepath.Join(r.dir, e.Name()))
		}
	}
	return nil
}

func (r *FileRepository) statePath(taskID string) string {
	return filepath.Join(r.dir, taskID+".state")
}

func (r *FileRepository) tmpPath(taskID string) string {
	return filepath.Join(r.dir, taskID+".state.tmp")
}
