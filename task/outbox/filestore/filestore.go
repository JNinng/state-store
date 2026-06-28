// Package filestore 提供基于本地文件系统的 outbox.Store 实现。
//
// 采用与 filestore 包相同的原子写模式（tmp 写 → Sync → Rename），
// 每条 outbox 消息一个文件：<dir>/<msgID>.msg.json。
//
// 适用场景: 单进程或共享文件系统，消息量在数千级别。
// 高吞吐场景建议使用数据库或消息队列作为 Store 后端。
package filestore

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"state-store/task/outbox"
)

const (
	msgSuffix = ".msg.json"
	tmpSuffix = ".msg.json.tmp"
)

// FileStore 是基于本地文件系统的 outbox.Store 实现。
type FileStore struct {
	dir string
}

// New 创建一个 FileStore。如果目录不存在则自动创建（os.MkdirAll 0755）。
// 构造时自动清理上次崩溃遗留的 .msg.json.tmp 文件。
func New(dir string) (*FileStore, error) {
	if err := os.MkdirAll(dir, 0755); err != nil {
		return nil, fmt.Errorf("outbox filestore: create dir %s: %w", dir, err)
	}
	s := &FileStore{dir: dir}
	s.cleanup()
	return s, nil
}

// ---- outbox.Store 实现 ----

// Append 原子追加一条消息。ID 重复时返回 outbox.ErrDuplicateID。
func (s *FileStore) Append(ctx context.Context, msg *outbox.Message) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	finalPath := s.msgPath(msg.ID)

	// 检查是否已存在（Append 不幂等）
	if _, err := os.Stat(finalPath); err == nil {
		return fmt.Errorf("%w: %s", outbox.ErrDuplicateID, msg.ID)
	}

	// 设置默认值
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}
	if msg.Status == "" {
		msg.Status = outbox.StatusPending
	}

	return s.writeAtomic(msg.ID, msg)
}

// FetchPending 返回所有待处理的消息（status=pending 或 processing），
// 按 CreatedAt 升序排列（FIFO）。
func (s *FileStore) FetchPending(ctx context.Context) ([]*outbox.Message, error) {
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return nil, fmt.Errorf("outbox filestore: read dir: %w", err)
	}

	pending := make([]*outbox.Message, 0)
	for _, entry := range entries {
		name := entry.Name()

		// 跳过临时文件和无关文件
		if strings.HasSuffix(name, tmpSuffix) || !strings.HasSuffix(name, msgSuffix) {
			continue
		}

		data, err := os.ReadFile(filepath.Join(s.dir, name))
		if err != nil {
			// 文件可能被 Cleanup 或其他进程删除，跳过
			continue
		}

		var msg outbox.Message
		if err := json.Unmarshal(data, &msg); err != nil {
			// 损坏的文件，跳过
			continue
		}

		if msg.Status == outbox.StatusPending || msg.Status == outbox.StatusProcessing {
			pending = append(pending, &msg)
		}
	}

	// 按 CreatedAt 稳定排序（FIFO）
	sort.SliceStable(pending, func(i, j int) bool {
		return pending[i].CreatedAt.Before(pending[j].CreatedAt)
	})

	return pending, nil
}

// MarkProcessed 将消息标记为已完成。消息不存在时幂等返回 nil。
func (s *FileStore) MarkProcessed(ctx context.Context, msgID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg, err := s.read(msgID)
	if err != nil {
		return nil // 文件不存在 — 幂等
	}

	now := time.Now()
	msg.Status = outbox.StatusCompleted
	msg.DeliveredAt = &now

	return s.writeAtomic(msgID, msg)
}

// MarkFailed 将消息标记为失败。消息不存在时幂等返回 nil。
func (s *FileStore) MarkFailed(ctx context.Context, msgID string, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg, err := s.read(msgID)
	if err != nil {
		return nil // 文件不存在 — 幂等
	}

	msg.Status = outbox.StatusFailed
	msg.LastError = lastError

	return s.writeAtomic(msgID, msg)
}

// UpdateStatus 原子更新消息的状态、重试计数和错误信息。
// 消息不存在时返回错误。
func (s *FileStore) UpdateStatus(ctx context.Context, msgID string, status outbox.MessageStatus, retries int, lastError string) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	msg, err := s.read(msgID)
	if err != nil {
		return fmt.Errorf("outbox filestore: message %s not found", msgID)
	}

	msg.Status = status
	msg.Retries = retries
	msg.LastError = lastError

	return s.writeAtomic(msgID, msg)
}

// Cleanup 删除所有残留的 .msg.json.tmp 临时文件（崩溃恢复）。
func (s *FileStore) Cleanup() error {
	return s.cleanup()
}

// ---- 内部方法 ----

func (s *FileStore) msgPath(msgID string) string {
	return filepath.Join(s.dir, msgID+msgSuffix)
}

func (s *FileStore) tmpPath(msgID string) string {
	return filepath.Join(s.dir, msgID+tmpSuffix)
}

// read 读取一条消息。文件不存在时返回 os.IsNotExist 错误。
func (s *FileStore) read(msgID string) (*outbox.Message, error) {
	path := s.msgPath(msgID)
	// 清理可能残留的 tmp 文件
	os.Remove(s.tmpPath(msgID))

	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var msg outbox.Message
	if err := json.Unmarshal(data, &msg); err != nil {
		return nil, fmt.Errorf("outbox filestore: unmarshal %s: %w", msgID, err)
	}

	return &msg, nil
}

// writeAtomic 以原子方式写入消息（tmp → Sync → Rename）。
func (s *FileStore) writeAtomic(msgID string, msg *outbox.Message) error {
	tmpPath := s.tmpPath(msgID)
	finalPath := s.msgPath(msgID)

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("outbox filestore: marshal %s: %w", msgID, err)
	}

	f, err := os.Create(tmpPath)
	if err != nil {
		return fmt.Errorf("outbox filestore: create tmp %s: %w", msgID, err)
	}

	if _, err := f.Write(data); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("outbox filestore: write tmp %s: %w", msgID, err)
	}

	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmpPath)
		return fmt.Errorf("outbox filestore: sync tmp %s: %w", msgID, err)
	}
	f.Close()

	if err := os.Rename(tmpPath, finalPath); err != nil {
		os.Remove(tmpPath)
		return fmt.Errorf("outbox filestore: rename %s: %w", msgID, err)
	}

	return nil
}

// cleanup 删除所有 .msg.json.tmp 文件。
func (s *FileStore) cleanup() error {
	entries, err := os.ReadDir(s.dir)
	if err != nil {
		return fmt.Errorf("outbox filestore: read dir: %w", err)
	}

	for _, entry := range entries {
		if strings.HasSuffix(entry.Name(), tmpSuffix) {
			os.Remove(filepath.Join(s.dir, entry.Name()))
		}
	}

	return nil
}

// 编译期接口检查
var _ outbox.Store = (*FileStore)(nil)
