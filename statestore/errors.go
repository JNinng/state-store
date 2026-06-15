package statestore

import "errors"

var (
	// ErrSaveFailed 表示状态保存失败，底层存储无法完成原子写入。
	ErrSaveFailed = errors.New("statestore: save failed")

	// ErrLoadFailed 表示状态加载失败，底层存储读取错误（非"不存在"）。
	ErrLoadFailed = errors.New("statestore: load failed")
)
