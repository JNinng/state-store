package phys

import "context"

// Row 是通用的数据行载体，调用方自行定义键值语义。
type Row map[string]interface{}

// DataSource 是导出引擎从数据源分页读取的抽象。
// 调用方实现具体的数据库查询、API 调用等。
type DataSource interface {
	// FetchPage 获取第 page 页的数据（page 从 0 开始）。
	// 每页返回最多 pageSize 行。
	// 返回 io.EOF 表示无更多数据可供读取。
	FetchPage(ctx context.Context, page int, pageSize int) ([]Row, error)
}

// DataTarget 是导入引擎向目标批量写入的抽象。
//
// 契约：实现方必须保证写入的幂等性。进程崩溃恢复时，
// 已入库但未 checkpoint 的数据会被重新写入，
// DataTarget 需通过 UPSERT / 唯一键去重 / 预检等方式处理。
type DataTarget interface {
	// WriteBatch 批量写入行，返回实际插入的行数。
	WriteBatch(ctx context.Context, rows []Row) (inserted int64, err error)
}
