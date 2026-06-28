package outbox

import "errors"

// ErrDuplicateID 表示尝试追加一条 ID 已存在的消息。
// Store.Append 在消息 ID 重复时返回此错误（或包裹此错误）。
var ErrDuplicateID = errors.New("outbox: message ID already exists")
