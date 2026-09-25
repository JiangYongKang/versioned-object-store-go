package objectstore

import (
	"log/slog"
	"time"
)

// Config 存储服务的可配置项。
type Config struct {
	// Dir 为本地数据根目录（元数据与对象数据均存放于此）。
	Dir string
	// MaxParts 为单次分片上传允许的最大分片数，<=0 时取默认值 10000。
	MaxParts int
	// Logger 运行日志，nil 时使用 slog.Default()。
	Logger *slog.Logger
}

func (c *Config) withDefaults() {
	if c.MaxParts <= 0 {
		c.MaxParts = 10000
	}
	if c.Logger == nil {
		c.Logger = slog.Default()
	}
}

// PutOptions 条件写入的前置条件。
type PutOptions struct {
	// IfAbsent 为 true 时要求键当前不存在（或当前版本为删除标记）。
	IfAbsent bool
	// ExpectVersion 非空时要求键的当前版本号与之相等。
	ExpectVersion string
}

// VersionInfo 描述一个对象版本。
type VersionInfo struct {
	Key       string
	VersionID string
	Size      int64
	Deleted   bool // 删除标记（tombstone）
	CreatedAt time.Time
}

// UploadState 分片上传状态机状态。
type UploadState string

const (
	UploadInitiated UploadState = "initiated"
	UploadCompleted UploadState = "completed"
	UploadAborted   UploadState = "aborted"
)

// PartInfo 描述一个已上传分片。
type PartInfo struct {
	PartNumber int
	Size       int64
	ETag       string
}

// UploadInfo 描述一次分片上传会话。
type UploadInfo struct {
	UploadID  string
	Key       string
	State     UploadState
	Parts     []PartInfo
	CreatedAt time.Time
}
