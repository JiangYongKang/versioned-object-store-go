package objectstore

import "errors"

// PreconditionKind 区分条件写入失败的具体原因。
type PreconditionKind string

const (
	// PreconditionKeyExists 表示要求键不存在但键已存在。
	PreconditionKeyExists PreconditionKind = "key_exists"
	// PreconditionVersionMismatch 表示期望的当前版本与实际不符。
	PreconditionVersionMismatch PreconditionKind = "version_mismatch"
)

// PreconditionError 表示条件写入的前置条件不成立。
type PreconditionError struct {
	Key      string
	Kind     PreconditionKind
	Expected string
	Actual   string
}

func (e *PreconditionError) Error() string {
	return "objectstore: precondition failed for key " + e.Key +
		": kind=" + string(e.Kind) + " expected=" + e.Expected + " actual=" + e.Actual
}

// Is 支持 errors.Is(err, ErrPreconditionFailed)。
func (e *PreconditionError) Is(target error) bool {
	return target == ErrPreconditionFailed
}

// LockError 表示数据目录已被其他使用者独占打开。
// Holder 尽量给出当前持有者的信息（来自目录内 LOCK 文件），便于定位占用方。
type LockError struct {
	Dir    string
	Holder string
}

func (e *LockError) Error() string {
	if e.Holder == "" {
		return "objectstore: directory " + e.Dir + " is exclusively locked by another process"
	}
	return "objectstore: directory " + e.Dir + " is exclusively locked by another process: " + e.Holder
}

// Is 支持 errors.Is(err, ErrDirLocked)。
func (e *LockError) Is(target error) bool { return target == ErrDirLocked }

// CorruptVersion 描述一个被判定为损坏的版本。
type CorruptVersion struct {
	Key       string
	VersionID string
	Reason    string
}

// CorruptionError 表示打开时发现真正的数据损坏（区别于可安全丢弃的崩溃残留）。
// 此时 Open 失败且不修改任何磁盘状态，重复打开结论一致。
type CorruptionError struct {
	Dir      string
	Versions []CorruptVersion // 元数据引用但数据缺失/不一致的版本
	Detail   string           // 其他损坏（如 meta.json 无法解析）的描述
}

func (e *CorruptionError) Error() string {
	msg := "objectstore: corruption detected in " + e.Dir
	if e.Detail != "" {
		msg += ": " + e.Detail
	}
	for _, v := range e.Versions {
		msg += "; key=" + v.Key + " version=" + v.VersionID + ": " + v.Reason
	}
	return msg
}

// Is 支持 errors.Is(err, ErrCorruption)。
func (e *CorruptionError) Is(target error) bool { return target == ErrCorruption }

var (
	// ErrPreconditionFailed 前置条件不成立（可用 errors.As 取 *PreconditionError 区分原因）。
	ErrPreconditionFailed = errors.New("objectstore: precondition failed")
	// ErrNotFound 对象或版本不存在。
	ErrNotFound = errors.New("objectstore: not found")
	// ErrDeleted 对象当前版本为删除标记。
	ErrDeleted = errors.New("objectstore: object is deleted")
	// ErrUploadNotFound 分片上传会话不存在。
	ErrUploadNotFound = errors.New("objectstore: upload not found")
	// ErrUploadCompleted 上传已完成，拒绝进一步修改。
	ErrUploadCompleted = errors.New("objectstore: upload already completed")
	// ErrUploadAborted 上传已中止，拒绝进一步修改。
	ErrUploadAborted = errors.New("objectstore: upload already aborted")
	// ErrPartNumberInvalid 分片编号越界。
	ErrPartNumberInvalid = errors.New("objectstore: part number out of range")
	// ErrPartMissing 完成上传时缺少必需分片。
	ErrPartMissing = errors.New("objectstore: required part missing")
	// ErrNotImplemented 骨架占位。
	ErrNotImplemented = errors.New("objectstore: not implemented")
	// ErrDirLocked 数据目录已被其他使用者独占打开（可用 errors.As 取 *LockError 看持有者）。
	ErrDirLocked = errors.New("objectstore: directory locked")
	// ErrCorruption 打开时发现真正的数据损坏（可用 errors.As 取 *CorruptionError 定位键与版本）。
	ErrCorruption = errors.New("objectstore: corruption detected")
)
