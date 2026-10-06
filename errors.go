package objectstore

import (
	"errors"
	"strconv"
)

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
	// ErrDirLocked 数据目录已被其他使用者独占打开。
	ErrDirLocked = errors.New("objectstore: directory is locked by another opener")
	// ErrCorruption 打开时发现真正的数据损坏（区别于可安全丢弃的崩溃残留）。
	ErrCorruption = errors.New("objectstore: data corruption detected")
)

// DirLockedError 表示打开被拒绝：目录已被其他使用者独占持有。
type DirLockedError struct {
	Dir string
	// HolderPID 为锁文件中记录的持有方进程号（不可用时为 0）。
	HolderPID int
}

func (e *DirLockedError) Error() string {
	if e.HolderPID > 0 {
		return "objectstore: directory " + e.Dir + " is exclusively opened by pid " +
			strconv.Itoa(e.HolderPID)
	}
	return "objectstore: directory " + e.Dir + " is exclusively opened by another process"
}

// Is 支持 errors.Is(err, ErrDirLocked)。
func (e *DirLockedError) Is(target error) bool { return target == ErrDirLocked }

// CorruptVersion 指出一个损坏的版本及其原因。
type CorruptVersion struct {
	Key       string
	VersionID string
	Reason    string
}

// CorruptionError 表示打开时发现不可自动修复的数据损坏。
// 与崩溃残留（无引用文件、未提交临时文件）不同，此类问题必须显式报错，
// 不得静默抹除有问题的版本。
type CorruptionError struct {
	// Meta 非空表示元数据文件本身损坏（截断/非法内容）。
	Meta error
	// Versions 列出元数据完好但数据缺失/不一致的版本。
	Versions []CorruptVersion
}

func (e *CorruptionError) Error() string {
	if e.Meta != nil {
		return "objectstore: corrupt meta.json: " + e.Meta.Error()
	}
	msg := "objectstore: corrupted versions:"
	for _, v := range e.Versions {
		msg += " [" + v.Key + "@" + v.VersionID + ": " + v.Reason + "]"
	}
	return msg
}

// Is 支持 errors.Is(err, ErrCorruption)。
func (e *CorruptionError) Is(target error) bool { return target == ErrCorruption }

