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
)
