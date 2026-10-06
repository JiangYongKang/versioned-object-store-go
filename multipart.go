package objectstore

import (
	"crypto/md5"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sort"
	"time"
)

// partMeta 是单个分片的持久化元数据。
type partMeta struct {
	Size int64  `json:"size"`
	ETag string `json:"etag"`
}

// uploadMeta 是分片上传会话的持久化状态。
type uploadMeta struct {
	UploadID      string           `json:"uploadId"`
	Key           string           `json:"key"`
	State         UploadState      `json:"state"`
	Parts         map[int]partMeta `json:"parts"`
	CreatedAt     time.Time        `json:"createdAt"`
	ResultVersion string           `json:"resultVersion,omitempty"` // 完成后幂等返回用
}

func (s *Store) uploadsDir() string { return filepath.Join(s.cfg.Dir, "uploads") }

func (s *Store) uploadDir(id string) string { return filepath.Join(s.uploadsDir(), id) }

func (s *Store) partPath(id string, n int) string {
	return filepath.Join(s.uploadDir(id), fmt.Sprintf("part-%d", n))
}

// CreateUpload 发起一次分片上传，返回上传会话标识。
func (s *Store) CreateUpload(key string) (string, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return "", errors.New("objectstore: store closed")
	}
	id := fmt.Sprintf("u%08d", s.m.NextVersion)
	s.m.NextVersion++
	um := &uploadMeta{UploadID: id, Key: key, State: UploadInitiated, Parts: map[int]partMeta{}, CreatedAt: time.Now()}
	s.m.Uploads[id] = um
	if err := os.MkdirAll(s.uploadDir(id), 0o755); err != nil {
		return "", err
	}
	if err := s.persistLocked(); err != nil {
		return "", err
	}
	s.log.Info("upload created", "key", key, "upload", id)
	return id, nil
}

// getUploadLocked 取出会话并按状态机拒绝非法操作。
// allowTerminal 为 true 时直接返回会话（由调用方决定幂等语义）。
func (s *Store) getUploadLocked(id string) (*uploadMeta, error) {
	um := s.m.Uploads[id]
	if um == nil {
		return nil, ErrUploadNotFound
	}
	return um, nil
}

// UploadPart 上传一个分片；乱序到达与重复上传均被接受（重复为幂等覆盖）。
// 已完成/已中止的会话拒绝继续上传；分片编号越界被拒绝。
func (s *Store) UploadPart(uploadID string, partNumber int, r io.Reader) (PartInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return PartInfo{}, errors.New("objectstore: store closed")
	}
	um, err := s.getUploadLocked(uploadID)
	if err != nil {
		return PartInfo{}, err
	}
	switch um.State {
	case UploadCompleted:
		s.log.Info("upload part rejected", "upload", uploadID, "part", partNumber, "reason", "upload already completed")
		return PartInfo{}, ErrUploadCompleted
	case UploadAborted:
		s.log.Info("upload part rejected", "upload", uploadID, "part", partNumber, "reason", "upload already aborted")
		return PartInfo{}, ErrUploadAborted
	}
	if partNumber < 1 || partNumber > s.cfg.MaxParts {
		s.log.Info("upload part rejected", "upload", uploadID, "part", partNumber, "reason", "part number out of range", "max", s.cfg.MaxParts)
		return PartInfo{}, ErrPartNumberInvalid
	}
	// 先写临时文件再原子就位；重复上传同一编号为幂等覆盖。
	tmp := s.partPath(uploadID, partNumber) + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return PartInfo{}, err
	}
	h := md5.New()
	n, err := io.Copy(io.MultiWriter(f, h), r)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return PartInfo{}, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return PartInfo{}, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return PartInfo{}, err
	}
	if err := os.Rename(tmp, s.partPath(uploadID, partNumber)); err != nil {
		os.Remove(tmp)
		return PartInfo{}, err
	}
	etag := hex.EncodeToString(h.Sum(nil))
	um.Parts[partNumber] = partMeta{Size: n, ETag: etag}
	if err := s.persistLocked(); err != nil {
		return PartInfo{}, err
	}
	s.log.Info("part uploaded", "upload", uploadID, "part", partNumber, "size", n, "etag", etag)
	return PartInfo{PartNumber: partNumber, Size: n, ETag: etag}, nil
}

// CompleteUpload 按给定分片编号顺序拼接完成上传，产生对象新版本。
// 重复完成幂等返回首次完成的版本；已中止的会话拒绝完成；
// 引用了未上传分片时被拒绝并指出缺失分片。
func (s *Store) CompleteUpload(uploadID string, parts []int) (VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return VersionInfo{}, errors.New("objectstore: store closed")
	}
	um, err := s.getUploadLocked(uploadID)
	if err != nil {
		return VersionInfo{}, err
	}
	switch um.State {
	case UploadCompleted:
		// 幂等：返回首次完成产生的版本。
		s.log.Info("complete idempotent replay", "upload", uploadID, "version", um.ResultVersion)
		return s.versionInfoLocked(um.Key, um.ResultVersion)
	case UploadAborted:
		s.log.Info("complete rejected", "upload", uploadID, "reason", "upload already aborted")
		return VersionInfo{}, ErrUploadAborted
	}
	if len(parts) == 0 {
		return VersionInfo{}, fmt.Errorf("%w: empty part list", ErrPartMissing)
	}
	for _, n := range parts {
		if n < 1 || n > s.cfg.MaxParts {
			s.log.Info("complete rejected", "upload", uploadID, "part", n, "reason", "part number out of range")
			return VersionInfo{}, ErrPartNumberInvalid
		}
		if _, ok := um.Parts[n]; !ok {
			s.log.Info("complete rejected", "upload", uploadID, "part", n, "reason", "required part missing")
			return VersionInfo{}, fmt.Errorf("%w: part %d", ErrPartMissing, n)
		}
	}
	// 拼接分片为新版本数据：先写数据就位，再提交元数据。
	vid := s.nextVersionIDLocked()
	if err := os.MkdirAll(s.keyDir(um.Key), 0o755); err != nil {
		return VersionInfo{}, err
	}
	tmp := filepath.Join(s.keyDir(um.Key), ".tmp-"+vid)
	out, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return VersionInfo{}, err
	}
	h := sha256.New()
	w := io.MultiWriter(out, h)
	var size int64
	for _, n := range parts {
		pf, err := os.Open(s.partPath(uploadID, n))
		if err != nil {
			out.Close()
			os.Remove(tmp)
			return VersionInfo{}, err
		}
		written, err := io.Copy(w, pf)
		pf.Close()
		size += written
		if err != nil {
			out.Close()
			os.Remove(tmp)
			return VersionInfo{}, err
		}
	}
	if err := out.Sync(); err != nil {
		out.Close()
		os.Remove(tmp)
		return VersionInfo{}, err
	}
	if err := out.Close(); err != nil {
		os.Remove(tmp)
		return VersionInfo{}, err
	}
	if err := os.Rename(tmp, s.dataPath(um.Key, vid)); err != nil {
		os.Remove(tmp)
		return VersionInfo{}, err
	}
	vm := &versionMeta{VersionID: vid, Size: size, CreatedAt: time.Now(), SHA256: hex.EncodeToString(h.Sum(nil))}
	s.commitVersionLocked(um.Key, vm)
	um.State = UploadCompleted
	um.ResultVersion = vid
	if err := s.persistLocked(); err != nil {
		return VersionInfo{}, err
	}
	// 元数据已提交，分片目录可回收；失败留给恢复/GC 清理。
	if err := os.RemoveAll(s.uploadDir(uploadID)); err != nil {
		s.log.Warn("complete: failed to remove upload dir, left for GC", "upload", uploadID, "err", err)
	}
	s.log.Info("upload completed", "key", um.Key, "upload", uploadID, "version", vid, "size", size, "parts", parts)
	return VersionInfo{Key: um.Key, VersionID: vid, Size: size, CreatedAt: vm.CreatedAt}, nil
}

// AbortUpload 中止上传并回收已上传分片；重复中止幂等成功；
// 已完成的会话拒绝中止。中止后不产生任何可见对象。
func (s *Store) AbortUpload(uploadID string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return errors.New("objectstore: store closed")
	}
	um, err := s.getUploadLocked(uploadID)
	if err != nil {
		return err
	}
	switch um.State {
	case UploadCompleted:
		s.log.Info("abort rejected", "upload", uploadID, "reason", "upload already completed")
		return ErrUploadCompleted
	case UploadAborted:
		s.log.Info("abort idempotent replay", "upload", uploadID)
		return nil
	}
	um.State = UploadAborted
	if err := s.persistLocked(); err != nil {
		return err
	}
	if err := os.RemoveAll(s.uploadDir(uploadID)); err != nil {
		s.log.Warn("abort: failed to remove upload dir, left for GC", "upload", uploadID, "err", err)
	}
	s.log.Info("upload aborted", "key", um.Key, "upload", uploadID)
	return nil
}

// ListParts 列出上传会话已收到的分片（按编号升序）。
func (s *Store) ListParts(uploadID string) (UploadInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return UploadInfo{}, errors.New("objectstore: store closed")
	}
	um, err := s.getUploadLocked(uploadID)
	if err != nil {
		return UploadInfo{}, err
	}
	info := UploadInfo{UploadID: um.UploadID, Key: um.Key, State: um.State, CreatedAt: um.CreatedAt}
	for n, pm := range um.Parts {
		info.Parts = append(info.Parts, PartInfo{PartNumber: n, Size: pm.Size, ETag: pm.ETag})
	}
	sort.Slice(info.Parts, func(i, j int) bool { return info.Parts[i].PartNumber < info.Parts[j].PartNumber })
	return info, nil
}

func (s *Store) versionInfoLocked(key, versionID string) (VersionInfo, error) {
	obj := s.m.Objects[key]
	if obj == nil {
		return VersionInfo{}, ErrNotFound
	}
	vm := obj.find(versionID)
	if vm == nil {
		return VersionInfo{}, ErrNotFound
	}
	return VersionInfo{Key: key, VersionID: vm.VersionID, Size: vm.Size, Deleted: vm.Deleted, CreatedAt: vm.CreatedAt}, nil
}

// recoverUploadsLocked 恢复上传目录：仅保留 initiated 会话在元数据中登记的分片，
// 其余目录与文件全部清除，避免无法回收的残留分片。
func (s *Store) recoverUploadsLocked() error {
	entries, err := os.ReadDir(s.uploadsDir())
	if err != nil {
		return err
	}
	for _, e := range entries {
		if !e.IsDir() {
			s.log.Info("recovery: removing stray file in uploads dir", "name", e.Name())
			if err := os.Remove(filepath.Join(s.uploadsDir(), e.Name())); err != nil {
				return err
			}
			continue
		}
		id := e.Name()
		um := s.m.Uploads[id]
		if um == nil || um.State != UploadInitiated {
			s.log.Info("recovery: removing terminal/unknown upload dir", "upload", id)
			if err := os.RemoveAll(s.uploadDir(id)); err != nil {
				return err
			}
			continue
		}
		// initiated 会话：删除未登记的分片文件与临时文件。
		files, err := os.ReadDir(s.uploadDir(id))
		if err != nil {
			return err
		}
		for _, f := range files {
			var n int
			if _, err := fmt.Sscanf(f.Name(), "part-%d", &n); err == nil {
				if _, ok := um.Parts[n]; ok {
					continue
				}
			}
			s.log.Info("recovery: removing unregistered part file", "upload", id, "file", f.Name())
			if err := os.Remove(filepath.Join(s.uploadDir(id), f.Name())); err != nil {
				return err
			}
		}
	}
	return nil
}
