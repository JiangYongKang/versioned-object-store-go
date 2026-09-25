package objectstore

import (
	"crypto/sha1"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"os"
	"path/filepath"
	"sort"
	"sync"
	"time"
)

// 磁盘布局：
//   Dir/meta.json                 元数据（tmp+rename 原子提交）
//   Dir/data/<hash(key)>/<verID>  对象版本数据（先写 tmp 再 rename 就位）
//   Dir/uploads/<uploadID>/...    进行中的分片（见 multipart.go）

// versionMeta 是单个版本的持久化元数据。
type versionMeta struct {
	VersionID string    `json:"versionId"`
	Size      int64     `json:"size"`
	Deleted   bool      `json:"deleted"`
	CreatedAt time.Time `json:"createdAt"`
}

// objectMeta 是一个键的全部版本，按创建顺序排列。
type objectMeta struct {
	Versions []*versionMeta `json:"versions"`
}

func (o *objectMeta) current() *versionMeta {
	if len(o.Versions) == 0 {
		return nil
	}
	return o.Versions[len(o.Versions)-1]
}

func (o *objectMeta) find(versionID string) *versionMeta {
	for _, v := range o.Versions {
		if v.VersionID == versionID {
			return v
		}
	}
	return nil
}

// meta 是整个存储的持久化元数据。
type meta struct {
	NextVersion uint64                 `json:"nextVersion"`
	Objects     map[string]*objectMeta `json:"objects"`
	Uploads     map[string]*uploadMeta `json:"uploads"`
}

// Store 是本机版本化对象存储。
type Store struct {
	mu     sync.Mutex
	cfg    Config
	log    *slog.Logger
	m      *meta
	closed bool
}

// Open 打开（必要时创建）位于 cfg.Dir 的存储，并执行崩溃恢复：
// 丢弃未提交完的元数据临时文件，清除无元数据引用的数据文件与上传目录，
// 保证重启后不存在悬挂条目。
func Open(cfg Config) (*Store, error) {
	cfg.withDefaults()
	s := &Store{cfg: cfg, log: cfg.Logger}
	for _, d := range []string{cfg.Dir, s.dataDir(), s.uploadsDir()} {
		if err := os.MkdirAll(d, 0o755); err != nil {
			return nil, err
		}
	}
	// 丢弃崩溃残留的未提交元数据临时文件。
	if err := os.Remove(s.metaPath() + ".tmp"); err != nil && !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	m := &meta{NextVersion: 1, Objects: map[string]*objectMeta{}, Uploads: map[string]*uploadMeta{}}
	if b, err := os.ReadFile(s.metaPath()); err == nil {
		if err := json.Unmarshal(b, m); err != nil {
			return nil, fmt.Errorf("objectstore: corrupt meta.json: %w", err)
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return nil, err
	}
	if m.Objects == nil {
		m.Objects = map[string]*objectMeta{}
	}
	if m.Uploads == nil {
		m.Uploads = map[string]*uploadMeta{}
	}
	if m.NextVersion == 0 {
		m.NextVersion = 1
	}
	s.m = m
	if err := s.recoverLocked(); err != nil {
		return nil, err
	}
	s.log.Info("store opened", "dir", cfg.Dir, "objects", len(m.Objects), "uploads", len(m.Uploads))
	return s, nil
}

// Close 关闭存储，后续调用返回错误。
func (s *Store) Close() error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.closed = true
	return nil
}

func (s *Store) metaPath() string { return filepath.Join(s.cfg.Dir, "meta.json") }
func (s *Store) dataDir() string  { return filepath.Join(s.cfg.Dir, "data") }

func (s *Store) keyDir(key string) string {
	sum := sha1.Sum([]byte(key))
	return filepath.Join(s.dataDir(), hex.EncodeToString(sum[:]))
}

func (s *Store) dataPath(key, versionID string) string {
	return filepath.Join(s.keyDir(key), versionID)
}

// persistLocked 原子提交元数据：写临时文件、fsync、rename。
func (s *Store) persistLocked() error {
	b, err := json.MarshalIndent(s.m, "", "  ")
	if err != nil {
		return err
	}
	tmp := s.metaPath() + ".tmp"
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return err
	}
	if _, err := f.Write(b); err != nil {
		f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	return os.Rename(tmp, s.metaPath())
}

// writeDataLocked 将 r 的内容先写入临时文件再原子就位，返回字节数。
// 调用方必须已持有锁；就位失败时清理临时文件。
func (s *Store) writeDataLocked(key, versionID string, r io.Reader) (int64, error) {
	dir := s.keyDir(key)
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return 0, err
	}
	tmp := filepath.Join(dir, ".tmp-"+versionID)
	f, err := os.OpenFile(tmp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, 0o644)
	if err != nil {
		return 0, err
	}
	n, err := io.Copy(f, r)
	if err != nil {
		f.Close()
		os.Remove(tmp)
		return 0, err
	}
	if err := f.Sync(); err != nil {
		f.Close()
		os.Remove(tmp)
		return 0, err
	}
	if err := f.Close(); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	if err := os.Rename(tmp, s.dataPath(key, versionID)); err != nil {
		os.Remove(tmp)
		return 0, err
	}
	return n, nil
}

// checkPreconditionsLocked 校验条件写入前置条件，失败时返回可区分原因的 *PreconditionError。
func (s *Store) checkPreconditionsLocked(key string, opts PutOptions) error {
	obj := s.m.Objects[key]
	var cur *versionMeta
	if obj != nil {
		cur = obj.current()
	}
	exists := cur != nil && !cur.Deleted
	if opts.IfAbsent && exists {
		return &PreconditionError{Key: key, Kind: PreconditionKeyExists, Expected: "<absent>", Actual: cur.VersionID}
	}
	if opts.ExpectVersion != "" {
		actual := ""
		if cur != nil {
			actual = cur.VersionID
		}
		if cur == nil || cur.Deleted || cur.VersionID != opts.ExpectVersion {
			return &PreconditionError{Key: key, Kind: PreconditionVersionMismatch, Expected: opts.ExpectVersion, Actual: actual}
		}
	}
	return nil
}

// commitVersionLocked 在元数据中追加版本并持久化。
func (s *Store) commitVersionLocked(key string, vm *versionMeta) {
	obj := s.m.Objects[key]
	if obj == nil {
		obj = &objectMeta{}
		s.m.Objects[key] = obj
	}
	obj.Versions = append(obj.Versions, vm)
}

func (s *Store) nextVersionIDLocked() string {
	id := fmt.Sprintf("v%08d", s.m.NextVersion)
	s.m.NextVersion++
	return id
}

// Put 写入对象新版本，可附带条件写入前置条件。
// 数据先完整落盘再提交元数据，崩溃不会留下半写版本。
func (s *Store) Put(key string, r io.Reader, opts PutOptions) (VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return VersionInfo{}, errors.New("objectstore: store closed")
	}
	if err := s.checkPreconditionsLocked(key, opts); err != nil {
		s.log.Info("put rejected", "key", key, "reason", err)
		return VersionInfo{}, err
	}
	vid := s.nextVersionIDLocked()
	size, err := s.writeDataLocked(key, vid, r)
	if err != nil {
		return VersionInfo{}, err
	}
	vm := &versionMeta{VersionID: vid, Size: size, CreatedAt: time.Now()}
	s.commitVersionLocked(key, vm)
	if err := s.persistLocked(); err != nil {
		return VersionInfo{}, err
	}
	s.log.Info("put committed", "key", key, "version", vid, "size", size)
	return VersionInfo{Key: key, VersionID: vid, Size: size, CreatedAt: vm.CreatedAt}, nil
}

// Get 读取键的当前版本。
func (s *Store) Get(key string) (io.ReadCloser, VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, VersionInfo{}, errors.New("objectstore: store closed")
	}
	obj := s.m.Objects[key]
	cur := (*versionMeta)(nil)
	if obj != nil {
		cur = obj.current()
	}
	if cur == nil {
		s.log.Info("get missed", "key", key, "reason", "no such key")
		return nil, VersionInfo{}, ErrNotFound
	}
	if cur.Deleted {
		s.log.Info("get missed", "key", key, "reason", "current version is delete marker", "version", cur.VersionID)
		return nil, VersionInfo{}, ErrDeleted
	}
	return s.openVersionLocked(key, cur)
}

// GetVersion 按版本号寻址读取历史版本（含被覆盖或被删除的版本）。
func (s *Store) GetVersion(key, versionID string) (io.ReadCloser, VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, VersionInfo{}, errors.New("objectstore: store closed")
	}
	obj := s.m.Objects[key]
	if obj == nil {
		return nil, VersionInfo{}, ErrNotFound
	}
	vm := obj.find(versionID)
	if vm == nil {
		return nil, VersionInfo{}, ErrNotFound
	}
	if vm.Deleted {
		// 删除标记本身无数据，但可寻址返回其元数据。
		s.log.Info("get version is delete marker", "key", key, "version", versionID)
		return io.NopCloser(emptyReader{}), VersionInfo{Key: key, VersionID: vm.VersionID, Deleted: true, CreatedAt: vm.CreatedAt}, nil
	}
	return s.openVersionLocked(key, vm)
}

func (s *Store) openVersionLocked(key string, vm *versionMeta) (io.ReadCloser, VersionInfo, error) {
	f, err := os.Open(s.dataPath(key, vm.VersionID))
	if err != nil {
		// 元数据引用了缺失数据：恢复流程应已杜绝，此处防御性报错。
		return nil, VersionInfo{}, fmt.Errorf("objectstore: data file missing for key %s version %s: %w", key, vm.VersionID, err)
	}
	s.log.Info("get version", "key", key, "version", vm.VersionID, "size", vm.Size)
	return f, VersionInfo{Key: key, VersionID: vm.VersionID, Size: vm.Size, CreatedAt: vm.CreatedAt}, nil
}

type emptyReader struct{}

func (emptyReader) Read([]byte) (int, error) { return 0, io.EOF }

// Delete 写入删除标记作为新的当前版本；历史版本仍可寻址读取。
func (s *Store) Delete(key string) (VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return VersionInfo{}, errors.New("objectstore: store closed")
	}
	obj := s.m.Objects[key]
	if obj == nil || obj.current() == nil || obj.current().Deleted {
		s.log.Info("delete rejected", "key", key, "reason", "no such key")
		return VersionInfo{}, ErrNotFound
	}
	vid := s.nextVersionIDLocked()
	vm := &versionMeta{VersionID: vid, Deleted: true, CreatedAt: time.Now()}
	s.commitVersionLocked(key, vm)
	if err := s.persistLocked(); err != nil {
		return VersionInfo{}, err
	}
	s.log.Info("delete committed", "key", key, "version", vid)
	return VersionInfo{Key: key, VersionID: vid, Deleted: true, CreatedAt: vm.CreatedAt}, nil
}

// ListVersions 列出键的全部版本（含删除标记），按创建顺序排列。
func (s *Store) ListVersions(key string) ([]VersionInfo, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return nil, errors.New("objectstore: store closed")
	}
	obj := s.m.Objects[key]
	if obj == nil {
		return nil, ErrNotFound
	}
	out := make([]VersionInfo, 0, len(obj.Versions))
	for _, vm := range obj.Versions {
		out = append(out, VersionInfo{Key: key, VersionID: vm.VersionID, Size: vm.Size, Deleted: vm.Deleted, CreatedAt: vm.CreatedAt})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].VersionID < out[j].VersionID })
	return out, nil
}

// recoverLocked 崩溃恢复：清除无元数据引用的数据文件与上传残留，
// 保证不存在引用了缺失数据或无对应元数据的悬挂条目。
func (s *Store) recoverLocked() error {
	// 1. 元数据引用的数据文件必须存在；缺失则移除该版本条目（防御，正常不应发生）。
	referenced := map[string]bool{}
	for key, obj := range s.m.Objects {
		kept := obj.Versions[:0]
		for _, vm := range obj.Versions {
			if vm.Deleted {
				kept = append(kept, vm)
				continue
			}
			p := s.dataPath(key, vm.VersionID)
			if _, err := os.Stat(p); err != nil {
				s.log.Warn("recovery: dropping version with missing data", "key", key, "version", vm.VersionID)
				continue
			}
			referenced[p] = true
			kept = append(kept, vm)
		}
		obj.Versions = kept
	}
	// 2. 删除无元数据引用的数据文件（含崩溃残留的临时文件）。
	err := filepath.Walk(s.dataDir(), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !referenced[path] {
			s.log.Info("recovery: removing unreferenced data file", "path", path)
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return err
	}
	// 3. 上传目录：仅保留 initiated 状态会话的分片，其余全部清除。
	if err := s.recoverUploadsLocked(); err != nil {
		return err
	}
	return s.persistLocked()
}
