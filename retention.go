package objectstore

import (
	"errors"
	"os"
	"sort"
)

// RemovedVersion 描述保留回收中被移除（或暂缓移除）的一个版本。
type RemovedVersion struct {
	Key       string
	VersionID string
	Bytes     int64
}

// RetentionReport 描述一次按保留窗口回收历史版本的结果。
// ReclaimedBytes 为实际删除文件的字节数，可与磁盘占用变化对照。
type RetentionReport struct {
	Removed        []RemovedVersion // 已移除并回收磁盘的版本
	Deferred       []RemovedVersion // 因正在被读取而暂缓移除的版本（元数据与数据均保留）
	ReclaimedBytes int64            // 实际回收的字节数
}

func (r *RetentionReport) merge(o RetentionReport) {
	r.Removed = append(r.Removed, o.Removed...)
	r.Deferred = append(r.Deferred, o.Deferred...)
	r.ReclaimedBytes += o.ReclaimedBytes
}

// RetainVersions 对单个键只保留最近 keep 个版本（含当前版本与删除标记），
// 超出窗口的旧版本其元数据条目与数据文件一并回收。
// keep < 1 时按 1 处理（当前版本始终保留）。
// 正在被读取的版本数据不会被删除：对应版本本次暂缓回收并记入 Deferred。
func (s *Store) RetainVersions(key string, keep int) (RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return RetentionReport{}, errors.New("objectstore: store closed")
	}
	obj := s.m.Objects[key]
	if obj == nil {
		return RetentionReport{}, ErrNotFound
	}
	rep, err := s.retainObjLocked(key, obj, keep)
	if err != nil {
		return rep, err
	}
	s.log.Info("retention finished", "key", key, "keep", keep,
		"removed", len(rep.Removed), "deferred", len(rep.Deferred),
		"reclaimedBytes", rep.ReclaimedBytes)
	return rep, nil
}

// RetainAll 对整个存储的每个键只保留最近 keep 个版本，语义同 RetainVersions。
func (s *Store) RetainAll(keep int) (RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return RetentionReport{}, errors.New("objectstore: store closed")
	}
	keys := make([]string, 0, len(s.m.Objects))
	for k := range s.m.Objects {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	var rep RetentionReport
	for _, k := range keys {
		r, err := s.retainObjLocked(k, s.m.Objects[k], keep)
		rep.merge(r)
		if err != nil {
			return rep, err
		}
	}
	s.log.Info("retention finished", "scope", "all", "keep", keep,
		"keys", len(keys), "removed", len(rep.Removed), "deferred", len(rep.Deferred),
		"reclaimedBytes", rep.ReclaimedBytes)
	return rep, nil
}

// retainObjLocked 回收单个键超出保留窗口的版本，并在有变更时持久化元数据。
func (s *Store) retainObjLocked(key string, obj *objectMeta, keep int) (RetentionReport, error) {
	var rep RetentionReport
	if keep < 1 {
		keep = 1
	}
	if len(obj.Versions) <= keep {
		return rep, nil
	}
	excess := obj.Versions[:len(obj.Versions)-keep]
	kept := obj.Versions[len(obj.Versions)-keep:]
	remaining := make([]*versionMeta, 0, len(obj.Versions))
	changed := false
	for _, vm := range excess {
		if vm.Deleted {
			// 删除标记无数据文件，直接移除条目。
			s.log.Info("retention: removing old delete marker", "key", key, "version", vm.VersionID)
			rep.Removed = append(rep.Removed, RemovedVersion{Key: key, VersionID: vm.VersionID})
			changed = true
			continue
		}
		p := s.dataPath(key, vm.VersionID)
		if s.activeReads[p] > 0 {
			// 正在被读取：暂缓回收，元数据与数据都保留。
			s.log.Info("retention: deferring version with active reader", "key", key, "version", vm.VersionID)
			rep.Deferred = append(rep.Deferred, RemovedVersion{Key: key, VersionID: vm.VersionID, Bytes: vm.Size})
			remaining = append(remaining, vm)
			continue
		}
		fi, err := os.Stat(p)
		if err != nil {
			// 数据缺失属于损坏（恢复应已拦截），不静默抹除条目。
			remaining = append(remaining, vm)
			return rep, &CorruptionError{Versions: []CorruptVersion{{Key: key, VersionID: vm.VersionID, Reason: "data file missing during retention: " + err.Error()}}}
		}
		if err := os.Remove(p); err != nil {
			remaining = append(remaining, vm)
			return rep, err
		}
		s.log.Info("retention: removed old version", "key", key, "version", vm.VersionID, "bytes", fi.Size())
		rep.Removed = append(rep.Removed, RemovedVersion{Key: key, VersionID: vm.VersionID, Bytes: fi.Size()})
		rep.ReclaimedBytes += fi.Size()
		changed = true
	}
	obj.Versions = append(remaining, kept...)
	if changed {
		if err := s.persistLocked(); err != nil {
			return rep, err
		}
	}
	return rep, nil
}
