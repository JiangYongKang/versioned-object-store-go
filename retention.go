package objectstore

import (
	"errors"
	"fmt"
	"os"
)

// KeyRetention 描述单个键在一次保留回收中的结果。
type KeyRetention struct {
	Key               string
	RemovedVersionIDs []string // 被回收的版本（按创建顺序）
	ReclaimedBytes    int64    // 实际删除的数据文件字节数，可与磁盘占用核对
}

// RetentionReport 描述一次保留窗口回收的结果。
type RetentionReport struct {
	Keys            []KeyRetention
	RemovedVersions int   // 被回收的版本总数（含删除标记）
	ReclaimedBytes  int64 // 实际回收的磁盘字节数
}

// RetainVersions 对单个键只保留最近 keep 个版本（含当前版本与删除标记），
// 超出窗口的旧版本条目连同其数据文件一并回收。
// keep 必须 >= 1，因此当前版本（无论是否为删除标记）始终被保留。
// 与并发读写交错是安全的：正在进行的读取已持有文件描述符，
// 数据文件被回收后读取仍可完成；新的读取只能看到保留窗口内的版本。
func (s *Store) RetainVersions(key string, keep int) (RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return RetentionReport{}, errors.New("objectstore: store closed")
	}
	if keep < 1 {
		return RetentionReport{}, fmt.Errorf("objectstore: keep must be >= 1, got %d", keep)
	}
	obj := s.m.Objects[key]
	if obj == nil {
		return RetentionReport{}, ErrNotFound
	}
	rep := RetentionReport{}
	changed, err := s.retainKeyLocked(key, obj, keep, &rep)
	if err != nil {
		return rep, err
	}
	if changed {
		if err := s.persistLocked(); err != nil {
			return rep, err
		}
	}
	s.log.Info("retention finished", "key", key, "keep", keep,
		"removedVersions", rep.RemovedVersions, "reclaimedBytes", rep.ReclaimedBytes)
	return rep, nil
}

// RetainAll 对整个存储的每个键只保留最近 keep 个版本，语义同 RetainVersions。
func (s *Store) RetainAll(keep int) (RetentionReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return RetentionReport{}, errors.New("objectstore: store closed")
	}
	if keep < 1 {
		return RetentionReport{}, fmt.Errorf("objectstore: keep must be >= 1, got %d", keep)
	}
	rep := RetentionReport{}
	changed := false
	for key, obj := range s.m.Objects {
		c, err := s.retainKeyLocked(key, obj, keep, &rep)
		if err != nil {
			return rep, err
		}
		changed = changed || c
	}
	if changed {
		if err := s.persistLocked(); err != nil {
			return rep, err
		}
	}
	s.log.Info("retention finished", "scope", "all", "keep", keep,
		"removedVersions", rep.RemovedVersions, "reclaimedBytes", rep.ReclaimedBytes)
	return rep, nil
}

// retainKeyLocked 裁剪单个键的版本列表并删除超窗版本的数据文件，返回元数据是否有变化。
func (s *Store) retainKeyLocked(key string, obj *objectMeta, keep int, rep *RetentionReport) (bool, error) {
	n := len(obj.Versions)
	if n <= keep {
		return false, nil
	}
	excess := obj.Versions[:n-keep]
	kr := KeyRetention{Key: key}
	for _, vm := range excess {
		kr.RemovedVersionIDs = append(kr.RemovedVersionIDs, vm.VersionID)
		if vm.Deleted {
			continue // 删除标记无数据文件
		}
		p := s.dataPath(key, vm.VersionID)
		fi, err := os.Stat(p)
		if err != nil {
			// 数据已缺失：打开时的校验应已拦截，此处防御性报错而非静默跳过。
			return false, fmt.Errorf("objectstore: retention cannot stat data for key %s version %s: %w", key, vm.VersionID, err)
		}
		if err := os.Remove(p); err != nil {
			return false, err
		}
		kr.ReclaimedBytes += fi.Size()
		s.log.Info("retention: reclaimed version", "key", key, "version", vm.VersionID, "bytes", fi.Size())
	}
	obj.Versions = obj.Versions[n-keep:]
	rep.Keys = append(rep.Keys, kr)
	rep.RemovedVersions += len(kr.RemovedVersionIDs)
	rep.ReclaimedBytes += kr.ReclaimedBytes
	return true, nil
}
