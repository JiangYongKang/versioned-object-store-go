package objectstore

import (
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

// GCReport 描述一次空间回收的结果。
type GCReport struct {
	RemovedDataFiles  int
	RemovedUploadDirs int
	ReclaimedBytes    int64
}

// GC 回收崩溃残留与已终结上传的临时数据。
// 只删除无任何元数据引用的文件，因此不影响任何可见对象与版本；
// 正在被读取的版本其数据文件始终被元数据引用，回收期间读取可正常完成。
func (s *Store) GC() (GCReport, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if s.closed {
		return GCReport{}, errors.New("objectstore: store closed")
	}
	var rep GCReport

	// 1. 收集元数据引用的全部数据文件。
	referenced := map[string]bool{}
	for key, obj := range s.m.Objects {
		for _, vm := range obj.Versions {
			if !vm.Deleted {
				referenced[s.dataPath(key, vm.VersionID)] = true
			}
		}
	}
	// 2. 删除无引用的数据文件（含崩溃残留的临时文件）。
	err := filepath.Walk(s.dataDir(), func(path string, info os.FileInfo, err error) error {
		if err != nil || info.IsDir() {
			return err
		}
		if !referenced[path] {
			s.log.Info("gc: removing unreferenced data file", "path", path, "bytes", info.Size())
			rep.RemovedDataFiles++
			rep.ReclaimedBytes += info.Size()
			return os.Remove(path)
		}
		return nil
	})
	if err != nil {
		return rep, err
	}
	// 3. 清除已终结（completed/aborted）或未知上传会话的目录；
	//    initiated 会话内未登记的分片文件一并清除。
	entries, err := os.ReadDir(s.uploadsDir())
	if err != nil {
		return rep, err
	}
	for _, e := range entries {
		if !e.IsDir() {
			p := filepath.Join(s.uploadsDir(), e.Name())
			if fi, err := os.Stat(p); err == nil {
				rep.ReclaimedBytes += fi.Size()
			}
			s.log.Info("gc: removing stray file in uploads dir", "name", e.Name())
			if err := os.Remove(p); err != nil {
				return rep, err
			}
			continue
		}
		id := e.Name()
		um := s.m.Uploads[id]
		if um == nil || um.State != UploadInitiated {
			size := dirSize(s.uploadDir(id))
			s.log.Info("gc: removing terminal/unknown upload dir", "upload", id, "bytes", size)
			if err := os.RemoveAll(s.uploadDir(id)); err != nil {
				return rep, err
			}
			rep.RemovedUploadDirs++
			rep.ReclaimedBytes += size
			continue
		}
		files, err := os.ReadDir(s.uploadDir(id))
		if err != nil {
			return rep, err
		}
		for _, f := range files {
			var n int
			if _, err := fmt.Sscanf(f.Name(), "part-%d", &n); err == nil {
				if _, ok := um.Parts[n]; ok {
					continue
				}
			}
			p := filepath.Join(s.uploadDir(id), f.Name())
			if fi, err := os.Stat(p); err == nil {
				rep.ReclaimedBytes += fi.Size()
			}
			s.log.Info("gc: removing unregistered part file", "upload", id, "file", f.Name())
			if err := os.Remove(p); err != nil {
				return rep, err
			}
		}
	}
	s.log.Info("gc finished", "removedDataFiles", rep.RemovedDataFiles,
		"removedUploadDirs", rep.RemovedUploadDirs, "reclaimedBytes", rep.ReclaimedBytes)
	return rep, nil
}

func dirSize(dir string) int64 {
	var total int64
	filepath.Walk(dir, func(_ string, info os.FileInfo, err error) error {
		if err == nil && !info.IsDir() {
			total += info.Size()
		}
		return nil
	})
	return total
}
