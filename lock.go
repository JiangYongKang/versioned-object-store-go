package objectstore

import (
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"time"
)

// 目录独占：Open 时对 Dir/LOCK 加排他 flock（非阻塞），
// 同一进程重复打开与多个进程同时打开都会被拒绝；
// LOCK 文件内记录持有者信息，被拒绝的一方能据此定位占用方。
// 持有者 Close 或进程退出（内核自动释放 flock）后，后来者才能打开。

// dirLock 是已持有的目录独占锁。
type dirLock struct {
	f   *os.File
	dir string
}

func lockPath(dir string) string { return filepath.Join(dir, "LOCK") }

// acquireDirLock 尝试独占锁定目录；被占用时返回 *LockError（含持有者信息）。
func acquireDirLock(dir string) (*dirLock, error) {
	p := lockPath(dir)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		// 读取持有者信息用于诊断；文件可能尚未写入或被截断中，容忍失败。
		holder := ""
		if b, rerr := os.ReadFile(p); rerr == nil {
			holder = strings.TrimSpace(string(b))
		}
		f.Close()
		return nil, &LockError{Dir: dir, Holder: holder}
	}
	// 记录持有者信息，供被拒绝方读取。
	hostname, _ := os.Hostname()
	info := fmt.Sprintf("pid=%d host=%s since=%s", os.Getpid(), hostname, time.Now().Format(time.RFC3339))
	if err := f.Truncate(0); err == nil {
		if _, err := f.WriteAt([]byte(info), 0); err != nil {
			// 持有者信息写失败不影响锁本身。
			_ = err
		}
	}
	return &dirLock{f: f, dir: dir}, nil
}

// release 释放目录锁。不删除 LOCK 文件：避免与正在打开该文件的竞争者产生竞态，
// 残留文件中的持有者信息仅作诊断参考，锁状态以 flock 为准。
func (l *dirLock) release() {
	if l == nil || l.f == nil {
		return
	}
	syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	l.f.Close()
	l.f = nil
}
