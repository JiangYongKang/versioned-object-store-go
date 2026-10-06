package objectstore

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
)

// dirLock 持有数据目录的独占文件锁（flock），
// 保证同一目录同一时刻只能被一个使用者打开，进程退出后由内核自动释放。
type dirLock struct {
	f *os.File
}

// lockFileName 是目录锁文件名。
const lockFileName = "LOCK"

// acquireDirLock 尝试对 dir 加独占非阻塞锁。
// 目录不存在时自动创建；已被其他使用者持有时返回 *DirLockedError，
// 锁文件内容（持有方 pid）用于诊断。成功时写入当前进程 pid。
func acquireDirLock(dir string) (*dirLock, error) {
	if err := os.MkdirAll(dir, 0o755); err != nil {
		return nil, err
	}
	p := filepath.Join(dir, lockFileName)
	f, err := os.OpenFile(p, os.O_RDWR|os.O_CREATE, 0o644)
	if err != nil {
		return nil, err
	}
	err = syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if err != nil {
		// 读取持有方 pid 以便诊断（内容可能尚未写入或为旧值，尽力而为）。
		var pid int
		if b, rerr := os.ReadFile(p); rerr == nil {
			if n, perr := strconv.Atoi(strings.TrimSpace(string(b))); perr == nil {
				pid = n
			}
		}
		f.Close()
		if err == syscall.EWOULDBLOCK || err == syscall.EAGAIN {
			return nil, &DirLockedError{Dir: dir, HolderPID: pid}
		}
		return nil, err
	}
	// 持锁成功：记录 pid 供其他使用者诊断。
	if err := f.Truncate(0); err == nil {
		if _, err := f.WriteString(strconv.Itoa(os.Getpid()) + "\n"); err == nil {
			f.Sync()
		}
	}
	return &dirLock{f: f}, nil
}

// release 释放锁并关闭锁文件；进程异常退出时由内核自动释放。
func (l *dirLock) release() error {
	if l == nil || l.f == nil {
		return nil
	}
	err := syscall.Flock(int(l.f.Fd()), syscall.LOCK_UN)
	cerr := l.f.Close()
	l.f = nil
	if err != nil {
		return err
	}
	return cerr
}
