package raft

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"path/filepath"
	"runtime"
	"sync"
)

type LogRWMutex struct {
	mu     sync.RWMutex
	uid    int64
	raftId int
}

func (l *LogRWMutex) Lock() {
	uid := generateUID()
	lockLogger.Printf("%s Node[%v] LOCK[%v] 尝试获取锁", getFileLocation(), l.raftId, uid)
	l.mu.Lock()
	l.uid = uid
	lockLogger.Printf("%s Node[%v] LOCK[%v] LOCK", getFileLocation(), l.raftId, uid)
}

func (l *LogRWMutex) Unlock() {
	lockLogger.Printf("%s Node[%v] LOCK[%v] UNLOCK", getFileLocation(), l.raftId, l.uid)
	l.mu.Unlock()
}
func (l *LogRWMutex) RLock() {
	uid := generateUID()
	l.mu.RLock()
	l.uid = uid
	lockLogger.Printf("%s Node[%v] RLOCK[%v] LOCK", getFileLocation(), l.raftId, uid)
}
func (l *LogRWMutex) RUnlock() {
	lockLogger.Printf("%s Node[%v] RLOCK[%v] UNLOCK", getFileLocation(), l.raftId, l.uid)
	l.mu.RUnlock()
}
func getFileLocation() string {
	pc, file, line, ok := runtime.Caller(2)
	if !ok {
		file = "unknown"
		line = 0
	}
	funcName := runtime.FuncForPC(pc).Name()
	// 获取短文件名
	shortFile := filepath.Base(file)
	return fmt.Sprintf("%s:%d, function: %s", shortFile, line, funcName)
}
func generateUID() int64 {
	// 生成 8 字节的随机字节序列
	b := make([]byte, 8)
	_, err := rand.Read(b)
	if err != nil {
		panic(err)
	}

	// 将字节序列转换为 int64 类型的整数
	uid := int64(binary.BigEndian.Uint64(b))

	return uid
}
