package raft

import (
	"crypto/rand"
	"encoding/binary"
	"sync"
)

type LogRWMutex struct {
	mu  sync.RWMutex
	uid int64
}

func (l *LogRWMutex) Lock() {
	uid := generateUID()
	l.mu.Lock()
	l.uid = uid
	logger.Printf("LOCK[%v] LOCK", uid)
}

func (l *LogRWMutex) Unlock() {
	logger.Printf("LOCK[%v] UNLOCK", l.uid)
	l.mu.Unlock()
}
func (l *LogRWMutex) RLock() {
	uid := generateUID()
	l.mu.RLock()
	l.uid = uid
	logger.Printf("LOCK[%v] ReadLOCK", uid)
}
func (l *LogRWMutex) RUnlock() {
	logger.Printf("LOCK[%v] ReadUNLOCK", l.uid)
	l.mu.RUnlock()
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
