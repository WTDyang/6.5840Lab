package raft

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"
)

// Debugging
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

//baseTimeout:最小超时时间
const baseTimeout = 180

//randomElectionTimeout:随机超时时间（180-360ms）
func randomElectionTimeout() time.Duration {
	//To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms)
	//由于存在丢包，这里这是为180-360ms
	extendedTimeout, err := rand.Int(rand.Reader, big.NewInt(180))
	if err == nil {

	}
	return time.Duration(baseTimeout+extendedTimeout.Int64()) * time.Millisecond
}

func min(a int, b int) int {
	if a < b {
		return a
	}
	return b
}

func max(a int, b int) int {
	if a > b {
		return a
	}
	return b
}
