package raft

import (
	"crypto/rand"
	"io"
	"log"
	"math/big"
	"os"
	"time"
)

type logTopic string

const (
	common    = "[COMMON]\t"
	follower  = "[FOLLOWER]\t"
	leader    = "[LEADER]\t"
	candidate = "[CANDIDATE]\t"
	debug     = "[DEBUG]\t"
	lock      = "[LOCK]\t"
	rpc       = "[RPC]\t"
)

var logger *log.Logger

func init() {
	writer, err := os.OpenFile("log.log", os.O_WRONLY|os.O_TRUNC|os.O_CREATE, 0755) //覆盖
	if err != nil {
		log.Fatalf("create file log.txt failed: %v", err)
	}
	title := "|--------------------------------------------------------------------------------------------|\n" +
		"|--------------------" + time.Now().String() + "---------------------|\n" +
		"|--------------------------------------------------------------------------------------------|\n"
	_, err = writer.WriteString(title)
	if err != nil {
		log.Fatalf("write the title failed: %v", err)
	}
	logger = log.New(io.MultiWriter(writer), "", log.Lshortfile|log.Ldate|log.Lmicroseconds)
}

// Debugging
const Debug = true

func DPrintf(topic logTopic, format string, a ...interface{}) (n int, err error) {
	if Debug {
		format = string(topic) + format
		logger.Printf(format, a...)
	}
	return
}

//baseTimeout:最小超时时间
const baseTimeout = 250

//randomElectionTimeout:随机超时时间（250-600ms）
func randomElectionTimeout() time.Duration {
	//To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms)
	//由于存在丢包，这里这是为250-600ms
	extendedTimeout, err := rand.Int(rand.Reader, big.NewInt(350))
	if err != nil {
		return baseTimeout * time.Millisecond
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
