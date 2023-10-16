package raft

import (
	"crypto/rand"
	"io"
	"log"
	"math/big"
	"os"
	"strconv"
	"strings"
	"time"
)

type logTopic string

const (
	common    = "[COMMON]    "
	follower  = "[FOLLOWER]  "
	leader    = "[LEADER]    "
	candidate = "[CANDIDATE] "
	debug     = "[DEBUG]     "
	lock      = "[LOCK]      "
	rpc       = "[RPC]       "
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
	logger = log.New(io.MultiWriter(os.Stdout), "", 0)
}

// Debugging
const Debug = false

func DPrintf(topic logTopic, format string, a ...interface{}) (n int, err error) {
	if Debug {
		currentTime := time.Now()
		timeStr := currentTime.Format("2006-01-02 15:04:05")
		builder := strings.Builder{}
		builder.WriteString(string(topic))
		builder.WriteString(timeStr)
		builder.WriteString("  Stamp:")
		builder.WriteString(strconv.FormatInt(currentTime.UnixMicro(), 10))
		builder.WriteString("  ")
		if topic != lock {
			builder.WriteString(getFileLocation())
		}
		builder.WriteString(format)
		logFormat := builder.String()
		logger.Printf(logFormat, a...)
	}
	return
}

//baseTimeout:最小超时时间(110*4 + 27 + 23) 110 : 心跳间隔，27为网络short delay，可以接收失败三次 给23秒处理时间
const baseTimeout = 490

//randomElectionTimeout:随机超时时间（490-810ms）
func randomElectionTimeout() time.Duration {
	//To prevent split votes in the first place, election timeouts are chosen randomly from a fixed interval (e.g., 150–300ms)
	//由于存在丢包，这里这是为250-600ms
	extendedTimeout, err := rand.Int(rand.Reader, big.NewInt(320))
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
