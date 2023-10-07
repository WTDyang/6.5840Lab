package raft

import (
	"bufio"
	"fmt"
	"os"
	"regexp"
	"testing"
)

type lockTypeEnum string

const (
	LOCK   lockTypeEnum = "LOCK"
	UNLOCK lockTypeEnum = "UNLOCK"
)

var lockSet = make(map[string]lockTypeEnum) // 用于存储所有未解锁的锁

// 检测锁是否被正确解锁
func checkLockUnlock(t *testing.T, line string) {
	pattern := `\[(LOCK)\] (\d{4}/\d{2}/\d{2} \d{2}:\d{2}:\d{2}\.\d+) (.+):(\d+), function: (.+) (LOCK|RLOCK)\[(-?\d+)\] (LOCK|UNLOCK)`
	re := regexp.MustCompile(pattern)
	matches := re.FindStringSubmatch(line)
	if len(matches) == 0 {
		return
	}
	logType, _, filename, lineno, funcname, lockType, id, operation := matches[1], matches[2], matches[3], matches[4], matches[5], matches[6], matches[7], matches[8]
	if logType != "LOCK" {
		t.Errorf("%s not a lock log", line)
		return
	}
	if lockType != "LOCK" && lockType != "RLOCK" {
		t.Errorf("%s:%s funcname:%s- Invalid lock type: %s", filename, lineno, funcname, lockType)
		return
	}
	fullID := fmt.Sprintf("%s[%s]", lockType, id)
	if operation == "LOCK" {
		if lockType, exist := lockSet[fullID]; exist {
			if lockType == UNLOCK {
				delete(lockSet, fullID)
			} else {
				t.Errorf("%s:%s - Duplicate lock ID \"%s\" LOCK", filename, lineno, fullID)
			}
		} else {
			lockSet[fullID] = LOCK
		}
	} else if operation == "UNLOCK" {
		if lockType, exist := lockSet[fullID]; exist {
			if lockType == LOCK {
				delete(lockSet, fullID)
			} else {
				t.Errorf("%s:%s - Duplicate lock ID \"%s\" UNLOCK", filename, lineno, fullID)
			}
		} else {
			lockSet[fullID] = UNLOCK
		}
	} else {
		t.Errorf("%s:%s - Invalid lock operation: %s", filename, lineno, operation)
	}
}

// 读取日志并检测锁的正确性
func TestParseLog(t *testing.T) {
	println("开始通过日志检测死锁")
	fileName := "log.log"
	file, err := os.Open(fileName)
	if err != nil {
		t.Errorf("Failed to open log file: %v", err)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	i := 0
	for scanner.Scan() {
		i++
		line := scanner.Text()
		//	println("line:", line[0], line[1:6])
		if len(line) > 0 && line[0] == '[' && line[1:6] == "LOCK]" {
			checkLockUnlock(t, line)
		}
	}
	if len(lockSet) > 0 {
		t.Errorf("There are locks without unlock\nlockSet:%v", lockSet)
	} else {
		fmt.Printf("经过检测，文件[%s] 共[%v]行未发现死锁", fileName, i)
	}
}
