package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.5840/labgob"
	"bytes"
	"io"
	"log"
	"os"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type STATE string

const (
	FOLLOWER  STATE = "FOLLOWER"
	CANDIDATE STATE = "CANDIDATE"
	LEADER    STATE = "LEADER"
)

// HEARTBEAT_INTERVAL 心跳间隔50ms
const HEARTBEAT_INTERVAL = 50 * time.Millisecond

var logger *log.Logger
var followLogger *log.Logger
var leaderLogger *log.Logger
var candidateLogger *log.Logger
var debugger *log.Logger //debugger会在控制台输出，用于关键打点，谨慎使用
var lockLogger *log.Logger

func init() {
	//初始化方法
	//日志系统初始化
	//writer, err := os.OpenFile("log.log", os.O_WRONLY|os.O_CREATE|os.O_APPEND, 0755) //追加模式
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
	logger = log.New(io.MultiWriter(writer), "[COMMON] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	followLogger = log.New(io.MultiWriter(writer), "[FOLLOWER] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	leaderLogger = log.New(io.MultiWriter(writer), "[LEADER] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	candidateLogger = log.New(io.MultiWriter(writer), "[CANDIDATE] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	//debugger = log.New(io.MultiWriter(writer, os.Stdout), "[DEBUG] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	debugger = log.New(io.MultiWriter(writer), "[DEBUG] ", log.Lshortfile|log.Ldate|log.Lmicroseconds)
	lockLogger = log.New(io.MultiWriter(writer), "[LOCK] ", log.Ldate|log.Lmicroseconds)
}

// LogEntry 日志实体结构
type LogEntry struct {
	Term    int         // 任期
	Command interface{} // 命令
}

// ApplyMsg as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//连续的日志条目
type ApplyMsg struct {
	CommandValid bool //CommandValid为true表示ApplyMsg包含新的提交的日志条目。
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        LogRWMutex          // Lock to protect shared access to this peer's state;change from Mutex to RWMutex,in order to increase efficiency
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm    int         //latest term server has seen (initialized to 0	on first boot, increases monotonically)
	votedFor       int         //candidateId that received vote in current	term (or null if none)
	logs           []LogEntry  //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1
	state          STATE       //enum for state(follower , candidate and leader)
	electionTimer  *time.Timer //timeout
	heartbeatTimer *time.Timer //heartbeatTimer

	commitIndex int           //index of the highest log entry know to bew committed(init to 0)
	lastApplied int           //index of the highest log entry applied to state machine(init to 0)
	nextIndex   []int         //for each server,index of the next log entry to send to that server(initialized to leader last log index +1)
	matchIndex  []int         //for each server,index of the highest log entry known to be replicated on server(initialized to 0,increases monotonically[单调递增])
	applyCh     chan ApplyMsg //chan to apply
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
}

// AppendEntriesArgs request for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int        //leader's term
	LeaderId     int        //so follower can redirect clients
	PrevLogIndex int        //index of log entry immediately preceding new ones
	PrevLogTerm  int        //term of prevLogIndex entry
	Entries      []LogEntry //log entries to store (empty for heartbeat;	may send more than one for efficiency)
	LeaderCommit int        //leader’s commitIndex

}

// AppendEntriesReply result for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int  //currentTerm,for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries Invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	logger.Printf("Node[%v] Term[%v] 接收到Node[%v] Term[%v]心跳", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term < rf.currentTerm {
		//logger.Printf("%d屈服于%d(1),term:(%d,%d)\n", args.LeaderId, rf.me, rf.currentTerm, args.Term)
		// Reply false if term < currentTerm
		//如果leader的term比某一个follower的term还小，说明有问题
		reply.Success = false
		//用于leader更新自己的term
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm
		// 更新状态
		logger.Printf("Node[%v] term[%v] 接收到心跳[%v],term:[%v],成为FOLLOWER\n", rf.me, args.Term, args.LeaderId, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	//只要接收到正常的心跳就可以重置计时器，更新任期信息
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(randomElectionTimeout())
	followLogger.Printf("Node[%v] term[%v] 心跳刷新", rf.me, args.Term)
	//follower 在prevLogIndex 位置没有entry
	if args.PrevLogIndex >= len(rf.logs) {
		reply.Term, reply.Success = rf.currentTerm, false
		followLogger.Printf("Node[%v] receives unexpected AppendEntriesRequest %v from Node[%v] because prevLogIndex %v >= logs.len %v", rf.me, args, args.LeaderId, args.PrevLogIndex, len(rf.logs))
		return
	}

	//在prevLogIndex位置entry的term与leader不同
	if args.PrevLogIndex >= 0 && args.PrevLogTerm != rf.logs[args.PrevLogIndex].Term {
		reply.Term, reply.Success = rf.currentTerm, false
		followLogger.Printf("Node [%v] receives unexpected AppendEntriesRequest %v from Node [%v] because PrevLogTerm %v != rf.logs[args.PrevLogIndex].Term %v", rf.me, args, args.LeaderId, args.PrevLogTerm, len(rf.logs))
		return
	}
	//日志复制
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + i + 1
		if index > len(rf.logs)-1 || rf.logs[index].Term < entry.Term {
			//之前没有这个索引位置，或者发生了冲突(保持term递增的趋势)
			if index <= rf.commitIndex {
				followLogger.Fatalf("Node[%v] 试图删除已提交的日志[%v] index[%v] commitIndex[%v]", rf.me, rf.logs, index, rf.commitIndex)
			}
			rf.logs = rf.logs[:index]
			//这里为什么不能直接添加，而是批量添加（打散处理后）：会导致在冲突或缺失的情况下无法正确维护日志的连续性，可能会产生不一致的状态
			rf.logs = append(rf.logs, append([]LogEntry{}, args.Entries[i:]...)...)
			followLogger.Printf("Node[%v] Term[%v] 追加日志，索引为:%v, 内容为%v", rf.me, rf.currentTerm, index, entry)
			break
		}
	}
	rf.persist()
	//leaderCommit > commitIndex，令 commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		lastCommitIndex := rf.commitIndex
		rf.commitIndex = min(args.LeaderCommit, len(rf.logs)-1)
		//提交日志
		var applyMsgs []ApplyMsg
		for i := lastCommitIndex + 1; i <= rf.commitIndex; i++ {
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i + 1, //+1是因为和test中从1开始计数保持一直
			}
			followLogger.Printf("Node[%v] Term[%v],准备提交日志 index[%v],ApplyMsg:%v", rf.me, rf.currentTerm, i, applyMsg)
			applyMsgs = append(applyMsgs, applyMsg)

		}
		go func(applyMsgs []ApplyMsg, rf *Raft) {
			//同步组装，异步发送
			for _, applyMsg := range applyMsgs {
				rf.applyCh <- applyMsg
			}
		}(applyMsgs, rf)
	}
	//设置返回值
	reply.Success = true
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A)
	//rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	//rf.mu.RUnlock()
	logger.Println(rf.me, "任期为：", term, "是否为leader：", isleader)
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	err := e.Encode(rf.currentTerm)
	if err != nil {
		return
	}
	err = e.Encode(rf.votedFor)
	if err != nil {
		return
	}
	err = e.Encode(rf.logs)
	if err != nil {
		return
	}
	data := w.Bytes()
	rf.persister.Save(data, []byte{})
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logs []LogEntry
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil {
		panic("readPersist decode fail")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.persist()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	//2A
	Term        int //candidate’s term
	CandidateId int //candidate requesting vote
	//2B
	LastLogIndex int //index of candidate’s last log entry (§5.4)
	LastLogTerm  int //term of candidate’s last log entry (§5.4)
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //currentTerm, for candidate to update itself
	VoteGranted bool //true means candidate received vote
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		// Reply false if term < currentTerm
		logger.Printf("Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为args.Term[%v] < rf.currentTerm[%v]", rf.me, rf.currentTerm, args.CandidateId, args.Term, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:set currentTerm = T, convert to follower
		logger.Printf("Node[%v] 更新为follow节点，因为args.Term[%v] > rf.currentTerm[%v]", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		//这里赋值为-1，下面判断是否为-1即可排除这种可能
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	//为了保证安全性，首先检验日志的合法性
	//候选人应当包含所有已提交的日志条目
	if len(rf.logs) > 0 {
		if rf.logs[len(rf.logs)-1].Term > args.LastLogTerm {
			//最后一条日志的任期号更大，则不投票，但是会根据term更新自己状态
			logger.Printf("Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为最后日志任期号[%v]比候选者最后日志任期号[%v]更大", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.logs[len(rf.logs)-1].Term, args.LastLogTerm)
			reply.Term = args.Term
			rf.votedFor = -1
			rf.persist()
			reply.VoteGranted = false
			return
		}
		if rf.logs[len(rf.logs)-1].Term == args.LastLogTerm && len(rf.logs)-1 > args.LastLogIndex {
			//日志条目更多，也不更新
			logger.Printf("Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为日志条目[%v]比候选者日志条目[%v]更多", rf.me, rf.currentTerm, args.CandidateId, args.Term, len(rf.logs)-1, args.LastLogIndex)
			reply.VoteGranted = false
			rf.votedFor = -1
			rf.persist()
			reply.Term = args.Term
			return
		}
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// If votedFor is null or candidateId, grant vote; otherwise reject
		//已投票
		logger.Printf("Node[%v] Term[%v] 已经投票给其他节点[%v],因此拒绝投票给Node[%v] Term[%v]", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote to candidate, reset election timer
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.votedFor = args.CandidateId
	rf.persist()
	logger.Printf("Node[%v] 投票给Node[%v]", rf.me, rf.votedFor)
	reply.VoteGranted = true
	reply.Term = rf.currentTerm
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//rpc to call the RequestVote
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//rpc to call the AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// Start the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	index := len(rf.logs) + 1
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	if !isLeader {
		//不是leader不处理，直接返回
		return index, term, isLeader
	}

	LogEntry := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, LogEntry)
	rf.persist()
	leaderLogger.Printf("Node[%v]传入命令[%v],index:%v,term:%v,isLeader:%v", rf.me, command, index, term, isLeader)
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	logger.Println(rf.me, "死掉了", rf)
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (2A)
		// Check if a leader election should be started.
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			logger.Printf("Node[%v] Term[%v]超时,身份是[%v]\n", rf.me, rf.currentTerm, rf.state)
			if rf.state == LEADER {
				//if is a leader:beak
				rf.mu.Unlock()
				break
			}
			rf.state = CANDIDATE
			rf.mu.Unlock()
			go rf.elections()
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		//ms := 50 + (rand.Int63() % 300)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
func (rf *Raft) appendLog(server int, args *AppendEntriesArgs) {
	//向请求中添加日志
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= 0 {
		args.PrevLogTerm = rf.logs[args.PrevLogIndex].Term
	}
	args.Entries = rf.logs[rf.nextIndex[server]:]
}

//heartbeat 同步发送心跳给指定的server
func (rf *Raft) heartbeat(server int) {
	//logger.Printf("%d->%d\n", rf.me, server)
	args := AppendEntriesArgs{Term: -1, LeaderCommit: rf.commitIndex, PrevLogTerm: -1, PrevLogIndex: -1, LeaderId: rf.me}
	reply := AppendEntriesReply{}
	rf.mu.Lock()
	if rf.state == LEADER {
		//最后一次检验，因为可能已经不是leader了但是拿到了锁，然后发送了错误的term请求
		args.Term = rf.currentTerm
	}
	//检查是否需要传入log
	//debugger.Printf("Node[%v] 检查是否需要向Node[%v] 传入日志，len(rf.logs)-1：[%v] >= rf.nextIndex[server]：[%v]", rf.me, server, len(rf.logs)-1, rf.nextIndex[server])
	rf.appendLog(server, &args)
	leaderLogger.Printf("Node[%v] term[%v] 向Node[%v] 追加日志,LeaderCommit[%v], nextIndex[server]:%v,len(logs):%v,len(entries):[%v],logs:[%v]", rf.me, rf.currentTerm, server, args.LeaderCommit, rf.nextIndex[server], len(rf.logs), len(args.Entries), args.Entries)
	rf.mu.Unlock()
	if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
		//leaderLogger.Printf("Node[%d]心跳丢失->Node[%d]\n", rf.me, server)
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		leaderLogger.Printf("Node[%v] Term[%v] 变更为follower节点，因为reply.Term[%v] > rf.currentTerm[%v]", rf.me, rf.currentTerm, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.mu.Unlock()
		return
	}
	//发生了log的复制
	if reply.Success {
		leaderLogger.Printf("Node[%v] Term[%v] 向server[%v] 日志复制成功,log:%v", rf.me, rf.currentTerm, server, args.Entries)
		//日志复制成功
		//新的nextIndex为原来的nextIndex[server](PrevLogIndex+1)+len(args.Entries)
		//由于是个高并发问题，可能这里已经被更新了
		rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		//查找是否存在可以提交的日志N(过半matchIndex[i] >= N)
		//这里可以快排的方法快速求出中位数，再看中位数是否符合要求[nLog(n)],为避免增加复杂度，暂且采用简单的方法
		N := len(rf.logs) - 1 //最大不会超过日志的最大索引
		for ; N > rf.commitIndex; N-- {
			//最小也要比上一次提交的小
			cnt := 1
			for index, matchIndex := range rf.matchIndex {
				//	logger.Printf("DEBUG Node[%v] Term[%v] Index[%v] matchIndex[%v]", rf.me, rf.currentTerm, index, matchIndex)
				if index == rf.me {
					continue
				}
				if matchIndex >= N {
					cnt++
				}
			}
			if cnt > len(rf.peers)/2 {
				leaderLogger.Printf("Node[%v] Term[%v] Find max N[%v],commitIndex[%v]", rf.me, rf.currentTerm, N, rf.commitIndex)
				break
			}
		}
		var applyMsgs []ApplyMsg
		for i := rf.commitIndex + 1; i <= N; i++ {
			//提交日志
			applyMsg := ApplyMsg{
				CommandValid: true,
				Command:      rf.logs[i].Command,
				CommandIndex: i + 1, //+1是因为和test中从1开始计数保持一直
			}
			leaderLogger.Printf("Node[%v] Term[%v],准备提交日志 index[%v],ApplyMsg:%v", rf.me, rf.currentTerm, i, applyMsg)
			applyMsgs = append(applyMsgs, applyMsg)
		}
		go func(applyMsgs []ApplyMsg, rf *Raft) {
			//同步组装，异步发送
			for _, applyMsg := range applyMsgs {
				rf.applyCh <- applyMsg
			}
		}(applyMsgs, rf)
		rf.commitIndex = N
	} else {
		//日志复制失败
		rf.nextIndex[server] = min(args.PrevLogIndex, rf.nextIndex[server])
		leaderLogger.Printf("Node[%v] Term[%v] 日志复制失败，更新Node[%v] 的nextIndex为[%v]", rf.me, rf.currentTerm, server, rf.nextIndex[server])
	}
	rf.mu.Unlock()
	return
}

//Execute the activities of the leader
//leader的状态转化
//定期发送心跳广播
func (rf *Raft) startLeaderAction() {
	leaderLogger.Printf("Node[%v]号成为leader 状态为:%v\n", rf.me, rf)
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	nextIndexInit := len(rf.logs)
	//nextIndex 初始化为leader的log的最后一条索引+1
	//matchIndex 初始化为0
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = nextIndexInit
		rf.matchIndex[i] = 0
	}
	rf.mu.Unlock()
	//rf.electionTimer.Stop()
	//匿名函数：异步广播
	broadcast := func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			go rf.heartbeat(i)
		}
	}
	//循环广播
	for rf.killed() == false {
		rf.mu.RLock()
		if rf.state != LEADER {
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		//logger.Printf("%d - %d - %d\n", rf.me, rf.currentTerm, rf.state)
		broadcast()
		rf.heartbeatTimer.Reset(HEARTBEAT_INTERVAL)
		<-rf.heartbeatTimer.C
	}
}

//elections the leader
func (rf *Raft) elections() {
	rf.mu.Lock()
	rf.currentTerm++    // Increment currentTerm
	rf.votedFor = rf.me // Vote for self
	rf.persist()
	rf.electionTimer.Reset(randomElectionTimeout()) // Reset election timer
	rf.mu.Unlock()

	requestVoteArg := RequestVoteArgs{LastLogTerm: -1, LastLogIndex: -1, CandidateId: rf.me}
	rf.mu.RLock()
	if len(rf.logs) > 0 {
		requestVoteArg.LastLogTerm = rf.logs[len(rf.logs)-1].Term
		requestVoteArg.LastLogIndex = len(rf.logs) - 1
	}
	requestVoteArg.Term = rf.currentTerm
	rf.mu.RUnlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers { // Send RequestVote RPCs to all other servers
		if i == rf.me { // in PARALLEL
			continue
		}
		//异步拉票
		candidateLogger.Printf("Node[%v] Term[%v] 拉票于[%v]", rf.me, rf.currentTerm, i)
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &requestVoteArg, &reply); !ok {
				//异常情况也要算成失败
				//logger.Println("拉票异常", rf.me, "->", i)
				voteCh <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				candidateLogger.Printf("Node[%d]拉票于Node[%d]失败，变为follower\n", rf.me, i)
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.persist()
				//voteCh <- reply.VoteGranted
				rf.mu.Unlock()
				return
			}
			candidateLogger.Printf("Node[%d]拉票于Node[%d]成功,结果为[%v]，任期为[%d]\n", rf.me, i, reply.VoteGranted, rf.currentTerm)
			rf.mu.Unlock()
			voteCh <- reply.VoteGranted
		}(i)
	}

	voteCnt := 1
	voteGrantedCnt := 1
	for voteGranted := range voteCh {
		//计数+1
		voteCnt++
		rf.mu.RLock()
		state := rf.state
		rf.mu.RUnlock()
		if state != CANDIDATE {
			break
		}
		if voteGranted {
			voteGrantedCnt++
		}
		if voteGrantedCnt > len(rf.peers)/2 {
			// gain over a half votes, switch to leader
			rf.mu.Lock()
			rf.state = LEADER
			rf.mu.Unlock()
			//开始leader的动作(发送心跳)
			go rf.startLeaderAction()
			break
		}

		if voteCnt == len(rf.peers) {
			// election completed without getting enough votes, break
			break
		}
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.commitIndex = -1 //表示从未提交过日志
	rf.applyCh = applyCh
	rf.state = FOLLOWER //初始化为follower节点

	rf.mu = LogRWMutex{raftId: me}
	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(randomElectionTimeout())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	logger.Println(me, "初始化完成", rf)
	go rf.ticker()
	return rf
}
