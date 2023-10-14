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
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

const (
	FOLLOWER  int32 = 0
	CANDIDATE int32 = 1
	LEADER    int32 = 2
)

// HEARTBEAT_INTERVAL 心跳间隔105ms
const HEARTBEAT_INTERVAL = 105 * time.Millisecond

// LogEntry 日志实体结构
type LogEntry struct {
	Term    int         // 任期
	Index   int         //接受时的索引
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
	state          int32       //enum for state(follower , candidate and leader)
	electionTimer  *time.Timer //timeout
	heartbeatTimer *time.Timer //heartbeatTimer

	commitIndex int           //index of the highest log entry know to bew committed(init to 0)
	lastApplied int           //index of the highest log entry applied to state machine(init to 0)
	nextIndex   []int         //for each server,index of the next log entry to send to that server(initialized to leader last log index +1)
	matchIndex  []int         //for each server,index of the highest log entry known to be replicated on server(initialized to 0,increases monotonically[单调递增])
	applyCh     chan ApplyMsg //chan to apply
	commitFlag  chan struct{} //用来触发提交日志

	logNum            int           //日志的总数
	lastIncludedIndex int           //已压缩的日志最后一条的索引
	lastIncludedTerm  int           //已压缩日志最后一条的term
	snapshot          []byte        //快照包
	snapshotFlag      chan ApplyMsg //用来触发提交快照包
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
	Term                int  //currentTerm,for leader to update itself
	Success             bool //true if follower contained entry matching prevLogIndex and prevLogTerm
	LastIncludeLogIndex int  //最后一条日志的Index，如果之前的日志发生了快照提交，则使用这个变量重置next

	Conflict         bool //true if conflict happened
	ConflictLogTerm  int  //the term of log which is conflict
	ConflictLogIndex int  //the first log which one it's term equals to conflict one
}

func (rf *Raft) getLog(index int) LogEntry {
	n := index - rf.lastIncludedIndex
	if n == 0 {
		DPrintf(common, "Node[%v] Term[%v] 的日志[%v] 已被裁剪，返回日志不包含日志内容", rf.me, rf.currentTerm, index)
		DPrintf(debug, "调用位置为：%v", getFileLocation())
		return LogEntry{
			Term:    rf.lastIncludedTerm,
			Index:   rf.lastIncludedIndex,
			Command: nil,
		}
	}
	//println(len(rf.logs), index, rf.lastIncludedIndex)
	return rf.logs[n]
}
func (rf *Raft) getLastLogIndex() int {
	return rf.logNum
}

// AppendEntries Invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	DPrintf(common, "Node[%v] Term[%v] 接收到Node[%v] Term[%v]心跳", rf.me, rf.currentTerm, args.LeaderId, args.Term)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term < rf.currentTerm {
		//DPrintf(common,"%d屈服于%d(1),term:(%d,%d)\n", args.LeaderId, rf.me, rf.currentTerm, args.Term)
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
		DPrintf(common, "Node[%v] term[%v] 接收到心跳[%v],term:[%v],成为FOLLOWER\n", rf.me, args.Term, args.LeaderId, rf.currentTerm)
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	//只要接收到正常的心跳就可以重置计时器，更新任期信息
	reply.Term = rf.currentTerm
	rf.electionTimer.Reset(randomElectionTimeout())
	DPrintf(follower, "Node[%v] term[%v] 心跳刷新", rf.me, args.Term)

	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Term, reply.LastIncludeLogIndex, reply.Success = rf.currentTerm, rf.lastIncludedIndex, true
		DPrintf(follower, "Node[%v] 选择忽略Node[%v]的日志 因为PrevLogIndex[%v] < lastIncludedIndex[%v] 说明是个过时RPC", rf.me, args.LeaderId, args.PrevLogIndex, rf.lastIncludedIndex)
		return
	}

	//follower 在prevLogIndex 位置没有entry
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictLogTerm = -1                        //设置成非法数字，这样强制主机从reply.ConflictLogIndex开始同步
		reply.ConflictLogIndex = rf.getLastLogIndex() + 1 //(总书为logNum 那么最后一个位置的索引即使rf.logNum 那么下一个位置的索引就是rf.logNum+1)
		reply.Conflict = true
		DPrintf(follower, "Node[%v] 拒绝接收Node[%v]的日志 因为prevLogIndex 位置没有entry prevLogIndex[%v] >= rf.logNum[%v]", rf.me, args.LeaderId, args.PrevLogIndex, rf.logNum)
		return
	}

	//在prevLogIndex位置entry的term与leader不同
	if args.PrevLogIndex > 0 && args.PrevLogTerm != rf.getLog(args.PrevLogIndex).Term {
		reply.Term, reply.Success = rf.currentTerm, false
		reply.ConflictLogTerm = rf.getLog(args.PrevLogIndex).Term
		reply.ConflictLogIndex = args.PrevLogIndex
		for i := args.PrevLogIndex; i > rf.commitIndex && rf.getLog(i).Term == rf.getLog(args.PrevLogIndex).Term; i-- {
			reply.ConflictLogIndex = i //设置成第一个符合条件的值（将会是被覆盖掉的）
		}
		reply.Conflict = true
		DPrintf(follower, "Node[%v] 拒绝接收Node[%v]的日志 因为在prevLogIndex位置entry的term与leader不同 PrevLogTerm[%v] != rf.getLog(args.PrevLogIndex).Term[%v]", rf.me, args.LeaderId, args.PrevLogTerm, rf.getLog(args.PrevLogIndex).Term)
		return
	}
	//日志复制
	for i, entry := range args.Entries {
		index := args.PrevLogIndex + 1 + i
		if index > rf.getLastLogIndex() || rf.getLog(index).Term != entry.Term {
			//之前没有这个索引位置，或者发生了冲突(保持强一致性原则，只要冲突就与leader保持一直)
			if index <= rf.commitIndex {
				DPrintf(follower, "Node[%v] 试图删除已提交的日志[%v] index[%v] commitIndex[%v]", rf.me, rf.logs, index, rf.commitIndex)
			}
			lastLogNum := len(rf.logs)
			rf.logs = rf.logs[:index-rf.lastIncludedIndex]
			//这里为什么不能直接添加，而是批量添加（打散处理后）：会导致在冲突或缺失的情况下无法正确维护日志的连续性，可能会产生不一致的状态
			rf.logs = append(rf.logs, append([]LogEntry{}, args.Entries[i:]...)...)
			rf.logNum = rf.logNum + len(rf.logs) - lastLogNum
			DPrintf(follower, "Node[%v] Term[%v] 追加来自Node[%v]的日志，索引为:%v, 内容为%v", rf.me, rf.currentTerm, args.LeaderId, index, entry)
			break
		}
	}
	rf.persist()
	//leaderCommit > commitIndex，令 commitIndex = min(leaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.persist()
		if rf.lastApplied != rf.commitIndex {
			DPrintf(follower, "Node[%v] Term[%v] 准备提交日志[%v] to [%v]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
			go func() {
				rf.commitFlag <- struct{}{} //开启一次提交检查
			}()
		}
	}
	//设置返回值
	reply.Success = true
}

//顺序提交日志
func (rf *Raft) commandLog() {
	//只要没有被回收就一直提交日志
	for !rf.killed() {
		select {
		case <-rf.commitFlag:
			//启动一次提交检查
			var applyMsgs []ApplyMsg
			rf.mu.Lock()
			DPrintf(common, "Node[%v] Term[%v] 开启一次提交检查 lastApplied[%v] CommitIndex[%v]", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				//println(rf.me, i, rf.lastIncludedIndex, rf.lastApplied, rf.commitIndex)
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.getLog(i).Command,
					CommandIndex: i,
				}
				applyMsgs = append(applyMsgs, applyMsg)
			}
			rf.lastApplied = rf.commitIndex
			rf.mu.Unlock()
			for _, applyMsg := range applyMsgs {
				rf.applyCh <- applyMsg
				DPrintf(common, "Node[%v] Term[%v] state[%v],已提交日志 ApplyMsg:%v", rf.me, rf.currentTerm, rf.state, applyMsg)
			}
		}
	}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.RLock()
	defer rf.mu.RUnlock()
	var term int
	var isleader bool
	// Your code here (2A)
	//rf.mu.RLock()
	term = rf.currentTerm
	isleader = rf.state == LEADER
	//rf.mu.RUnlock()
	DPrintf(common, "Node[%v] 任期为[%v] 身份为[%v]", rf.me, term, rf.state)
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
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.lastIncludedIndex) != nil || e.Encode(rf.lastIncludedTerm) != nil || e.Encode(rf.logNum) != nil || e.Encode(rf.commitIndex) != nil || e.Encode(rf.lastApplied) != nil {
		return
	}
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	DPrintf(common, "%v Node[%v] writePersist lastIncludedIndex[%v] raft:%v", getFileLocation(), rf.me, rf.lastIncludedIndex, rf)
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
	var lastIncludedIndex int
	var lastIncludedTerm int
	var logNum int
	var commitIndex int
	var lastApplied int
	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&logs) != nil || d.Decode(&lastIncludedIndex) != nil || d.Decode(&lastIncludedTerm) != nil || d.Decode(&logNum) != nil || d.Decode(&commitIndex) != nil || d.Decode(&lastApplied) != nil {
		panic("readPersist decode fail")
	}
	rf.currentTerm = currentTerm
	rf.votedFor = votedFor
	rf.logs = logs
	rf.lastIncludedIndex = lastIncludedIndex
	rf.lastIncludedTerm = lastIncludedTerm
	rf.logNum = logNum
	DPrintf(common, "Node[%v] readPersist raft:%v", rf.me, rf)
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if index <= rf.lastIncludedIndex {
		return
	}
	rf.snapshot = snapshot
	lastIncludeIndex := rf.lastIncludedIndex
	delEntries := []LogEntry{}
	for cutIndex, val := range rf.logs {
		if val.Index == index {
			rf.lastIncludedIndex = index
			rf.lastIncludedTerm = val.Term
			//为了保证被gc
			delEntries = rf.logs[:cutIndex+1]
			rf.logs = append([]LogEntry{{Term: rf.lastIncludedTerm}}, rf.logs[cutIndex+1:]...)
			break
		}
	}
	DPrintf(common, "Node[%v] Term[%v] state[%v] 创建快照,压缩日志len[%v]: %v from[%v](lastIncludedIndex+1) to[%v] LogNum[%v] len(logs)[%v] logs:%v", rf.me, rf.currentTerm, rf.state, len(delEntries), delEntries, lastIncludeIndex+1, index, rf.logNum, len(rf.logs), rf.logs)
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	if e.Encode(rf.currentTerm) != nil || e.Encode(rf.votedFor) != nil || e.Encode(rf.logs) != nil || e.Encode(rf.lastIncludedIndex) != nil || e.Encode(rf.lastIncludedTerm) != nil {
		return
	}
	data := w.Bytes()
	rf.persister.Save(data, snapshot)
}
func (rf *Raft) ApplySnapshot(args *InstallSnapshotArgs) {
	rf.mu.RLock()
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		rf.mu.RUnlock()
		return
	}
	if args.LastIncludedIndex == rf.lastIncludedIndex && args.LastIncludedTerm == rf.lastIncludedTerm {
		rf.mu.RUnlock()
		return
	}
	msg := ApplyMsg{
		CommandValid:  false,
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	rf.mu.RUnlock()
	rf.applyCh <- msg
	DPrintf(common, "Node[%v] Term[%v] state[%v],已提交快照包 ApplyMsg:%v", rf.me, rf.currentTerm, rf.state, msg)
	rf.mu.Lock()
	//日志压缩
	rf.snapshot = args.Data
	i := 1
	flag := true
	for ; i < len(rf.logs); i++ {
		if rf.logs[i].Index == args.LastIncludedIndex && rf.logs[i].Term <= args.Term {
			//找到提交位置
			flag = false
			rf.lastIncludedTerm = args.LastIncludedTerm
			rf.lastIncludedIndex = args.LastIncludedIndex
			rf.logs = append([]LogEntry{{Term: rf.lastIncludedTerm}}, rf.logs[i+1:]...)
			rf.lastApplied = args.LastIncludedIndex
			rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
			//压缩过的日志任务已经被提交了，所以提交位置也要增加
			DPrintf(follower, "Node[%v] Term[%v] 截断压缩 日志压缩后lastIncludedTerm[%v] lastIncludedIndex[%v] LogNum[%v] logs:%v", rf.me, rf.currentTerm, rf.lastIncludedTerm, rf.lastIncludedIndex, rf.logNum, rf.logs)
			rf.persister.SaveSnapshot(rf.snapshot)
			rf.persist()
			rf.mu.Unlock()
			DPrintf(common, "Node[%v] Term[%v] lastApplied[%v] 变更为[%v]", rf.me, rf.currentTerm, rf.lastApplied, rf.lastIncludedIndex)
			break
		}
	}
	if flag {
		//没有找到相同位置，说明落后太多了，新的日志还没有来得及更新
		rf.lastIncludedIndex = args.LastIncludedIndex
		rf.lastIncludedTerm = args.LastIncludedTerm
		rf.logs = []LogEntry{{Term: rf.lastIncludedTerm}}
		rf.logNum = rf.lastIncludedIndex
		rf.lastApplied = args.LastIncludedIndex
		rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
		DPrintf(follower, "Node[%v] Term[%v] 补交压缩 日志压缩后lastIncludedTerm[%v] lastIncludedIndex[%v] LogNum[%v] logs:%v", rf.me, rf.currentTerm, rf.lastIncludedTerm, rf.lastIncludedIndex, rf.logNum, rf.logs)
		rf.persister.SaveSnapshot(rf.snapshot)
		rf.persist()
		rf.mu.Unlock()
	}
}

//InstallSnapshot leader send InstallSnapshot to follower when leader has logs not enough to follower
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		//收到Term更小的快照，直接忽略，并向leader汇报
		reply.Term = rf.currentTerm
		return
	}
	defer rf.persist()
	//在身份转化的流程上面和心跳包把持一致
	rf.state = FOLLOWER
	rf.electionTimer.Reset(randomElectionTimeout())
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		DPrintf(common, "Node[%v] term[%v] 接收到[%v]快照包,term:[%v],成为FOLLOWER\n", rf.me, args.Term, args.LeaderId, rf.currentTerm)
	}
	reply.Term = rf.currentTerm
	if args.LastIncludedIndex < rf.lastIncludedIndex || (rf.lastIncludedIndex == args.LastIncludedIndex && rf.lastIncludedTerm == args.LastIncludedTerm) {
		//如果快照包的数据已经滞后了，那么直接忽略
		return
	}
	go func(args *InstallSnapshotArgs) {
		rf.ApplySnapshot(args)
	}(args)
}

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}
type InstallSnapshotReply struct {
	Term int
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
		DPrintf(common, "Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为args.Term[%v] < rf.currentTerm[%v]", rf.me, rf.currentTerm, args.CandidateId, args.Term, args.Term, rf.currentTerm)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:set currentTerm = T, convert to follower
		DPrintf(common, "Node[%v] 更新为follow节点，因为args.Term[%v] > rf.currentTerm[%v]", rf.me, args.Term, rf.currentTerm)
		rf.currentTerm = args.Term
		//这里赋值为-1，下面判断是否为-1即可排除这种可能
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
	}
	//为了保证安全性，首先检验日志的合法性
	//候选人应当包含所有已提交的日志条目
	if rf.logNum > 0 && (rf.votedFor == -1 || rf.votedFor == args.CandidateId) {
		if rf.getLog(rf.getLastLogIndex()).Term > args.LastLogTerm {
			//最后一条日志的任期号更大，则不投票，但是会根据term更新自己状态
			DPrintf(common, "Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为最后日志任期号[%v]比候选者最后日志任期号[%v]更大", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.getLog(rf.getLastLogIndex()).Term, args.LastLogTerm)
			reply.Term = args.Term
			reply.VoteGranted = false
			return
		}
		if rf.getLog(rf.getLastLogIndex()).Term == args.LastLogTerm && rf.getLastLogIndex() > args.LastLogIndex {
			//日志条目更多，也不更新
			DPrintf(common, "Node[%v] Term[%v] 拒绝投票给Node[%v] Term[%v]，因为日志条目[%v]比候选者日志条目[%v]更多", rf.me, rf.currentTerm, args.CandidateId, args.Term, rf.logNum, args.LastLogIndex)
			reply.VoteGranted = false
			reply.Term = args.Term
			return
		}
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// If votedFor is null or candidateId, grant vote; otherwise reject
		//已投票
		DPrintf(common, "Node[%v] Term[%v] 已经投票给其他节点[%v],因此拒绝投票给Node[%v] Term[%v]", rf.me, rf.currentTerm, rf.votedFor, args.CandidateId, args.Term)
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote to candidate, reset election timer
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.votedFor = args.CandidateId
	rf.persist()
	DPrintf(common, "Node[%v] 投票给Node[%v]", rf.me, rf.votedFor)
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
	uid := generateUID()
	DPrintf(rpc, "Node[%v] 发送rpc:sendRequestVote给 Node[%v] uid:[%v]", rf.me, server, uid)
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	DPrintf(rpc, "Node[%v] 接收rpc:sendRequestVote响应 ok[%v] Node[%v] uid:[%v]", rf.me, ok, server, uid)
	return ok
}

//rpc to call the AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	uid := generateUID()
	DPrintf(rpc, "Node[%v] 发送rpc:sendAppendEntries Node[%v] uid:[%v]", rf.me, server, uid)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	DPrintf(rpc, "Node[%v] 接收rpc:sendAppendEntries ok[%v] Node[%v] uid:[%v]", rf.me, ok, server, uid)
	return ok
}

//rpc to call InstallSnapshot
func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	uid := generateUID()
	DPrintf(rpc, "Node[%v] 发送rpc:InstallSnapshot Node[%v] uid:[%v]", rf.me, server, uid)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	DPrintf(rpc, "Node[%v] 接收rpc:InstallSnapshot ok[%v] Node[%v] uid:[%v]", rf.me, ok, server, uid)
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
	if !rf.isLeader() {
		return -1, -1, false
	}
	rf.mu.Lock()
	rf.mu.Unlock()

	isLeader := rf.state == LEADER
	if !isLeader {
		//二次判断
		return -1, -1, isLeader
	}
	index := rf.getLastLogIndex() + 1
	term := rf.currentTerm

	LogEntry := LogEntry{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.logs = append(rf.logs, LogEntry)
	rf.logNum++
	rf.persist()
	DPrintf(leader, "Node[%v]传入命令[%v],index:%v,term:%v,isLeader:%v", rf.me, command, index, term, isLeader)
	return index, term, isLeader
}
func (rf *Raft) isLeader() bool {
	return atomic.LoadInt32(&rf.state) == LEADER
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
	rf.electionTimer.Stop()
	rf.heartbeatTimer.Stop()
	DPrintf(common, "Node[%v] 死掉了 属性为:%v", rf.me, rf)
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
			DPrintf(common, "Node[%v] Term[%v]超时,身份是[%v]\n", rf.me, rf.currentTerm, rf.state)
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
func (rf *Raft) checkAndCommit() {
	N := rf.getLastLogIndex() //最大不会超过日志的最大索引
	for ; N > rf.commitIndex; N-- {
		//最小也要比上一次提交的小
		cnt := 1
		for index, matchIndex := range rf.matchIndex {
			//	DPrintf(common,"DEBUG Node[%v] Term[%v] Index[%v] matchIndex[%v]", rf.me, rf.currentTerm, index, matchIndex)
			if index == rf.me {
				continue
			}
			if matchIndex >= N {
				cnt++
			}
		}
		if cnt > len(rf.peers)/2 {
			DPrintf(leader, "Node[%v] Term[%v] Find max N[%v],commitIndex[%v]", rf.me, rf.currentTerm, N, rf.commitIndex)
			break
		}
	}
	if N < rf.lastIncludedIndex || rf.getLog(N).Term != rf.currentTerm {
		//根据Rules for Servers leaders的第四条，直接跳过
		if N >= 1 {
			DPrintf(leader, "Node[%v] Term[%v] 虽然发现max N[%v] 但N < rf.lastIncludedIndex[%v] 或logs[N].Term[%v] != currentTerm[%v]", rf.me, rf.currentTerm, N, rf.lastIncludedIndex, rf.getLog(N).Term, rf.currentTerm)
		}
		return
	}
	rf.commitIndex = N
	rf.persist()
	if rf.lastApplied != rf.commitIndex {
		DPrintf(leader, "Node[%v] Term[%v] 准备提交日志[%v] to [%v]", rf.me, rf.currentTerm, rf.lastApplied+1, rf.commitIndex)
		go func() {
			rf.commitFlag <- struct{}{} //开启一次提交检查
		}()
	}
}

func (rf *Raft) appendLog(server int, args *AppendEntriesArgs) {
	//向请求中添加日志
	args.PrevLogIndex = rf.nextIndex[server] - 1
	if args.PrevLogIndex >= 1 {
		args.PrevLogTerm = rf.getLog(args.PrevLogIndex).Term
	}
	//println(len(rf.logs), rf.nextIndex[server], rf.lastIncludedIndex)
	args.Entries = rf.logs[rf.nextIndex[server]-rf.lastIncludedIndex:]
}

//InstallSnapshotModel 需要发送快照包以快速同步压缩的情况
func (rf *Raft) InstallSnapshotModel(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	// nextIndex is located in snapshot
	DPrintf(leader, "Node[%v] Term[%v] 向Node[%v] 发送快照包 LastIncludedIndex[%v] LastIncludedTern[%v]", rf.me, rf.currentTerm, server, args.LastIncludedIndex, args.LastIncludedTerm)
	for ok, times := false, 0; !ok && times < 3; times++ {
		//重试三次
		ok = rf.sendInstallSnapshot(server, args, reply)
		if ok {
			rf.mu.Lock()
			//收到了响应
			if reply.Term > rf.currentTerm {
				//收到了更大的Term
				DPrintf(leader, "Node[%v] Term[%v] 变更为follower节点，因为server[%v] reply.Term[%v] > rf.currentTerm[%v] 重试测试为[%v]", rf.me, rf.currentTerm, server, reply.Term, rf.currentTerm, times)
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.electionTimer.Reset(randomElectionTimeout())
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if rf.currentTerm != args.Term {
				rf.mu.Unlock()
				return
			}
			rf.nextIndex[server] = max(rf.nextIndex[server], args.LastIncludedIndex+1)
			rf.matchIndex[server] = rf.nextIndex[server] - 1
			rf.checkAndCommit()
			DPrintf(leader, "Node[%v] Term[%v] 向Node[%v] 发送快照包成功 Next变更为[%v] LastIncludedIndex[%v] LastIncludedTern[%v]", rf.me, rf.currentTerm, server, rf.nextIndex[server], args.LastIncludedIndex, args.LastIncludedTerm)
			rf.mu.Unlock()
			return
		}
		//重试间隔为0 , 50ms ,   100ms
		time.Sleep(time.Duration(50*times) * time.Millisecond)
	}

}

//heartbeat 同步发送心跳给指定的server
func (rf *Raft) heartbeat(server int) {
	//DPrintf(common,"%d->%d\n", rf.me, server)
	args := AppendEntriesArgs{Term: -1, LeaderCommit: rf.commitIndex, PrevLogTerm: -1, PrevLogIndex: -1, LeaderId: rf.me}
	reply := AppendEntriesReply{}
	rf.mu.Lock()
	if rf.state != LEADER {
		//最后一次检验，因为可能已经不是leader了但是拿到了锁，然后发送了错误的term请求
		rf.mu.Unlock()
		return
	}
	if rf.nextIndex[server] <= rf.lastIncludedIndex {
		//说明需要的下一个请求已经压缩了
		args := InstallSnapshotArgs{
			Term:              rf.currentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.lastIncludedIndex,
			LastIncludedTerm:  rf.lastIncludedTerm,
			Data:              rf.snapshot,
		}
		reply := InstallSnapshotReply{}
		rf.mu.Unlock()
		rf.InstallSnapshotModel(server, &args, &reply)
		return
	}
	//检查是否需要传入log
	//DPrintf(debug,"Node[%v] 检查是否需要向Node[%v] 传入日志，rf.logNum-1：[%v] >= rf.nextIndex[server]：[%v]", rf.me, server, rf.logNum-1, rf.nextIndex[server])
	args.Term = rf.currentTerm
	rf.appendLog(server, &args)
	DPrintf(leader, "Node[%v] term[%v] 向Node[%v] 追加日志,LeaderCommit[%v], nextIndex[server]:%v,rf.logNum:%v,len(entries):[%v],logs:[%v]", rf.me, rf.currentTerm, server, args.LeaderCommit, rf.nextIndex[server], rf.logNum, len(args.Entries), args.Entries)
	rf.mu.Unlock()
	if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
		//DPrintf(leader,"Node[%d]心跳丢失->Node[%d]\n", rf.me, server)
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		DPrintf(leader, "Node[%v] Term[%v] 变更为follower节点，因为reply.Term[%v] > rf.currentTerm[%v]", rf.me, rf.currentTerm, reply.Term, rf.currentTerm)
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
		rf.persist()
		rf.electionTimer.Reset(randomElectionTimeout())
		rf.mu.Unlock()
		return
	}

	if rf.state != LEADER || rf.currentTerm != args.Term {
		DPrintf(common, "Node[%v] Term[%v] 收到发送给Node[%v]的延迟消息 但是已经不是leader身份 选择忽略", rf.me, rf.currentTerm, server)
		rf.mu.Unlock()
		return
	}
	//发生了log的复制
	if reply.Success {
		if reply.LastIncludeLogIndex > 0 {
			//follower的最后一条日志不为0，就是发生了快照压缩
			rf.nextIndex[server] = max(rf.nextIndex[server], reply.LastIncludeLogIndex+1)
			DPrintf(leader, "Node[%v] Term[%v] 发送给server[%v] prevLogIndex[%v] 已经被压缩了 next重置到[%v]", rf.me, rf.currentTerm, server, args.PrevLogIndex, rf.nextIndex[server])
			rf.mu.Unlock()
			return
		}
		DPrintf(leader, "Node[%v] Term[%v] 向server[%v] 日志复制成功,log:%v", rf.me, rf.currentTerm, server, args.Entries)
		//日志复制成功
		//新的nextIndex为原来的nextIndex[server](PrevLogIndex+1)+len(args.Entries)
		//由于是个高并发问题，可能这里已经被更新了
		rf.nextIndex[server] = max(rf.nextIndex[server], args.PrevLogIndex+len(args.Entries)+1)
		rf.matchIndex[server] = rf.nextIndex[server] - 1
		//查找是否存在可以提交的日志N(过半matchIndex[i] >= N)
		//这里可以快排的方法快速求出中位数，再看中位数是否符合要求[nLog(n)],为避免增加复杂度，暂且采用简单的方法
		rf.checkAndCommit()
	} else if reply.Conflict {
		//日志复制失败
		newPrevLogIndex := args.PrevLogIndex
		for newPrevLogIndex > reply.ConflictLogIndex {
			if newPrevLogIndex < rf.lastIncludedIndex || rf.getLog(newPrevLogIndex).Term == reply.ConflictLogTerm {
				break //寻找第一个相同Term的日志
			}
			newPrevLogIndex--
		}
		rf.nextIndex[server] = min(newPrevLogIndex, rf.nextIndex[server])
		DPrintf(leader, "Node[%v] Term[%v] 日志复制失败 更新Node[%v] 的nextIndex为[%v] ConflictLogIndex[%v] ConflictLogTerm[%v]", rf.me, rf.currentTerm, server, rf.nextIndex[server], reply.ConflictLogIndex, reply.ConflictLogTerm)
	}
	rf.mu.Unlock()
	return
}

//Execute the activities of the leader
//leader的状态转化
//定期发送心跳广播
func (rf *Raft) startLeaderAction() {
	DPrintf(leader, "Node[%v]号成为leader Term[%v] 状态为:%v\n", rf.me, rf.currentTerm, rf)
	rf.mu.Lock()
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	nextIndexInit := rf.getLastLogIndex() + 1
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
	for rf.killed() == false || rf.isLeader() == false {
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
	me := rf.me
	peers := rf.peers
	if rf.logNum > 0 {
		requestVoteArg.LastLogTerm = rf.getLog(rf.getLastLogIndex()).Term
		requestVoteArg.LastLogIndex = rf.getLastLogIndex()
	}
	requestVoteArg.Term = rf.currentTerm
	rf.mu.RUnlock()

	voteCh := make(chan bool, len(peers)-1)
	for i := range peers { // Send RequestVote RPCs to all other servers
		if i == me { // in PARALLEL
			continue
		}
		//异步拉票
		DPrintf(candidate, "Node[%v] Term[%v] 拉票于[%v]", rf.me, rf.currentTerm, i)
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
				DPrintf(candidate, "Node[%d]拉票于Node[%d]失败，变为follower\n", rf.me, i)
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.persist()
				rf.mu.Unlock()
				return
			}
			if rf.currentTerm != requestVoteArg.Term || rf.state != CANDIDATE {
				rf.mu.Unlock()
				voteCh <- false
				return
			}
			DPrintf(candidate, "Node[%d]拉票于Node[%d]成功,结果为[%v]，任期为[%d]\n", rf.me, i, reply.VoteGranted, rf.currentTerm)
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
		if rf.state != CANDIDATE || rf.currentTerm != requestVoteArg.Term {
			//推出了选举或者是rpc延迟导致失效
			rf.mu.RUnlock()
			break
		}
		rf.mu.RUnlock()
		if voteGranted {
			voteGrantedCnt++
		}
		if voteGrantedCnt > len(rf.peers)/2 {
			// gain over a half votes, switch to leader
			rf.mu.Lock()
			if rf.state != CANDIDATE || rf.currentTerm != requestVoteArg.Term {
				rf.mu.Unlock()
				break
			}
			rf.state = LEADER
			rf.mu.Unlock()
			//开始leader的动作(发送心跳)
			go rf.startLeaderAction()
			break
		}

		if voteCnt == len(peers) {
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
	rf.commitIndex = 0 //表示从未提交过日志
	rf.lastApplied = 0
	rf.logNum = 0
	rf.lastIncludedIndex = 0 //标识从来没有压缩过日志
	rf.applyCh = applyCh
	rf.state = FOLLOWER //初始化为follower节点
	rf.commitFlag = make(chan struct{})
	rf.snapshotFlag = make(chan ApplyMsg)
	rf.logs = make([]LogEntry, 1)
	rf.mu = LogRWMutex{raftId: me}
	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(10 * time.Millisecond)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.snapshot = persister.ReadSnapshot()
	if rf.lastIncludedIndex > 0 {
		rf.lastApplied = rf.lastIncludedIndex
	}
	DPrintf(common, "Node[%v] 初始化完成 状态为[%v]", me, rf)
	go rf.commandLog()
	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}
