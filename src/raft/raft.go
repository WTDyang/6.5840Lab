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
	"fmt"

	"sync"
	"sync/atomic"
	"time"

	"6.5840/labrpc"
)

type STATE int

const (
	FOLLOWER STATE = iota
	CANDIDATE
	LEADER
)

// HEARTBEAT_INTERVAL 心跳间隔50ms
const HEARTBEAT_INTERVAL = 50 * time.Millisecond

// as each Raft peer becomes aware that successive log entries are
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
	mu        sync.RWMutex        // Lock to protect shared access to this peer's state;change from Mutex to RWMutex,in order to increase efficiency
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm    int           //latest term server has seen (initialized to 0	on first boot, increases monotonically)
	votedFor       int           //candidateId that received vote in current	term (or null if none)
	logs           []interface{} //log entries; each entry contains command for state machine, and term when entry was received by leader (first index is 1
	state          STATE         //enum for state(follower , candidate and leader)
	electionTimer  *time.Ticker  //timeout
	heartbeatTimer *time.Ticker  //heartbeatTimer
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// AppendEntriesArgs request for AppendEntries RPC
type AppendEntriesArgs struct {
	Term         int           //leader's term
	LeaderId     int           //so follower can redirect clients
	PrevLogIndex int           //index of log entry immediately preceding new ones
	PrevLogTerm  int           //term of prevLogIndex entry
	Entries      []interface{} //log entries to store (empty for heartbeat;	may send more than one for efficiency)
	LeaderCommit bool          //leader’s commitIndex

}

// AppendEntriesReply result for AppendEntries RPC
type AppendEntriesReply struct {
	Term    int  //currentTerm,for leader to update itself
	Success bool //true if follower contained entry matching prevLogIndex and prevLogTerm
}

// AppendEntries Invoked by leader to replicate log entries; also used as heartbeat
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.killed() {
		return
	}
	if args.Term < rf.currentTerm {
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
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	// received AppendEntries RPC from current leader, reset election timer
	//更新随机超时时间()
	rf.electionTimer.Reset(randomElectionTimeout())
	//设置返回值
	reply.Success = true
	reply.Term = rf.currentTerm
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
	println(rf.me, "任期为：", term, "是否为leader：", isleader)
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
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
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
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		// If RPC request contains term T > currentTerm:set currentTerm = T, convert to follower
		rf.currentTerm = args.Term
		//这里赋值为-1，下面判断是否为-1即可排除这种可能
		rf.votedFor = -1
		rf.state = FOLLOWER
	}

	if rf.votedFor != -1 && rf.votedFor != args.CandidateId {
		// If votedFor is null or candidateId, grant vote; otherwise reject
		//已投票
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}

	// grant vote to candidate, reset election timer
	rf.electionTimer.Reset(randomElectionTimeout())
	rf.votedFor = args.CandidateId

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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//rpc to call the AppendEntries
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
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
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

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
	println(rf.me, "死掉了")
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
			fmt.Printf("%d号超时,身份是%d\n", rf.me, rf.state)
			if rf.state == LEADER {
				//if is a leader:beak
				//rf.electionTimer.Reset(randomElectionTimeout())
				rf.electionTimer.Stop()
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

//heartbeat 发送心跳给指定的server
func (rf *Raft) heartbeat(server int) {
	//fmt.Printf("%d->%d\n", rf.me, server)
	args := AppendEntriesArgs{LeaderId: rf.me}
	reply := AppendEntriesReply{}
	rf.mu.RLock()
	args.Term = rf.currentTerm
	rf.mu.RUnlock()
	go func() {
		if ok := rf.sendAppendEntries(server, &args, &reply); !ok {
			return
		}
		rf.mu.Lock()
		if reply.Term > rf.currentTerm {
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			rf.state = FOLLOWER
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
	}()
	return
}

//Execute the activities of the leader
//定期发送心跳广播
func (rf *Raft) startLeaderAction() {
	fmt.Printf("%d号成为leader\n", rf.me)
	rf.electionTimer.Stop()
	//匿名函数：广播
	broadcast := func() {
		for i := range rf.peers {
			if i == rf.me {
				continue
			}
			rf.heartbeat(i)
		}
	}
	//循环广播
	for rf.killed() == false {
		rf.mu.RLock()
		if rf.state != LEADER {
			break
		}
		rf.mu.RUnlock()
		//fmt.Printf("%d - %d - %d\n", rf.me, rf.currentTerm, rf.state)
		broadcast()
		rf.heartbeatTimer.Reset(HEARTBEAT_INTERVAL)
		<-rf.heartbeatTimer.C
	}
}

//elections the leader
func (rf *Raft) elections() {
	rf.mu.Lock()
	rf.currentTerm++                                // Increment currentTerm
	rf.votedFor = rf.me                             // Vote for self
	rf.electionTimer.Reset(randomElectionTimeout()) // Reset election timer
	rf.mu.Unlock()

	requestVoteArg := RequestVoteArgs{CandidateId: rf.me}
	rf.mu.RLock()
	requestVoteArg.Term = rf.currentTerm
	rf.mu.RUnlock()

	voteCh := make(chan bool, len(rf.peers)-1)
	for i := range rf.peers { // Send RequestVote RPCs to all other servers
		if i == rf.me { // in PARALLEL
			continue
		}
		//异步拉票
		//fmt.Printf("%d拉票于%d，权重为%d\n", rf.me, i, rf.currentTerm)
		go func(i int) {
			reply := RequestVoteReply{}
			if ok := rf.sendRequestVote(i, &requestVoteArg, &reply); !ok {
				//异常情况也要算成失败
				println("拉票异常", rf.me, "->", i)
				voteCh <- false
				return
			}
			rf.mu.Lock()
			if reply.Term > rf.currentTerm {
				fmt.Printf("%d拉票于%d成功，变为follower\n", rf.me, i)
				// If RPC response contains term T > currentTerm:
				// set currentTerm = T, convert to follower
				rf.currentTerm = reply.Term
				rf.votedFor = -1
				rf.state = FOLLOWER
				rf.mu.Unlock()
				return
			}
			fmt.Printf("%d拉票于%d成功，权重为%d\n", rf.me, i, rf.currentTerm)
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

	// Your initialization code here (2A, 2B, 2C).
	rf.electionTimer = time.NewTicker(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTicker(randomElectionTimeout())
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	println(me, "初始化完成")
	go rf.ticker()
	return rf
}
