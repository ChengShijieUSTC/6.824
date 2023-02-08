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
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"
)

const (
	STATE_CANDIDATE = iota
	STATE_FOLLOWER
	STATE_LEADER
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	state     int
	voteCount int

	// Persistent state on all servers.
	currentTerm int
	votedFor    int
	log         []LogEntry

	// Volatile state on all servers.
	commitIndex int
	lastApplied int

	// Volatile state on leaders.
	nextIndex  []int
	matchIndex []int

	chanApply     chan ApplyMsg
	chanGrantVote chan bool
	chanWinElect  chan bool
	chanHeartbeat chan bool
}

type LogEntry struct {
	Index   int
	Term    int
	Command interface{}
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isLeader := rf.state == STATE_LEADER
	return term, isLeader
}

//
// 一些辅助函数
//
func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].Index
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}

func max(x, y int) int {
	if x > y {
		return x
	}
	return y
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	data := rf.getRaftState()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	d.Decode(&rf.currentTerm)
	d.Decode(&rf.votedFor)
	d.Decode(&rf.log)
}

func (rf *Raft) getRaftState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
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

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < reply.Term {
		return
	}
	if args.Term > reply.Term { //降级为follower, 服务器规则1
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}
	rf.chanHeartbeat <- true //心跳

	if args.LastIncludedIndex < rf.log[0].Index {
		return
	}

	if args.LastIncludedIndex < rf.getLastLogIndex() {
		//为了保证原子性
		newLog := make([]LogEntry, 0)
		newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.log[0].Index+1:]...)
		rf.log = newLog
	} else {
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Index: args.LastIncludedIndex}}
	}
	rf.commitIndex = max(rf.commitIndex, args.LastIncludedIndex)
	rf.lastApplied = max(rf.lastApplied, args.LastIncludedIndex)
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), args.Data)
	rf.chanApply <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	//DPrintf("[sendInstallSnapshot]  leader:%v  server:%v  LastIncludedIndex:%v  %v", args.LeaderId, server, args.LastIncludedIndex, rf.log[0].Index)
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}
	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
	return ok
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	//rf.mu.Lock()
	//defer rf.mu.Unlock()

	rf.trimLog(index, rf.log[index-rf.log[0].Index].Term)
	//DPrintf("[Snapshot] %v snapshot, index=%v ... LastLogIndex:%v  lastApplied:%v", rf.me, index, rf.getLastLogIndex(), rf.lastApplied)
	rf.persister.SaveStateAndSnapshot(rf.getRaftState(), snapshot)
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.currentTerm { //RequestVote RPC规则3，拒收
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm { //服务器规则1, 变成follower
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isUpToDate(args.LastLogTerm, args.LastLogIndex) { //投票
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.chanGrantVote <- true
	}
}

func (rf *Raft) isUpToDate(candidateTerm int, candidateIndex int) bool {
	term, index := rf.getLastLogTerm(), rf.getLastLogIndex()
	return candidateTerm > term || (candidateTerm == term && candidateIndex >= index)
}

//
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if ok {
		if rf.state != STATE_CANDIDATE || rf.currentTerm != args.Term { //拒收
			return ok
		}
		if rf.currentTerm < reply.Term { //降级为follower, 服务器规则1
			rf.state = STATE_FOLLOWER
			rf.currentTerm = reply.Term
			rf.votedFor = -1
			return ok
		}

		if reply.VoteGranted {
			rf.voteCount++
			if rf.voteCount > len(rf.peers)/2 { //升级为leader
				rf.state = STATE_LEADER
				rf.persist()
				rf.nextIndex = make([]int, len(rf.peers))
				rf.matchIndex = make([]int, len(rf.peers))
				nextIndex := rf.getLastLogIndex() + 1
				for i := range rf.nextIndex {
					rf.nextIndex[i] = nextIndex
				}
				rf.chanWinElect <- true
			}
		}
	}
	return ok
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	NextTryIndex int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Success = false

	if args.Term < rf.currentTerm { //拒收
		reply.Term = rf.currentTerm
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.Term > rf.currentTerm { //降级为follower, 服务器规则1
		rf.state = STATE_FOLLOWER
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	rf.chanHeartbeat <- true //心跳
	reply.Term = rf.currentTerm
	if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.NextTryIndex = rf.getLastLogIndex() + 1
		return
	}

	if args.PrevLogTerm != rf.log[args.PrevLogIndex-rf.log[0].Index].Term {
		term := rf.log[args.PrevLogIndex-rf.log[0].Index].Term
		for i := args.PrevLogIndex - 1; i >= rf.log[0].Index; i-- {
			if rf.log[i-rf.log[0].Index].Term != term {
				reply.NextTryIndex = i + 1
				break
			}
		}
	} else {
		rf.log = rf.log[:(args.PrevLogIndex + 1 - rf.log[0].Index)]
		rf.log = append(rf.log, args.Entries...)

		reply.Success = true
		reply.NextTryIndex = args.PrevLogIndex + len(args.Entries)
		if rf.commitIndex < args.LeaderCommit {
			rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
			go rf.applyLog()
		}
	}
}

func (rf *Raft) applyLog() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		msg := ApplyMsg{}
		msg.CommandIndex = i
		msg.CommandValid = true
		msg.Command = rf.log[i-rf.log[0].Index].Command
		rf.chanApply <- msg
	}
	rf.lastApplied = rf.commitIndex
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	//DPrintf("[sendAppendEntries]  leader:%v  server:%v  PrevLogIndex:%v", server, args.LeaderId, args.PrevLogIndex)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	//DPrintf("[returnAppendEntries]  leader:%v  server:%v  NextTryIndex:%v", server, args.LeaderId, reply.NextTryIndex)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !ok || rf.state != STATE_LEADER || args.Term != rf.currentTerm {
		// invalid request
		return ok
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.state = STATE_FOLLOWER
		rf.votedFor = -1
		rf.persist()
		return ok
	}

	if reply.Success {
		if len(args.Entries) > 0 {
			rf.nextIndex[server] = args.Entries[len(args.Entries)-1].Index + 1
			rf.matchIndex[server] = rf.nextIndex[server] - 1
		}
	} else {
		rf.nextIndex[server] = min(reply.NextTryIndex, rf.getLastLogIndex())
	}

	for N := rf.getLastLogIndex(); N > rf.commitIndex && rf.log[N-rf.log[0].Index].Term == rf.currentTerm; N-- { //figure 8
		count := 1
		for i := range rf.peers {
			if i != rf.me && rf.matchIndex[i] >= N {
				count++
			}
		}
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
			go rf.applyLog()
			break
		}
	}

	return ok
}

func (rf *Raft) trimLog(lastIncludedIndex int, lastIncludedTerm int) {
	newLog := make([]LogEntry, 0)
	newLog = append(newLog, LogEntry{Index: lastIncludedIndex, Term: lastIncludedTerm})

	for i := len(rf.log) - 1; i >= 0; i-- {
		if rf.log[i].Index == lastIncludedIndex && rf.log[i].Term == lastIncludedTerm {
			newLog = append(newLog, rf.log[i+1:]...)
			break
		}
	}
	rf.log = newLog
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term, index := -1, -1
	isLeader := rf.state == STATE_LEADER

	if isLeader {
		term = rf.currentTerm
		index = rf.getLastLogIndex() + 1
		rf.log = append(rf.log, LogEntry{Index: index, Term: term, Command: command})
		rf.persist()
	}
	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		rf.mu.Lock()
		state := rf.state
		rf.mu.Unlock()

		switch state {
		case STATE_FOLLOWER:
			select {
			case <-rf.chanGrantVote:
			case <-rf.chanHeartbeat:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
				rf.mu.Lock()
				rf.state = STATE_CANDIDATE
				rf.persist()
				rf.mu.Unlock()
			}
		case STATE_CANDIDATE:
			rf.mu.Lock()
			rf.currentTerm++
			rf.votedFor = rf.me
			rf.voteCount = 1
			rf.persist()
			rf.mu.Unlock()
			//选举
			go func() {
				rf.mu.Lock()
				args := &RequestVoteArgs{}
				args.Term = rf.currentTerm
				args.CandidateId = rf.me
				args.LastLogIndex = rf.getLastLogIndex()
				args.LastLogTerm = rf.getLastLogTerm()
				state := rf.state
				rf.mu.Unlock()
				for server := range rf.peers {
					if server != rf.me && state == STATE_CANDIDATE {
						go rf.sendRequestVote(server, args, &RequestVoteReply{})
					}
				}
			}()
			select {
			case <-rf.chanHeartbeat:
				rf.mu.Lock()
				rf.state = STATE_FOLLOWER
				rf.mu.Unlock()
			case <-rf.chanWinElect:
			case <-time.After(time.Millisecond * time.Duration(rand.Intn(300)+200)):
			}
		case STATE_LEADER:
			//发送心跳
			go func() {
				rf.mu.Lock()
				state := rf.state
				rf.mu.Unlock()
				for server := range rf.peers {
					if server != rf.me && state == STATE_LEADER {
						rf.mu.Lock()
						nextIndex := rf.nextIndex[server]
						snapshotIndex := rf.log[0].Index
						rf.mu.Unlock()
						if nextIndex <= snapshotIndex {
							//传快照
							rf.mu.Lock()
							args := &InstallSnapshotArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.LastIncludedIndex = rf.log[0].Index
							args.LastIncludedTerm = rf.log[0].Term
							args.Data = rf.persister.ReadSnapshot()
							rf.mu.Unlock()
							go rf.sendInstallSnapshot(server, args, &InstallSnapshotReply{})
						} else {
							//AppendEntries
							rf.mu.Lock()
							args := &AppendEntriesArgs{}
							args.Term = rf.currentTerm
							args.LeaderId = rf.me
							args.PrevLogIndex = rf.nextIndex[server] - 1
							args.PrevLogTerm = rf.log[args.PrevLogIndex-rf.log[0].Index].Term
							if rf.nextIndex[server] <= rf.getLastLogIndex() {
								args.Entries = rf.log[(rf.nextIndex[server] - rf.log[0].Index):]
							}
							args.LeaderCommit = rf.commitIndex
							rf.mu.Unlock()
							go rf.sendAppendEntries(server, args, &AppendEntriesReply{})
						}
					}
				}
			}()
			time.Sleep(time.Millisecond * 60)
		}
	}
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.state = STATE_FOLLOWER
	rf.voteCount = 0

	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Index: 0, Term: 0})

	rf.readPersist(persister.ReadRaftState())

	rf.commitIndex = rf.log[0].Index
	rf.lastApplied = rf.log[0].Index

	rf.chanApply = applyCh
	rf.chanGrantVote = make(chan bool, 100)
	rf.chanWinElect = make(chan bool, 100)
	rf.chanHeartbeat = make(chan bool, 100)

	// initialize from state persisted before a crash
	//rf.readPersist(persister.ReadRaftState())

	rf.persist()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
