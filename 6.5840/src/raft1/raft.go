package raft

// The file raftapi/raft.go defines the interface that raft must
// expose to servers (or the tester), but see comments below for each
// of these functions for more details.
//
// Make() creates a new raft peer that implements the raft interface.

import (
	//	"bytes"
	"math/rand"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
	"6.5840/raftapi"
	tester "6.5840/tester1"
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu         sync.Mutex          // Lock to protect shared access to this peer's state
	peers      []*labrpc.ClientEnd // RPC end points of all peers
	persister  *tester.Persister   // Object to hold this peer's persisted state
	me         int                 // this peer's index into peers[]
	dead       int32               // set by Kill()
	applyCh    chan raftapi.ApplyMsg
	leaderCond *sync.Cond
	applyCond  *sync.Cond

	leaderId      int
	status        int // 0 -> Follower, 1 -> Candidate, 2 -> Leader
	lastPingTime  time.Time
	numberOfPeers int

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent State on all Servers
	currentTerm int
	votedFor    int
	log         []Log

	// Volatile State on all Servers
	commitIndex int
	lastApplied int

	// Volatile State on leaders
	nextIndex  []int
	matchIndex []int
}

type Log struct {
	Index   int
	Term    int
	Command interface{}
}

func (l *Log) getIndex() int {
	return l.Index
}

func (l *Log) getTerm() int {
	return l.Term
}

func (rf *Raft) stepDownToFollower(term int) {
	rf.currentTerm = term
	rf.votedFor = -1
	rf.status = 0
}

func (rf *Raft) getLastLogIndex() int {
	return rf.log[len(rf.log)-1].getIndex()
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].getTerm()
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	// Your code here (3A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term := rf.currentTerm
	isleader := rf.leaderId == rf.me
	DPrintf("Peer %v: Term %v, leader:%v", rf.me, term, isleader)
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
	// Your code here (3C).
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
	// Your code here (3C).
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

// how many bytes in Raft's persisted log?
func (rf *Raft) PersistBytes() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.persister.RaftStateSize()
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term        int
	VoteGranted bool
}

func (rf *Raft) isCandidateLogUpToDate(lastLogTerm, lastLogIndex int) bool {
	currLastLogTerm := rf.log[len(rf.log)-1].getTerm()
	currLastLogIndex := rf.log[len(rf.log)-1].getIndex()

	if lastLogTerm < currLastLogTerm {
		return false
	}
	if lastLogTerm > currLastLogTerm {
		return true
	}
	if lastLogIndex >= currLastLogIndex {
		return true
	}
	return false
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	reply.Term = rf.currentTerm
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && rf.isCandidateLogUpToDate(args.LastLogTerm, args.LastLogIndex) {
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.lastPingTime = time.Now()
	} else {
		reply.VoteGranted = false
	}
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

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []Log
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntry(args *AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	rf.lastPingTime = time.Now()
	rf.leaderId = args.LeaderId
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Broadcast()
	}
	if args.Term > rf.currentTerm {
		rf.stepDownToFollower(args.Term)
	}

	// HeartBeatMessages
	reply.Term = rf.currentTerm
	if rf.getLastLogIndex() < args.PrevLogIndex || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Success = false
		return
	} else if args.Entries == nil {
		reply.Success = true
		return
	} else {
		insertionIndex := args.PrevLogIndex + 1
		rf.log = rf.log[:insertionIndex]
		rf.log = append(rf.log, args.Entries...)
		reply.Success = true
	}
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.applyCond.Broadcast()
	}
}

func (rf *Raft) sendAppendEntry(server int, args *AppendEntryArgs, reply *AppendEntryReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntry", args, reply)
	return ok
}

func (rf *Raft) reinitializeStateForLeader() {
	for i := 0; i < rf.numberOfPeers; i++ {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = 0
	}
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
}

func (rf *Raft) sendHeartbeats() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != 2 {
			rf.leaderCond.Wait()
		}
		DPrintf("Peer %v starting heartbeats", rf.me)
		args := &AppendEntryArgs{
			Term:         rf.currentTerm,
			LeaderId:     rf.me,
			PrevLogIndex: rf.log[len(rf.log)-1].getIndex(),
			PrevLogTerm:  rf.log[len(rf.log)-1].getTerm(),
			Entries:      nil,
			LeaderCommit: rf.commitIndex,
		}
		rf.lastPingTime = time.Now()
		rf.leaderId = rf.me
		rf.mu.Unlock()
		for i := 0; i < rf.numberOfPeers; i++ {
			if i == rf.me {
				continue
			}
			go func(index int, args *AppendEntryArgs) {
				reply := &AppendEntryReply{}
				ok := rf.sendAppendEntry(index, args, reply)
				if ok {
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.stepDownToFollower(reply.Term)
					}
					rf.mu.Unlock()
				}
			}(i, args)
		}
		time.Sleep(50 * time.Millisecond)
	}
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
	// Your code here (3B)
	DPrintf("Inside Start for Peer %v", rf.me)
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.status != 2 {
		return -1, -1, false
	}

	index := rf.getLastLogIndex() + 1

	rf.log = append(rf.log, Log{
		Index:   index,
		Term:    rf.currentTerm,
		Command: command,
	})

	rf.matchIndex[rf.me] = index

	DPrintf("Peer %v, Received New Log Entry Starting Agreement", rf.me)

	go rf.startAgreement()

	return index, rf.currentTerm, true
}

func (rf *Raft) startAgreement() {
	// For each log send log from [nextIndex[i]+1:]
	// If success nextIndex[i] = rf.lastIndex() + 1, matchIndex[i] = rf.lastIndex()
	// If fail nextIndex[i]-- then again retry

	for i := 0; i < rf.numberOfPeers; i++ {
		if i == rf.me {
			continue
		}
		go rf.sendNewEntriesToPeer(i)
	}
}

func (rf *Raft) sendNewEntriesToPeer(i int) {
	rf.mu.Lock()
	if rf.status != 2 {
		rf.mu.Unlock()
		return
	}
	startOfEntries := rf.nextIndex[i]
	endOfEntries := rf.getLastLogIndex()
	if startOfEntries > endOfEntries {
		rf.mu.Unlock()
		return
	}
	args := &AppendEntryArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.leaderId,
		PrevLogIndex: startOfEntries - 1,
		PrevLogTerm:  rf.log[startOfEntries-1].Term,
		Entries:      rf.log[startOfEntries : endOfEntries+1],
		LeaderCommit: rf.commitIndex,
	}
	reply := &AppendEntryReply{}
	rf.mu.Unlock()

	ok := rf.sendAppendEntry(i, args, reply)
	if !ok {
		go rf.sendNewEntriesToPeer(i)
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.status != 2 || rf.currentTerm != args.Term {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.stepDownToFollower(reply.Term)
	} else if reply.Success {
		rf.nextIndex[i] = endOfEntries + 1
		rf.matchIndex[i] = endOfEntries
	} else if !reply.Success {
		rf.nextIndex[i] = max(1, rf.nextIndex[i]-1)
		go rf.sendNewEntriesToPeer(i)
	}

}

func (rf *Raft) leaderUpdateCommitIndex() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.status != 2 {
			rf.leaderCond.Wait()
		}
		DPrintf("Peer %v is Leader and Updating Commit Index", rf.me)
		matchIndexCopy := make([]int, len(rf.matchIndex))
		copy(matchIndexCopy, rf.matchIndex)
		sort.Ints(matchIndexCopy)
		upperN := matchIndexCopy[rf.numberOfPeers/2]
		for n := upperN; n > rf.commitIndex; n-- {
			if rf.log[n].Term == rf.currentTerm {
				rf.commitIndex = n
				DPrintf("Peer %v, found new index to commit : %v", rf.me, n)
				rf.applyCond.Broadcast()
				break
			}
		}
		rf.mu.Unlock()

		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) applyToStateMachine() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.commitIndex <= rf.lastApplied {
			rf.applyCond.Wait()
		}

		// Collect all messages to apply
		msgs := make([]raftapi.ApplyMsg, 0)
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
			msgs = append(msgs, raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.lastApplied].Command,
				CommandIndex: rf.lastApplied,
			})
		}
		rf.mu.Unlock()

		for _, msg := range msgs {
			rf.applyCh <- msg
		}
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	rf.status = 1
	DPrintf("Peer %v did not receive heartbeat. Starting Election", rf.me)
	rf.currentTerm++
	rf.votedFor = rf.me
	term := rf.currentTerm
	args := &RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log[len(rf.log)-1].getIndex(),
		LastLogTerm:  rf.log[len(rf.log)-1].getTerm(),
	}
	rf.mu.Unlock()

	var votesMu sync.Mutex
	votes := 1

	for i := 0; i < rf.numberOfPeers; i++ {
		if i == rf.me {
			continue
		}
		go func(index int, args *RequestVoteArgs) {
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(index, args, reply)
			if ok {
				rf.mu.Lock()
				defer rf.mu.Unlock()
				if rf.currentTerm != term || rf.status != 1 {
					return
				}
				if reply.Term > term {
					rf.stepDownToFollower(reply.Term)
					return
				}
				if reply.VoteGranted {
					votesMu.Lock()
					votes++
					if votes > rf.numberOfPeers/2 {
						DPrintf("Peer %v has votes making it leader. Starting to send HeartBeatMessages", rf.me)
						rf.status = 2
						rf.reinitializeStateForLeader()
						rf.leaderCond.Broadcast()
					}
					votesMu.Unlock()
				}
			}
		}(i, args)
	}

}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		// Check if a leader election should be started.

		// pause for a random amount of time between 350 and 500
		// milliseconds.
		ms := 350 + (rand.Int63() % 150)
		timeout := time.Duration(ms) * time.Millisecond
		time.Sleep(timeout)

		rf.mu.Lock()
		if time.Since(rf.lastPingTime) > timeout {
			DPrintf("Peer %v. Election timeout triggered.", rf.me)
			go rf.startElection()
		}
		rf.mu.Unlock()

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
	persister *tester.Persister, applyCh chan raftapi.ApplyMsg) raftapi.Raft {
	DPrintf("Starting Raft Peer for Raft peer : %v", me)
	rf := &Raft{
		currentTerm:   0,
		votedFor:      -1,
		log:           make([]Log, 0),
		commitIndex:   0,
		lastApplied:   0,
		numberOfPeers: len(peers),
		nextIndex:     make([]int, len(peers)),
		matchIndex:    make([]int, len(peers)),
		status:        0,
	}
	rf.log = append(rf.log, Log{
		Index:   0,
		Term:    0,
		Command: "$",
	})
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.applyCh = applyCh
	rf.mu = sync.Mutex{}
	rf.lastPingTime = time.Now()
	rf.leaderCond = sync.NewCond(&rf.mu)
	rf.applyCond = sync.NewCond(&rf.mu)
	DPrintf("Initializing Raft Peer for Raft peer : %v", rf.me)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	DPrintf("Starting Raft Ticker for Raft peer : %v", rf.me)
	go rf.ticker()

	DPrintf("Starting Raft Heartbeat Sender for Raft peer : %v", rf.me)
	go rf.sendHeartbeats()

	DPrintf("Starting Raft Applier to State Machine for Raft peer : %v", rf.me)
	go rf.applyToStateMachine()

	DPrintf("Starting Raft Commit Index Updaterfor Raft peer : %v", rf.me)
	go rf.leaderUpdateCommitIndex()

	return rf
}
