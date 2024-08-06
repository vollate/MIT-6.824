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
	//	"bytes"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	index   int
	term    int
	command interface{}
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	commitIndex int // index of highest log entry known to be committed
	lastApplied int // index of highest log entry applied to state machine
	state       RaftServerState

	heartbeat bool

	// persistent state
	term        int
	log         []LogEntry
	appendCnt   int        // the count of follower that append success
	appendMutex sync.Mutex // use for condition variable in AppendEntries

	// vote related
	votefor     int // the candidate vote for in current term
	voteGranted int // num of votes granted

	// leader state
	nextIndex  []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (3A).
	term = rf.term
	isLeader = (rf.state == LEADER) && rf.dead != 1
	return term, isLeader
}

func (rf *Raft) transitRaftState(newState RaftServerState, newTerm int, lock bool) {
	DPrintf("%v transit from state %v to %v\t term: %v -> %v", rf.me, rf.state, newState, rf.term, newTerm)
	funcTable := transitTable.Instance()
	var currentState RaftServerState
	if lock {
		rf.mu.Lock()
		currentState = rf.state
		rf.mu.Unlock()
	} else {
		currentState = rf.state
	}
	transitFunc := (*funcTable)[currentState][newState]
	if transitFunc == nil {
		DPrintf("Error!!! %v can't transit from %v to %v", rf.me, currentState, newState)
		return
	}
	transitFunc(rf, newTerm, lock)
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

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.term
	if rf.state == LEADER || rf.heartbeat {
		reply.VoteGranted = false
	}
	if rf.term > args.Term || rf.commitIndex > args.LastLogIndex { // old term candidate or not up to date candidate
		reply.VoteGranted = false
		return
	}
	if rf.term < args.Term { // self is old term, update term to catch up candidate
		rf.transitRaftState(FOLLOWER, args.Term, false)
	}
	if votable := (rf.votefor == -1 || rf.votefor == args.CandidateId); !votable {
		reply.VoteGranted = false
		reply.Term = term
		return
	}
	DPrintf("%v vote for %v in term %v", rf.me, args.CandidateId, rf.term)
	rf.votefor = args.CandidateId
	rf.heartbeat = true
	reply.VoteGranted = true
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

func (rf *Raft) requetVote(targetServer int, requestArgs RequestVoteArgs) {
	var reply RequestVoteReply
	ok := rf.sendRequestVote(targetServer, &requestArgs, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.VoteGranted {
		DPrintf("%v receive %v votes in term %v", rf.me, rf.voteGranted, rf.term)
		if stale := (rf.term > requestArgs.Term) || rf.state != CANDIDATE; stale {
			DPrintf("candidate request %v is stale, %v ,self %v", rf.me, rf.state, rf.term)
			return
		}
		rf.voteGranted++
		if rf.voteGranted >= majorityNum(len(rf.peers)) { // get majority votes
			rf.transitRaftState(LEADER, -1, false)
		}
		return
	}
	if !reply.VoteGranted && reply.Term > rf.term {
		rf.transitRaftState(FOLLOWER, reply.Term, false)
	}
}

type AppendEntriesArgs struct {
	Term int //leader’s term
	//LeaderId     int    // so follower can redirect clients
	PrevLogIndex int    //index of log entry immediately preceding new ones
	PrevLogTerm  int    //term of prevLogIndex entry
	Entries      []byte //log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int    //leader’s commitIndex
}

type AppendEntriesReply struct {
	Term    int  // currentTerm, for leader to update itself
	Success bool // true if follower contained entry matching prevLogIndex and prevLogTerm
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.term { // stale leader
		reply.Term = rf.term
		reply.Success = false
		return
	} else if args.Term > rf.term { // update self term from heartbeat
		rf.transitRaftState(FOLLOWER, args.Term, false)
	}
	if args.Entries == nil { // heartbeat
		rf.heartbeat = true
		reply.Success = true
		return
	}
	//TODO: commit entries
	//if rf.commitIndex+len(args.Entries) == args.LeaderCommit {
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
	// Your code here (3B).

	term, isLeader = rf.GetState()
	if isLeader {
		rf.mu.Lock()
		defer rf.mu.Unlock()
		index = rf.commitIndex + 1
		rf.log = append(rf.log, LogEntry{index, term, command})
		rf.commitIndex = index
	}
	return index, term, isLeader
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	args := AppendEntriesArgs{rf.term /*rf.me,*/, rf.commitIndex, rf.lastApplied, nil, rf.commitIndex}
	rf.mu.Unlock()
	cond := sync.NewCond(&rf.appendMutex)
	cond.L.Lock()
	defer cond.L.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		go rf.sendAppendEntries(i, args)
	}
	for rf.appendCnt < majorityNum(len(rf.peers)) {
		cond.Wait()
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	for true {
		ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
		if !ok {
			return
		}
		time.Sleep(5 * time.Millisecond)
		if reply.Success {
			return
		}
		term, _ := rf.GetState()
		if reply.Term > term {
			rf.transitRaftState(FOLLOWER, reply.Term, true)
		}
	}
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.  //
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
	voteArgs := RequestVoteArgs{rf.term, rf.me, rf.commitIndex, rf.log[rf.commitIndex].term}
	rf.voteGranted = 1
	rf.mu.Unlock()
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		DPrintf("%v request %v for vote", rf.me, i)
		go rf.requetVote(i, voteArgs)
	}
	ms := 50 + (rand.Int63() % 300)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == CANDIDATE { // entrer next term to election
		rf.transitRaftState(CANDIDATE, -1, false)
	}
}

func (rf *Raft) broadcastHeartbeat() {
	for !rf.killed() {
		rf.mu.Lock()
		if rf.state != LEADER {
			rf.mu.Unlock()
			return
		}
		args := AppendEntriesArgs{Term: rf.term, PrevLogIndex: rf.commitIndex, PrevLogTerm: rf.log[rf.commitIndex].term}
		rf.mu.Unlock()
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go rf.sendHeartBeat(index, args)
		}
		time.Sleep(50 * time.Millisecond)
	}

}

func (rf *Raft) sendHeartBeat(server int, args AppendEntriesArgs) {
	var reply AppendEntriesReply
	ok := rf.peers[server].Call("Raft.AppendEntries", &args, &reply)
	if !ok {
		return
	}
	if !reply.Success {
		rf.transitRaftState(FOLLOWER, reply.Term, true)
	}
}

func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		rf.mu.Lock()
		state := rf.state
		heartBeat := rf.heartbeat
		rf.heartbeat = false
		rf.mu.Unlock()
		if state == FOLLOWER && !heartBeat {
			rf.transitRaftState(CANDIDATE, -1, true)
		}
		// pause for a random amount of time between 50 and 350
		// milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
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
	rf.state = FOLLOWER
	rf.votefor = -1
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.log = append(rf.log, LogEntry{0, 0, nil})

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
