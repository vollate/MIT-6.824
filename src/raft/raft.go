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

// A Go object implementing a single Raft peer.
type Raft struct {
	mu             sync.Mutex          // Lock to protect shared access to this peer's state
	peers          []*labrpc.ClientEnd // RPC end points of all peers
	persister      *Persister          // Object to hold this peer's persisted state
	me             int                 // this peer's index into peers[]
	dead           int32               // set by Kill()
	currentTerm    int
	heartbeat      atomic.Bool
	candidateAwait atomic.Int32
	electionTerm   atomic.Int32
	voteInfo       VoteInfo
	leader         *Leader
	commitIndex    atomic.Int32
	lastApplied    atomic.Int32

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// Your code here (2A).
	return rf.currentTerm, rf.leader != nil
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
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
	// Your data here (3A, 3B).
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
	// Your data here (3A).
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []byte
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) getCurrentTerm() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm
}

func (rf *Raft) enterNewTerm(newTerm int) {
	DPrintf("%v enter to term %v", rf.me, newTerm)
	rf.mu.Lock()
	rf.currentTerm = newTerm
	rf.mu.Unlock()
	rf.voteInfo.Reset()
	rf.electionTerm.Store(-1)
	rf.candidateAwait.Store(1)
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	term := rf.getCurrentTerm()
	if args.Term < term { // stale
		reply.Term = term
		reply.Success = false
		return
	}
	if args.Term > term { // update self term from heartbeat
		rf.enterNewTerm(args.Term)
	}
	if args.Entries == nil { // heartbeat
		rf.heartbeat.Store(true)
	} else {
		//TODO: commit entries
	}
	reply.Success = true
}

func (rf *Raft) sendHeartBeat() {
	currentTerm := rf.getCurrentTerm()
	args := AppendEntriesArgs{Term: currentTerm, LeaderId: rf.me, Entries: nil} //TODO: fill args
	for !rf.killed() {
		for index := range rf.peers {
			if index == rf.me {
				continue
			}
			go func(index int) {
				var reply AppendEntriesReply
				success := false
				cnt := 0
				for !success {
					success = rf.peers[index].Call("Raft.AppendEntries", &args, &reply)
					cnt++
					if cnt > MaxRetry {
						return
					}
				}
				DPrintf("%v send heartbeat to %v\n", rf.me, index)
				if !reply.Success {
					rf.leader = nil
					rf.enterNewTerm(reply.Term)
				}
			}(index)
		}
		rf.mu.Lock()
		if rf.leader == nil {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		time.Sleep(50 * time.Millisecond)
	}
}

func (rf *Raft) becomeLeader() {
	DPrintf("%v become leader in term %v\n", rf.me, rf.currentTerm)
	rf.leader = &Leader{}
	go rf.sendHeartBeat()
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	term := rf.getCurrentTerm()
	if args.Term < term || args.Term == term && (rf.leader != nil || rf.heartbeat.Load()) { // stale request term
		DPrintf("%v refuse to vote for %v in term %v\n", rf.me, args.CandidateId, term)
		reply.VoteGranted = false
		reply.Term = term
		return
	}
	if args.Term > term {
		rf.enterNewTerm(args.Term)
		term = args.Term
	}
	rf.voteInfo.Lock()
	votedFor := rf.voteInfo.votedFor
	defer rf.voteInfo.Unlock()
	lastLogTerm := rf.persister.ReadRaftState()
	if (votedFor == -1 || votedFor == args.CandidateId) && (int(lastLogTerm[rf.lastApplied.Load()]) <= args.LastLogTerm && rf.lastApplied.Load() <= int32(args.LastLogIndex)) {
		DPrintf("%v become follower in term %v\n", rf.me, rf.currentTerm)
		rf.voteInfo.votedFor = args.CandidateId
		reply.VoteGranted = true
		reply.Term = term
	} else { // not newest log
		DPrintf("%v refuse to vote for %v in term: not newest log %v\n", rf.me, args.CandidateId, term)
		reply.VoteGranted = false
		reply.Term = term
	}
	// Your code here (3A, 3B).
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) startElection() {
	rf.electionTerm.Add(1)
	DPrintf("%v start electionterm %v\n", rf.me, rf.electionTerm.Load())
	rf.mu.Lock()
	rf.currentTerm += 1
	newTerm := rf.currentTerm
	rf.voteInfo.InitPoll(rf.me)
	rf.mu.Unlock()
	go rf.electionTimer()
	lastApplied := int(rf.lastApplied.Load())
	args := RequestVoteArgs{Term: newTerm, CandidateId: rf.me, LastLogIndex: lastApplied, LastLogTerm: int(rf.persister.ReadRaftState()[lastApplied])}
	electionTerm := int(rf.electionTerm.Load())
	for index := range rf.peers {
		if index == rf.me {
			continue
		}
		go rf.voteAndCheck(electionTerm, index, args)
	}
}

func (rf *Raft) voteAndCheck(electionTerm int, peerIndex int, args RequestVoteArgs) {
	var reply RequestVoteReply
	success := false
	for !success {
		if int(rf.electionTerm.Load()) != electionTerm { // stale election
			return
		}
		DPrintf("%v request %v for vote, election term %v\n", rf.me, peerIndex, electionTerm)
		success = rf.sendRequestVote(peerIndex, &args, &reply)
	}
	rf.voteInfo.Lock()
	defer rf.voteInfo.Unlock()
	if reply.VoteGranted {
		rf.voteInfo.voteGranted += 1
		DPrintf("id %v voteGranted %v\n", rf.me, rf.voteInfo.voteGranted)
	} else {
		if reply.Term > args.Term { // candidate to follower
			rf.enterNewTerm(reply.Term)
			DPrintf("candidate %v retrieve to follower deal to meet higher term\n", rf.me)
		}
	}
	if !rf.voteInfo.finished && rf.voteInfo.voteGranted >= int(math.Ceil(float64(len(rf.peers))/2.0)) { // become leader
		rf.voteInfo.finished = true
		rf.electionTerm.Store(-1)
		rf.becomeLeader()
	}
}

func (rf *Raft) electionTimer() {
	ms := 100 + (rand.Int63() % 500)
	time.Sleep(time.Duration(ms) * time.Millisecond)
	if rf.killed() {
		return
	}
	rf.voteInfo.Lock()
	finished := rf.voteInfo.finished
	rf.voteInfo.Unlock()
	if rf.leader != nil && !finished && !rf.heartbeat.Load() { // restart election
		DPrintf("restart election id %v time %v\n", rf.me, rf.electionTerm.Load())
		rf.startElection()
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		ms := 50 + (rand.Int63() % 500)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		if rf.leader == nil { // is follower
			if !rf.heartbeat.Load() && rf.electionTerm.Load() == -1 && rf.candidateAwait.Load() <= 0 { // check heartbeat and election status
				rf.startElection()
			} else {
				rf.heartbeat.Store(false)
			}
		}
		if rf.candidateAwait.Load() > 0 {
			rf.candidateAwait.Add(-1)
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
	rf.persister.raftstate = append(rf.persister.raftstate, 0)
	rf.voteInfo.Reset()
	rf.electionTerm.Store(-1)

	// Your initialization code here (3A, 3B, 3C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
