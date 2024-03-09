package raft

import "sync"

const MaxTry = 3

type RaftServerState int

const (
	FOLLOWER RaftServerState = iota
	CANDIDATE
	LEADER
)

const (
	VOTE_GRANTED         = -1
	VOTE_TRANSMIT_FAILED = -2
)

type TransitFunc func(*Raft, int, bool)
type TransitTable struct {
	once  sync.Once
	table [][]TransitFunc
}

var transitTable TransitTable

func (tf *TransitTable) Instance() *[][]TransitFunc {
	tf.once.Do(func() {
		tf.table = make([][]TransitFunc, 3)
		tf.table[FOLLOWER] = []TransitFunc{follower2Follower, follower2Candidate, nil}
		tf.table[CANDIDATE] = []TransitFunc{candidate2Follower, candidate2Candidate, candidate2Leader}
		tf.table[LEADER] = []TransitFunc{leader2Follower, nil, nil}
	})
	return &tf.table
}
func follower2Follower(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.term = newTerm
	rf.votefor = -1
	rf.heartbeat = true
	if lock {
		rf.mu.Unlock()
	}
}

func follower2Candidate(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.state = CANDIDATE
	rf.term++
	rf.votefor = rf.me
	if lock {
		rf.mu.Unlock()
	}
	go rf.startElection()
}

func candidate2Candidate(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.term++
	if lock {
		rf.mu.Unlock()
	}
	go rf.startElection() // start new election
}

func candidate2Leader(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.state = LEADER
	if lock {
		rf.mu.Unlock()
	}
	go rf.broadcastHeartbeat()
}

func candidate2Follower(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.state = FOLLOWER
	rf.term = newTerm
	rf.votefor = -1
	rf.heartbeat = true // wait the candidate become leader
	if lock {
		rf.mu.Unlock()
	}
}

func leader2Follower(rf *Raft, newTerm int, lock bool) {
	if lock {
		rf.mu.Lock()
	}
	rf.state = FOLLOWER
	rf.term = newTerm
	rf.votefor = -1
	rf.heartbeat = true // avoid election timeout once
	if lock {
		rf.mu.Unlock()
	}
}
