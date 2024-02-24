package raft

import "sync"

const MaxRetry = 5

type VoteInfo struct {
	mu          sync.Mutex
	votedFor    int
	voteGranted int
	finished    bool
}

func (vi *VoteInfo) Lock() {
	vi.mu.Lock()
}

func (vi *VoteInfo) Unlock() {
	vi.mu.Unlock()
}

func (vi *VoteInfo) InitPoll(selfId int) {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.votedFor = selfId
	vi.voteGranted = 1
	vi.finished = false
}

func (vi *VoteInfo) Reset() {
	vi.mu.Lock()
	defer vi.mu.Unlock()
	vi.votedFor = -1
	vi.finished = true
}

type Leader struct {
	nextIndex  []int
	matchIndex []int
}

func MakeLeader(lastLog int) *Leader {
	ret := &Leader{nextIndex: make([]int, lastLog+1), matchIndex: make([]int, 0)}
	return ret
}
