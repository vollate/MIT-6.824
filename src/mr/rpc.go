package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

const (
	Map = iota
	Reduce
	Wait
	Terminate
)

// Add your RPC definitions here.

type GetTaskArgs struct {
}

type GetTaskReply struct {
	WorkerId int
	TaskType int
	NReduce  int
	FilePath []string
}

type AliveArgs struct {
	WorkerId  int
	TaskType  int
	TimeStamp string
}

type AliveReply struct {
	TimeStamp string
}

type FinishArgs struct {
	WorkerId    int
	TaskType    int
	ResultFiles []string
	TimeStamp   string
}

type FinishReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
