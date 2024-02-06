package kvsrv

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Id    uint64
	Seq   uint32
}

type PutAppendReply struct {
	Value string
	Id    uint64
}

type GetArgs struct {
	Key string
	Id  uint64
	Seq uint32
}

type GetReply struct {
	Value string
	Id    uint64
}
