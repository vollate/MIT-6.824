package kvsrv

import (
	"6.5840/labrpc"
	"crypto/rand"
	"math/big"
	"sync/atomic"
)

type Clerk struct {
	server *labrpc.ClientEnd
	id     atomic.Uint64
	seq    atomic.Uint32
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	ck.seq.Store(0)
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) Get(key string) string {
	ck.seq.Add(1)
	args := GetArgs{Key: key, Id: ck.id.Load(), Seq: ck.seq.Load()}
	var reply GetReply
	DPrintf("get: %v, %v, %v\n", args.Key, args.Id, args.Seq)
	for !ck.server.Call("KVServer.Get", &args, &reply) {
		DPrintf("resend get: %v, %v, %v\n", args.Key, args.Id, args.Seq)
	}
	ck.id.Store(reply.Id)
	return reply.Value
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	var target string
	if op == "Put" {
		target = "KVServer.Put"
	} else if op == "Append" {
		target = "KVServer.Append"
	} else {
		return "Invalid Op"
	}
	ck.seq.Add(1)
	args := PutAppendArgs{key, value, ck.id.Load(), ck.seq.Load()}
	var reply PutAppendReply
	DPrintf("pa: %v, %v, %v, %v\n", args.Key, args.Value, args.Id, args.Seq)
	for !ck.server.Call(target, &args, &reply) {
		DPrintf("resend pa: %v, %v, %v, %v\n", args.Key, args.Value, args.Id, args.Seq)
	}
	ck.id.Store(reply.Id)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
