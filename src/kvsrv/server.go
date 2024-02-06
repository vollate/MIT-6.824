package kvsrv

import (
	"log"
	"math/rand"
	//"runtime"
	"sync"
	"time"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientInfo struct {
	seq     uint32
	lastVal string
	mutex   sync.Mutex
}

func (ci *ClientInfo) GetSeq() uint32 {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	return ci.seq
}

func (ci *ClientInfo) IncreaseSeq() {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	ci.seq++
}

func (ci *ClientInfo) GetLastVal() string {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	return ci.lastVal
}

func (ci *ClientInfo) SetLastVal(val *string) {
	ci.mutex.Lock()
	defer ci.mutex.Unlock()
	ci.lastVal = *val
}

type KVServer struct {
	mu         sync.RWMutex
	data       map[string]string
	clientLogs map[uint64]*ClientInfo
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	if args.Id == 0 {
		args.Id = kv.createId()
	}
	reply.Id = args.Id
	cli := kv.clientLogs[args.Id]
	if cli.GetSeq() >= args.Seq {
		reply.Value = cli.GetLastVal()
		return
	}
	cli.IncreaseSeq()
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	reply.Value = kv.data[args.Key]
	cli.SetLastVal(&reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Id == 0 {
		args.Id = kv.createId()
	}
	reply.Id = args.Id
	cli := kv.clientLogs[args.Id]
	if cli.GetSeq() >= args.Seq {
		reply.Value = cli.GetLastVal()
		return
	}
	cli.IncreaseSeq()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	if args.Id == 0 {
		args.Id = kv.createId()
	}
	reply.Id = args.Id
	cli := kv.clientLogs[args.Id]
	if cli.GetSeq() >= args.Seq {
		reply.Value = cli.GetLastVal()
		return
	}
	cli.IncreaseSeq()
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	cli.SetLastVal(&reply.Value)
	kv.data[args.Key] += args.Value
}

func (kv *KVServer) createId() uint64 {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	for {
		id := rand.Uint64()
		_, ok := kv.clientLogs[id]
		if ok || id == 0 {
			continue
		}
		kv.clientLogs[id] = &ClientInfo{seq: 0, lastVal: ""}
		return id
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.clientLogs = make(map[uint64]*ClientInfo)
	rand.NewSource(time.Now().UnixNano())
	return kv
}
