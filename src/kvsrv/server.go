package kvsrv

import (
	"log"
	"math/rand"
	"sync"
	"time"
)

const Debug = false
const EmptyStr = ""

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type ClientRecord struct {
	seq     uint32
	lastVal string
	lastUse time.Time
	mu      sync.Mutex
}

func (cr *ClientRecord) CheckTimeOut(dur time.Duration) bool {
	if !cr.mu.TryLock() {
		return false
	}
	ret := time.Now().Sub(cr.lastUse) >= dur
	if !ret {
		cr.mu.Unlock()
	}
	return ret
}

func (cr *ClientRecord) UpdateRecords(lastVal *string) {
	if lastVal != nil {
		cr.lastVal = *lastVal
	} else {
		cr.lastVal = EmptyStr
	}
	cr.seq++
	cr.lastUse = time.Now()
}

type KVServer struct {
	mu              sync.RWMutex
	recordMu        sync.RWMutex
	data            map[string]string
	clientRecords   map[uint64]*ClientRecord
	cleanInterval   time.Duration
	timeoutInterval time.Duration
}

func (kv *KVServer) CleanUnused(id uint64, cr *ClientRecord) {
	for {
		time.Sleep(kv.cleanInterval)
		for cr.CheckTimeOut(kv.timeoutInterval) {
			if !kv.recordMu.TryLock() {
				cr.mu.Unlock()
				continue
			}
			cr.mu.Unlock()
			delete(kv.clientRecords, id)
			kv.recordMu.Unlock()
			//DPrintf("cleanUnused: delete client %v", id)
			return
		}
	}
}

func (kv *KVServer) GetLockedRecordPtr(id *uint64, seq uint32) *ClientRecord {
	kv.recordMu.RLock()
	ret, ok := kv.clientRecords[*id]
	kv.recordMu.RUnlock()
	if !ok {
		kv.recordMu.Lock()
		//if *id != 0 {
		//DPrintf("%v deleted client send request\n", *id)
		//}
		if *id == 0 {
			*id = kv.CreateId(seq)
		} else {
			kv.CreateDeletedRecord(*id, seq)
		}
		ret = kv.clientRecords[*id]
		kv.recordMu.Unlock()
	}
	ret.mu.Lock()
	return ret
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	cli := kv.GetLockedRecordPtr(&args.Id, args.Seq)
	reply.Id = args.Id
	defer cli.mu.Unlock()
	if cli.seq >= args.Seq {
		reply.Value = cli.lastVal
		cli.lastUse = time.Now()
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	cli.UpdateRecords(&reply.Value)
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	cli := kv.GetLockedRecordPtr(&args.Id, args.Seq)
	reply.Id = args.Id
	defer cli.mu.Unlock()
	if cli.seq >= args.Seq {
		reply.Value = cli.lastVal
		cli.lastUse = time.Now()
		return
	}
	cli.UpdateRecords(nil)
	kv.mu.Lock()
	defer kv.mu.Unlock()
	kv.data[args.Key] = args.Value
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	cli := kv.GetLockedRecordPtr(&args.Id, args.Seq)
	reply.Id = args.Id
	defer cli.mu.Unlock()
	if cli.seq >= args.Seq {
		DPrintf("get processed append, return pre val %v\n", cli.lastVal)
		reply.Value = cli.lastVal
		cli.lastUse = time.Now()
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	reply.Value = kv.data[args.Key]
	kv.data[args.Key] += args.Value
	cli.UpdateRecords(&reply.Value)
}

func (kv *KVServer) CreateId(seq uint32) uint64 {
	for {
		id := rand.Uint64()
		_, ok := kv.clientRecords[id]
		if ok || id == 0 {
			continue
		}
		DPrintf("createId: %v\n", id)
		kv.clientRecords[id] = &ClientRecord{seq: seq - 1, lastUse: time.Now()}
		go kv.CleanUnused(id, kv.clientRecords[id])
		return id
	}
}
func (kv *KVServer) CreateDeletedRecord(id uint64, seq uint32) {
	kv.clientRecords[id] = &ClientRecord{seq: seq - 1, lastUse: time.Now()}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.data = make(map[string]string)
	kv.clientRecords = make(map[uint64]*ClientRecord)
	kv.cleanInterval = 500 * time.Microsecond
	kv.timeoutInterval = 1000 * time.Microsecond
	rand.NewSource(time.Now().UnixNano())
	return kv
}
