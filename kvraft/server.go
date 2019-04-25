package raftkv

import (
	"log"
	"sync"
	"time"

	"github.com/Fallensouls/raft/labgob"
	"github.com/Fallensouls/raft/labrpc"
	"github.com/Fallensouls/raft/raft"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key       string
	Value     string
	Operation string
	ID        string
	Seq       uint64
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	done     chan int
	db       map[string]string // key-value database
	executed map[string]uint64 // the set of commands which have been executed
}

func (kv *KVServer) Get(req *GetRequest, res *GetResponse) {
	// return if receiver isn't the leader.
	//log.Printf("state: %v", kv.rf.State())

	if kv.rf.State() != raft.Leader {
		res.WrongLeader = true
		return
	}

	// ensure that receiver is still the leader.
	err := kv.rf.Read()
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if err == raft.ErrPartitioned {
		res.WrongLeader = false
		res.Err = ErrPartitioned
		return
	}

	if err == raft.ErrNotLeader {
		res.WrongLeader = true
		return
	}

	res.WrongLeader = false

	//kv.mu.Lock()
	var ok bool
	if res.Value, ok = kv.db[req.Key]; ok {
		log.Printf("key %v:  %v", req.Key, res.Value)
		res.Err = OK
	} else {
		res.Err = ErrNoKey
		log.Println(res.Err, req.Key, kv.db, kv.me)
	}
	//kv.mu.Unlock()
}

func (kv *KVServer) PutAppend(req *PutAppendRequest, res *PutAppendResponse) {
	// return if receiver isn't the leader.
	if kv.rf.State() != raft.Leader {
		res.WrongLeader = true
		return
	}
	res.WrongLeader = false

	kv.mu.Lock()
	seq, _ := kv.executed[req.ID]
	kv.mu.Unlock()
	if seq >= req.Seq {
		res.Err = ErrExecuted
		return
	}

	// print all the valid requests.
	//log.Printf("server %v recieve request: %v", kv.rf.ID, req)

	index, _, _ := kv.rf.Start(Op{Key: req.Key, Value: req.Value, Operation: req.Op, ID: req.ID, Seq: req.Seq})
	timeout := time.NewTimer(10 * raft.HeartBeatInterval)
	for {
		select {
		case doneIndex := <-kv.done:
			if doneIndex == index {
				res.Err = OK
				return
			}
		case <-timeout.C:
			timeout.Stop()
			res.Err = ErrTimeout
			return
		}
	}
}

func (kv *KVServer) apply() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok && !msg.NoOpCommand {
				op := msg.Command.(Op)
				kv.mu.Lock()
				if seq := kv.executed[op.ID]; seq < op.Seq {
					log.Printf("op of server %v: %v", kv.me, op)
					switch op.Operation {
					case "Put":
						kv.db[op.Key] = op.Value
						kv.executed[op.ID] = op.Seq
					case "Append":
						kv.db[op.Key] += op.Value
						kv.executed[op.ID] = op.Seq
					default:
					}
					if kv.rf.State() == raft.Leader {
						kv.done <- msg.CommandIndex
					}
				}
				kv.mu.Unlock()
			}
		}
	}
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *KVServer) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.done = make(chan int, 100)
	kv.db = make(map[string]string)
	kv.executed = make(map[string]uint64)
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	go kv.apply()

	return kv
}
