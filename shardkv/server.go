package shardkv

import (
	"bytes"
	"sync"
	"time"

	"github.com/Fallensouls/raft/labgob"
	"github.com/Fallensouls/raft/labrpc"
	"github.com/Fallensouls/raft/raft"
	"github.com/Fallensouls/raft/shardmaster"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Key   string
	Value string
	Type  string // get, put or append
	ID    string
	Seq   uint64
	Done  chan struct{}
}

type ShardKV struct {
	mu           sync.RWMutex
	me           int
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	masters      []*labrpc.ClientEnd
	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	sm          *shardmaster.Clerk
	data        map[string]string
	executed    map[string]uint64
	config      shardmaster.Config
	shutdown    chan struct{}
	lastApplied uint64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}

	err := kv.rf.Read()
	switch err {
	case raft.ErrPartitioned:
		reply.Err = ErrPartitioned
		return
	case raft.ErrNotLeader:
		reply.WrongLeader = true
		return
	case raft.ErrTimeout:
		reply.Err = ErrTimeout
		return
	case raft.ErrShutdown:
		reply.WrongLeader = true
		return
	}
	kv.mu.RLock()
	// log.Printf("data of gid %v: %v", kv.gid, kv.data)
	if value, ok := kv.data[args.Key]; ok {
		reply.Value = value
		reply.Err = OK
	} else {
		reply.Err = ErrNoKey
	}
	kv.mu.RUnlock()
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	seq := kv.executed[args.ID]
	kv.mu.RUnlock()
	if seq >= args.Seq {
		reply.Err = ErrExecuted
		return
	}

	op := Op{args.Key, args.Value, args.Op, args.ID, args.Seq, make(chan struct{})}
	_, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.WrongLeader = true
		return
	}
	select {
	case <-op.Done:
		reply.Err = OK
	case <-time.After(10 * raft.HeartBeatInterval):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) createSnapshot(index uint64) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.RLock()
	// e.Encode(kv.executed)
	e.Encode(kv.data)
	e.Encode(kv.executed)
	kv.mu.RUnlock()
	data := w.Bytes()
	kv.rf.SnapshotData <- raft.Snapshot{Index: index, Data: data}
}

func (kv *ShardKV) readSnapshot(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	// d.Decode(&kv.executed)
	kv.mu.Lock()
	d.Decode(&kv.data)
	d.Decode(&kv.executed)
	kv.mu.Unlock()
}

func (kv *ShardKV) run() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok && !msg.NoOpCommand {
				if op, ok := msg.Command.(Op); ok {
					if kv.executed[op.ID] < op.Seq {
						kv.mu.Lock()
						switch op.Type {
						case "Put":
							kv.data[op.Key] = op.Value
						case "Append":
							kv.data[op.Key] += op.Value
						}
						kv.executed[op.ID] = op.Seq
						kv.mu.Unlock()
						go func() {
							if kv.rf.State() == raft.Leader && !msg.Recover {
								op.Done <- struct{}{}
							}
						}()

						kv.lastApplied = uint64(msg.CommandIndex)
					}
				}
			}
		// read snapshots from leader
		case index := <-kv.rf.InstallSnapshotCh:
			kv.readSnapshot(kv.rf.ReadSnapshot())
			kv.lastApplied = index
		// create snapshots
		case <-kv.rf.SnapshotCh:
			kv.createSnapshot(kv.lastApplied)
		case <-kv.shutdown:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (kv *ShardKV) Kill() {
	kv.rf.Kill()
	// Your code here, if desired.
	close(kv.shutdown)
}

//
// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)
	kv.shutdown = make(chan struct{})
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)
	kv.rf.SetMaxSize(kv.maxraftstate)
	kv.readSnapshot(kv.rf.ReadSnapshot())
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.data = make(map[string]string)
	kv.executed = make(map[string]uint64)
	kv.config = kv.sm.Query(-1)
	go kv.run()
	return kv
}
