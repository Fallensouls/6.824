package shardkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"github.com/Fallensouls/raft/labgob"
	"github.com/Fallensouls/raft/labrpc"
	"github.com/Fallensouls/raft/raft"
	"github.com/Fallensouls/raft/shardmaster"
)

const pollInterval = 50 * time.Millisecond

type Shard map[string]string

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Data interface{}
	Type string // get, put or append
	ID   string
	Seq  uint64
}

type KeyValue struct {
	Key   string
	Value string
}

type State struct {
	// OwnShards map[int]struct{}
	Config shardmaster.Config
	Data   map[int]Shard
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
	ownShards   map[int]struct{}
	oldShards   map[int]struct{}
	data        map[int]Shard
	executed    map[string]uint64
	notifyMap   sync.Map
	nextConfig  int
	config      shardmaster.Config
	pollTicker  *time.Ticker
	shutdown    chan struct{}
	lastApplied uint64
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	if kv.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	kv.mu.RLock()
	num, oldNum := kv.nextConfig, kv.config.Num
	kv.mu.RUnlock()
	if args.Num != num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Num != oldNum {
		reply.Err = ErrWaitingData
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
	defer kv.mu.RUnlock()

	if shard, ok := kv.data[key2shard(args.Key)]; ok {
		if value, ok := shard[args.Key]; ok {
			reply.Value = value
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongGroup
	}
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.RLock()
	seq := kv.executed[args.ID]
	num, oldNum := kv.nextConfig, kv.config.Num
	kv.mu.RUnlock()
	if seq >= args.Seq {
		reply.Err = ErrExecuted
		return
	}
	if args.Num != num {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Num != oldNum {
		reply.Err = ErrWaitingData
		return
	}
	op := Op{KeyValue{args.Key, args.Value}, args.Op, args.ID, args.Seq}
	index, _, isLeader := kv.rf.Start(op)

	if !isLeader {
		reply.WrongLeader = true
		return
	}
	done := make(chan struct{}, 1)
	kv.notifyMap.Store(index, done)
	defer kv.notifyMap.Delete(index)
	select {
	case <-done:
		reply.Err = OK
	case <-time.After(10 * raft.HeartBeatInterval):
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) Migrate(args *PullArgs, reply *PullReply) {
	if kv.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	kv.mu.RLock()
	defer kv.mu.RUnlock()
	if args.ConfigNum > kv.config.Num {
		reply.Err = ErrConfigNum
		return
	}

	log.Printf("pull args: %v", args)
	reply.Data = make(map[int]Shard)
	for _, shard := range args.Shards {
		log.Printf("pull data: %v", kv.data[shard])
		reply.Data[shard] = make(Shard)
		reply.Data[shard] = kv.data[shard]
	}
	reply.Err = OK
}

func (kv *ShardKV) createSnapshot(index uint64) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kv.mu.RLock()

	e.Encode(kv.data)
	e.Encode(kv.executed)
	e.Encode(kv.config)
	e.Encode(kv.nextConfig)
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

	kv.mu.Lock()
	d.Decode(&kv.data)
	d.Decode(&kv.executed)
	d.Decode(&kv.config)
	d.Decode(&kv.nextConfig)
	kv.mu.Unlock()
}

func (kv *ShardKV) handleConfigChange() {
	if kv.rf.State() != raft.Leader {
		return
	}
	lastConfig := kv.sm.Query(-1)
	kv.mu.RLock()
	num := kv.nextConfig
	kv.mu.RUnlock()
	oldConfig := kv.sm.Query(num)
	for i := num + 1; i <= lastConfig.Num; i++ {
		config := kv.sm.Query(i)
		isLeader := kv.updateConfig(oldConfig, config)
		if !isLeader {
			return
		}
		oldConfig = config
	}
}

func (kv *ShardKV) updateConfig(oldConfig, newConfig shardmaster.Config) (isLeader bool) {
	kv.mu.Lock()
	kv.nextConfig = newConfig.Num
	kv.mu.Unlock()
	ownShards := make(map[int]struct{})
	waitingShards := make(map[int][]int)
	for shard, gid := range newConfig.Shards {
		if gid == kv.gid {
			ownShards[shard] = struct{}{}
			if gid := oldConfig.Shards[shard]; gid != 0 && gid != kv.gid {
				waitingShards[gid] = append(waitingShards[gid], shard)
			}
		}
	}

	shards := kv.pullShards(oldConfig, waitingShards)
	op := Op{Data: State{newConfig, shards}, Type: "UpdateConfig"}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}
	done := make(chan struct{}, 1)
	kv.notifyMap.Store(index, done)
	defer kv.notifyMap.Delete(index)
	select {
	case <-done:
		return
	case <-time.After(5 * raft.HeartBeatInterval):
		return
	}
}

func (kv *ShardKV) pullShards(config shardmaster.Config, waitingShards map[int][]int) (shards map[int]Shard) {
	if len(waitingShards) == 0 {
		return
	}
	log.Printf("gid %v pull shards, config num: %v", kv.gid, config.Num)
	ch := make(chan map[int]Shard, len(waitingShards))
	for gid, shards := range waitingShards {
		go func(gid int, shards []int) {
			for {
				if servers, ok := config.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						args := PullArgs{config.Num, shards}
						var reply PullReply
						ok := srv.Call("ShardKV.Migrate", &args, &reply)
						log.Printf("ok: %v", ok)
						log.Printf("arg: %v, reply: %v", args, reply)
						if ok {
							switch reply.Err {
							case OK:
								ch <- reply.Data
								return
							}
						}
					}
				}
			}
		}(gid, shards)
	}
	shards = make(map[int]Shard)
	for done := 0; done < len(waitingShards); done++ {
		select {
		case shard := <-ch:
			for num, data := range shard {
				shards[num] = data
			}
		}
	}
	return
}

func (kv *ShardKV) cleanShards() {
	for shard := range kv.data {
		if _, ok := kv.ownShards[shard]; !ok {
			delete(kv.data, shard)
		}
	}
}

func (kv *ShardKV) run() {
	for {
		select {
		case msg, ok := <-kv.applyCh:
			if ok && !msg.NoOpCommand {
				if op, ok := msg.Command.(Op); ok {
					kv.mu.Lock()
					switch op.Type {
					case "Put", "Append":
						if kv.executed[op.ID] < op.Seq {
							data := op.Data.(KeyValue)
							shard := kv.data[key2shard(data.Key)]
							if op.Type == "Put" {
								shard[data.Key] = data.Value
							} else {
								// log.Printf("append key %v value %v to server %v", data.Key, data.Value, kv.rf.ID)
								// log.Printf("ownshards of server %v: %v", kv.rf.ID, kv.ownShards)
								shard[data.Key] += data.Value
							}
							kv.executed[op.ID] = op.Seq
						}

					case "UpdateConfig":
						state := op.Data.(State)
						if state.Config.Num > kv.config.Num {
							kv.config = state.Config
							if kv.rf.State() != raft.Leader {
								kv.nextConfig = state.Config.Num
							}

							for num, shard := range state.Data {
								kv.data[num] = shard
							}
							log.Printf("gid %v updates config %v", kv.gid, kv.config.Num)
						}
					case "Clean":
						kv.cleanShards()
					}
					kv.mu.Unlock()
					if done, ok := kv.notifyMap.Load(msg.CommandIndex); ok {
						done := done.(chan struct{})
						done <- struct{}{}
					}
					kv.lastApplied = uint64(msg.CommandIndex)
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

func (kv *ShardKV) poll() {
	for {
		select {
		case <-kv.pollTicker.C:
			kv.handleConfigChange()
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
	labgob.Register(KeyValue{})
	labgob.Register(State{})
	labgob.Register(shardmaster.Config{})
	labgob.Register(map[int]struct{}{})
	labgob.Register(map[int]Shard{})

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
	kv.sm = shardmaster.MakeClerk(kv.masters)
	kv.data = make(map[int]Shard)
	kv.executed = make(map[string]uint64)
	// kv.config = kv.sm.Query(-1)
	// kv.nextConfig = kv.config.Num
	kv.pollTicker = time.NewTicker(pollInterval)
	kv.ownShards = make(map[int]struct{})
	kv.oldShards = make(map[int]struct{})

	for shard, gid := range kv.config.Shards {
		if gid == kv.gid {
			kv.ownShards[shard] = struct{}{}
		}
	}
	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(Shard)
	}
	kv.readSnapshot(kv.rf.ReadSnapshot())
	go kv.run()
	go kv.poll()
	return kv
}
