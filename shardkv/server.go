package shardkv

import (
	"bytes"
	"fmt"
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

type Migration struct {
	ConfigNum int
	Shards    map[int]Shard
	Executed  map[string]uint64
}

type Reconfig struct {
	Config       shardmaster.Config
	DeleteShards map[int][]int
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
	data        map[int]Shard
	executed    map[string]uint64
	notifyMap   sync.Map
	nextConfig  int
	config      shardmaster.Config
	ownShards   map[int]int // shard -> config number
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
	nextConfigNum, prevConfigNum := kv.nextConfig, kv.config.Num
	dataNum := kv.ownShards[key2shard(args.Key)]
	kv.mu.RUnlock()
	if args.Num != nextConfigNum {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Num != prevConfigNum && args.Num != dataNum {
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
	nextConfigNum, prevConfigNum := kv.nextConfig, kv.config.Num
	dataNum := kv.ownShards[key2shard(args.Key)]
	kv.mu.RUnlock()
	if seq >= args.Seq {
		reply.Err = ErrExecuted
		return
	}
	if args.Num != nextConfigNum {
		reply.Err = ErrWrongGroup
		return
	}
	if args.Num != prevConfigNum && args.Num != dataNum {
		reply.Err = ErrWaitingData
		return
	}
	op := Op{KeyValue{args.Key, args.Value}, args.Op, args.ID, args.Seq}
	index, _, isLeader := kv.rf.Start(op)
	log.Printf("put/append data %v to gid %v, config number: %v", args.Value, kv.gid, args.Num)
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
		reply.Err = ErrDataNotReady
		return
	}

	log.Printf("pull args: %v", args)
	reply.Shards = make(map[int]Shard)
	reply.Executed = make(map[string]uint64)
	for _, shard := range args.Shards {
		log.Printf("pull data: %v", kv.data[shard])
		reply.Shards[shard] = make(Shard)
		reply.Shards[shard] = kv.data[shard]
	}
	for id, seq := range kv.executed {
		reply.Executed[id] = seq
	}
	reply.Err = OK
}

func (kv *ShardKV) Delete(args *DeleteArgs, reply *DeleteReply) {
	op := Op{Data: DeleteArgs{args.ConfigNum, args.Shards}, Type: "Delete"}
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
	}
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
	prevConfig := kv.sm.Query(num)
	for i := num + 1; i <= lastConfig.Num; i++ {
		nextConfig := kv.sm.Query(i)
		if err := kv.updateConfig(prevConfig, nextConfig); err != nil {
			return
		}
		prevConfig = nextConfig
	}
}

func (kv *ShardKV) updateConfig(prevConfig, nextConfig shardmaster.Config) error {
	waitingShards := make(map[int][]int)
	ownShards := make(map[int]struct{})
	for shard, gid := range nextConfig.Shards {
		if gid == kv.gid {
			if gid := prevConfig.Shards[shard]; gid != 0 && gid != kv.gid {
				waitingShards[gid] = append(waitingShards[gid], shard)
			} else {
				ownShards[shard] = struct{}{}
			}
		}
	}
	kv.mu.Lock()
	kv.nextConfig = nextConfig.Num
	for shard := range ownShards {
		kv.ownShards[shard] = nextConfig.Num
	}
	kv.mu.Unlock()
	kv.pullShards(prevConfig, waitingShards)

	// op := Op{Data: Migration{nextConfig, shards, executed, waitingShards}, Type: "UpdateConfig"}
	op := Op{Data: Reconfig{nextConfig, waitingShards}, Type: "UpdateConfig"}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		kv.mu.Lock()
		kv.nextConfig--
		kv.mu.Unlock()
		return fmt.Errorf("ErrNotLeader")
	}

	done := make(chan struct{}, 1)
	kv.notifyMap.Store(index, done)
	defer kv.notifyMap.Delete(index)
	select {
	case <-done:
		return nil
		// case <-time.After(200 * time.Millisecond):
		// 	return fmt.Errorf("ErrTimeout")
	}
}

func (kv *ShardKV) pullShards(config shardmaster.Config, waitingShards map[int][]int) (shards map[int]Shard, executed map[string]uint64) {
	if len(waitingShards) == 0 {
		return
	}
	log.Printf("gid %v pull shards, config num: %v", kv.gid, config.Num+1)
	ch := make(chan struct{}, len(waitingShards))
	for gid, shards := range waitingShards {
		go func(gid int, shards []int) {
			for {
				if servers, ok := config.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						args := PullArgs{config.Num + 1, shards}
						var reply PullReply
						ok := srv.Call("ShardKV.Migrate", &args, &reply)
						log.Printf("ok: %v", ok)
						log.Printf("pull data from %v, arg: %v, reply: %v", gid, args, reply)
						if ok {
							switch reply.Err {
							case OK:
								op := Op{Data: Migration{config.Num + 1, reply.Shards, reply.Executed}, Type: "Migrate"}
								kv.rf.Start(op)
								ch <- struct{}{}
								return
							}
						}
					}
				}
			}
		}(gid, shards)
	}
	shards = make(map[int]Shard)
	executed = make(map[string]uint64)
	for done := 0; done < len(waitingShards); done++ {
		select {
		case <-ch:
			// case data := <-ch:
			// 	for num, data := range data.Shards {
			// 		shards[num] = data
			// 	}
			// 	for id, seq := range data.Executed {
			// 		if executed[id] < seq {
			// 			executed[id] = seq
			// 		}
			// 	}
		}
	}
	return
}

func (kv *ShardKV) cleanShards(config shardmaster.Config, deleteShards map[int][]int) {
	ch := make(chan struct{}, len(deleteShards))
	for gid, shards := range deleteShards {
		go func(gid int, shards []int) {
			for {
				if servers, ok := config.Groups[gid]; ok {
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						args := DeleteArgs{config.Num + 1, shards}
						var reply DeleteReply
						ok := srv.Call("ShardKV.Delete", &args, &reply)
						// log.Printf("ok: %v", ok)
						// log.Printf("pull data from %v, arg: %v, reply: %v", gid, args, reply)
						if ok {
							switch reply.Err {
							case OK:
								ch <- struct{}{}
								return
							}
						}
					}
				}
			}
		}(gid, shards)
	}
	for done := 0; done < len(deleteShards); done++ {
		select {
		case <-ch:
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
								log.Printf("append key %v value %v to gid %v", data.Key, data.Value, kv.gid)
								// log.Printf("ownshards of server %v: %v", kv.rf.ID, kv.ownShards)
								shard[data.Key] += data.Value
							}
							kv.executed[op.ID] = op.Seq
						}
					case "Migrate":
						migration := op.Data.(Migration)
						if migration.ConfigNum > kv.config.Num {
							for num, shard := range migration.Shards {
								kv.data[num] = shard
								kv.ownShards[num] = migration.ConfigNum
							}
							for id, seq := range migration.Executed {
								if kv.executed[id] < seq {
									kv.executed[id] = seq
								}
							}
						}
					case "UpdateConfig":
						reconfig := op.Data.(Reconfig)
						if reconfig.Config.Num > kv.config.Num {
							if kv.rf.State() == raft.Leader {
								go kv.cleanShards(kv.config, reconfig.DeleteShards)
							}
							kv.config = reconfig.Config
							if reconfig.Config.Num > kv.nextConfig {
								kv.nextConfig = reconfig.Config.Num
							}

							log.Printf("gid %v updates config %v", kv.gid, kv.config.Num)
						}
						// log.Printf("migrate data: %v", migration.Shards)
					case "Delete":
						args := op.Data.(DeleteArgs)
						for _, num := range args.Shards {
							if kv.ownShards[num] <= args.ConfigNum {
								delete(kv.data, num)
							}
						}
					}

					kv.mu.Unlock()
					if done, ok := kv.notifyMap.Load(msg.CommandIndex); ok {
						done := done.(chan struct{})
						done <- struct{}{}
					}
				}
				kv.lastApplied = uint64(msg.CommandIndex)
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
	labgob.Register(Migration{})
	labgob.Register(Reconfig{})
	labgob.Register(PullArgs{})
	labgob.Register(PullReply{})
	labgob.Register(DeleteArgs{})
	labgob.Register(DeleteReply{})
	labgob.Register(GetArgs{})
	labgob.Register(GetReply{})
	labgob.Register(PutAppendArgs{})
	labgob.Register(PutAppendReply{})
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
	kv.ownShards = make(map[int]int)
	kv.pollTicker = time.NewTicker(pollInterval)

	for i := 0; i < shardmaster.NShards; i++ {
		kv.data[i] = make(Shard)
	}
	kv.readSnapshot(kv.rf.ReadSnapshot())
	if kv.nextConfig > kv.config.Num {
		kv.nextConfig = kv.config.Num
	}
	go kv.run()
	go kv.poll()
	return kv
}
