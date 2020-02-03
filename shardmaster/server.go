package shardmaster

import (
	"log"
	"sort"
	"sync"
	"time"

	"github.com/Fallensouls/raft/labgob"
	"github.com/Fallensouls/raft/labrpc"
	"github.com/Fallensouls/raft/raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	executed  map[string]uint64
	configs   []Config // indexed by config num
	notifyMap sync.Map
	shutdown  chan struct{}
}

type Op struct {
	// Your data here.
	//Config Config
	ID   string
	Seq  uint64
	Type string // Join, Leave or Move

	Servers map[int][]string
	GIDs    []int
	Shard   int
	GID     int
}

func (sm *ShardMaster) lastConfig() *Config {
	return &sm.configs[len(sm.configs)-1]
}

func (sm *ShardMaster) start(id string, seq uint64, args interface{}) (wrongLeader bool) {
	sm.mu.Lock()
	seqExecuted := sm.executed[id]
	sm.mu.Unlock()

	if seqExecuted > seq {
		return
	}
	var op Op
	switch args := args.(type) {
	case *JoinArgs:
		op = Op{ID: args.ID, Seq: args.Seq, Type: "Join", Servers: args.Servers}
	case *LeaveArgs:
		op = Op{ID: args.ID, Seq: args.Seq, Type: "Leave", GIDs: args.GIDs}
	case *MoveArgs:
		op = Op{ID: args.ID, Seq: args.Seq, Type: "Move", Shard: args.Shard, GID: args.GID}
	default:
		return
	}
	index, _, isLeader := sm.rf.Start(op)
	if !isLeader {
		return true
	}
	done := make(chan struct{}, 1)
	sm.notifyMap.Store(index, done)
	defer sm.notifyMap.Delete(index)
	for {
		select {
		case <-done:
			return
		case <-time.After(100 * time.Millisecond):
			return
		}
	}
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	reply.WrongLeader = sm.start(args.ID, args.Seq, args)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	reply.WrongLeader = sm.start(args.ID, args.Seq, args)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	reply.WrongLeader = sm.start(args.ID, args.Seq, args)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if sm.executed[args.ID] > args.Seq {
		return
	}
	if sm.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}

	if sm.rf.Read() != nil {
		reply.WrongLeader = true
		return
	}
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
}

func (sm *ShardMaster) balance() {
	lastConfig := sm.lastConfig()
	if len(lastConfig.Groups) == 0 {
		for i := range lastConfig.Shards {
			lastConfig.Shards[i] = 0
		}
		return
	}
	avarage := NShards / len(lastConfig.Groups)
	if len(sm.configs[len(sm.configs)-2].Groups) == 0 {
		var gids []int
		for gid := range lastConfig.Groups {
			gids = append(gids, gid)
		}
		for i := range lastConfig.Shards {
			lastConfig.Shards[i] = gids[i%len(gids)]
		}
		return
	}
	oldAvarage := NShards / len(sm.configs[len(sm.configs)-2].Groups)
	diff, same, less := sm.findGroupChange()
	counts := sm.countShards()
	var movedShards []int
	// less groups
	if less {
		for _, gid := range diff {
			movedShards = append(movedShards, counts[gid]...)
		}

		var countMap PairList
		for _, gid := range same {
			var pair Pair
			pair.Key = gid
			if _, ok := counts[gid]; !ok {
				pair.Value = 0
			} else {
				pair.Value = len(counts[gid])
			}
			countMap = append(countMap, pair)
		}
		sort.Sort(countMap)
		for i, shard := range movedShards {
			lastConfig.Shards[shard] = countMap[i%len(countMap)].Key
		}
	} else { // more groups
		var gids []int
		for _, gid := range diff {
			gids = append(gids, gid)
		}
		for gid := range counts {
			if len(counts[gid]) > oldAvarage {
				movedShards = append(movedShards, counts[gid][0])
				counts[gid] = counts[gid][1:]
			}
		}
		for i := 0; len(movedShards) < avarage*len(diff); i++ {
			for _, shards := range counts {
				movedShards = append(movedShards, shards[i])
			}
		}
		movedShards = movedShards[:avarage*len(diff)]
		for i, shard := range movedShards {
			lastConfig.Shards[shard] = diff[i%len(diff)]
		}
	}
}

func (sm *ShardMaster) findGroupChange() (diff []int, same []int, less bool) {
	oldGroups := sm.configs[len(sm.configs)-2].Groups
	latestGroups := sm.lastConfig().Groups
	diffMap := make(map[int]bool)
	//var same []int
	for gid := range oldGroups {
		diffMap[gid] = true
	}

	for gid := range latestGroups {
		if _, ok := diffMap[gid]; ok {
			delete(diffMap, gid)
			same = append(same, gid)
		} else {
			diff = append(diff, gid)
		}
	}
	if diff == nil {
		less = true
		for gid := range diffMap {
			diff = append(diff, gid)
		}
	}
	return
}

func (sm *ShardMaster) countShards() map[int][]int {
	count := make(map[int][]int)
	for shard, gid := range sm.configs[len(sm.configs)-1].Shards {
		count[gid] = append(count[gid], shard)
	}
	return count
}

func (sm *ShardMaster) run() {
	for {
		select {
		case msg, ok := <-sm.applyCh:
			if ok && !msg.NoOpCommand {
				if op, ok := msg.Command.(Op); ok {
					if sm.executed[op.ID] < op.Seq {
						sm.mu.Lock()
						oldConfig := sm.lastConfig()
						newConfig := Config{Num: oldConfig.Num + 1, Shards: oldConfig.Shards, Groups: make(map[int][]string)}
						for gid, servers := range oldConfig.Groups {
							newConfig.Groups[gid] = servers
						}
						var needBalance bool
						switch op.Type {
						case "Join":
							for gid, servers := range op.Servers {
								newConfig.Groups[gid] = servers
							}
							needBalance = true
						case "Leave":
							for _, gid := range op.GIDs {
								delete(newConfig.Groups, gid)
							}
							needBalance = true
						case "Move":
							newConfig.Shards[op.Shard] = op.GID
						}
						sm.configs = append(sm.configs, newConfig)
						if needBalance {
							sm.balance()
						}
						sm.executed[op.ID] = op.Seq
						sm.mu.Unlock()
						if done, ok := sm.notifyMap.Load(msg.CommandIndex); ok {
							done := done.(chan struct{})
							done <- struct{}{}
						}
						log.Printf("last config: %v", sm.lastConfig())
					}
				}
			}
		case <-sm.shutdown:
			return
		}
	}
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
	close(sm.shutdown)
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.executed = make(map[string]uint64)
	sm.shutdown = make(chan struct{})
	go sm.run()

	return sm
}
