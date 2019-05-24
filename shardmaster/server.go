package shardmaster

import (
	"log"
	"sync"

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
	executed map[string]uint64
	done     chan int
	configs  []Config // indexed by config num
}

type Op struct {
	// Your data here.
	Config Config
	ID     string
	Seq    uint64
}

func (sm *ShardMaster) lastConfig() Config {
	return sm.configs[len(sm.configs)]
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	if sm.executed[args.ID] > args.Seq {
		return
	}
	if sm.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	newConfig := Config{Groups: make(map[int][]string)}
	oldConfig := sm.lastConfig()
	sm.mu.Unlock()
	for key, value := range oldConfig.Groups {
		newConfig.Groups[key] = value
	}
	for key, value := range args.Servers {
		newConfig.Groups[key] = value
	}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Shards = oldConfig.Shards
	index, _, _ := sm.rf.Start(Op{Config: newConfig, ID: args.ID, Seq: args.Seq})
	for {
		select {
		case doneIndex := <-sm.done:
			log.Println(doneIndex)
			if doneIndex == index {
				return
			}
			//case <-timeout.C:
			//	timeout.Stop()
			//	res.Err = ErrTimeout
			//	return
		}
	}
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	if sm.executed[args.ID] > args.Seq {
		return
	}
	if sm.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	newConfig := Config{Groups: make(map[int][]string)}
	oldConfig := sm.lastConfig()
	sm.mu.Unlock()
	for key, value := range oldConfig.Groups {
		newConfig.Groups[key] = value
	}
	for _, gid := range args.GIDs {
		delete(newConfig.Groups, gid)
	}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Shards = oldConfig.Shards
	index, _, _ := sm.rf.Start(Op{Config: newConfig, ID: args.ID, Seq: args.Seq})
	//timeout := time.NewTimer(10 * raft.HeartBeatInterval)
	for {
		select {
		case doneIndex := <-sm.done:
			if doneIndex == index {
				return
			}
			//case <-timeout.C:
			//	timeout.Stop()
			//	res.Err = ErrTimeout
			//	return
		}
	}
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	if sm.executed[args.ID] > args.Seq {
		return
	}
	if sm.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	newConfig := Config{Groups: make(map[int][]string)}
	oldConfig := sm.lastConfig()
	sm.mu.Unlock()
	for key, value := range oldConfig.Groups {
		newConfig.Groups[key] = value
	}
	newConfig.Num = oldConfig.Num + 1
	newConfig.Shards = oldConfig.Shards
	newConfig.Shards[args.Shard] = args.GID
	index, _, _ := sm.rf.Start(Op{Config: newConfig, ID: args.ID, Seq: args.Seq})
	for {
		select {
		case doneIndex := <-sm.done:
			if doneIndex == index {
				return
			}
			//case <-timeout.C:
			//	timeout.Stop()
			//	res.Err = ErrTimeout
			//	return
		}
	}
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	if sm.executed[args.ID] > args.Seq {
		return
	}
	if sm.rf.State() != raft.Leader {
		reply.WrongLeader = true
		return
	}
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if sm.rf.Read() != nil {
		reply.WrongLeader = true
		return
	}
	if args.Num == -1 || args.Num >= len(sm.configs) {
		reply.Config = sm.configs[len(sm.configs)-1]
	} else {
		reply.Config = sm.configs[args.Num]
	}
	log.Printf("reply of query: %v", reply)
}

func (sm *ShardMaster) assignShards() {
	lastConfig := sm.lastConfig()
	avarage := NShards / len(lastConfig.Groups)
	oldAvarage := NShards / len(sm.configs[len(sm.configs)-2].Groups)
	//remainder := NShards % len(lastConfig.Groups)
	diff, same, less := sm.findGroupChange()
	counts := sm.countShards()
	var (
		shards []int
		gids   []int
	)
	// less groups
	if less {
		for _, gid := range diff {
			shards = append(shards, counts[gid]...)
		}
		for i, shard := range shards {
			sm.lastConfig().Shards[shard] = same[i%len(same)]
		}
	} else { // more groups
		for _, gid := range diff {
			gids = append(gids, gid)
		}
		var movedShards []int
		for gid := range counts {
			if len(counts[gid]) > oldAvarage {
				movedShards = append(movedShards, counts[gid][0])
				counts[gid] = counts[gid][1:]
			}
		}
	outer:
		for len(movedShards) < avarage*len(diff) {
			var i int
			for _, shards := range counts {
				movedShards = append(movedShards, shards[i])
				if len(movedShards) == avarage*len(diff) {
					break outer
				}
			}
			i++
		}
		for i, shard := range movedShards {
			sm.lastConfig().Shards[shard] = diff[i%len(diff)]
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

func (sm *ShardMaster) apply() {
	for {
		select {
		case msg, ok := <-sm.applyCh:
			if ok && !msg.NoOpCommand {
				if op, ok := msg.Command.(Op); ok {
					if sm.executed[op.ID] < op.Seq {
						sm.mu.Lock()
						sm.configs = append(sm.configs, op.Config)
						sm.assignShards()
						sm.executed[op.ID] = op.Seq
						sm.mu.Unlock()
						if sm.rf.State() == raft.Leader && !msg.Recover {
							sm.done <- msg.CommandIndex
						}
					}
				}
			}
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
	sm.done = make(chan int, 100)
	sm.executed = make(map[string]uint64)
	go sm.apply()

	return sm
}
