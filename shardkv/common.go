package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrNoKey        = "ErrNoKey"
	ErrWrongGroup   = "ErrWrongGroup"
	ErrDataNotReady = "ErrDataNotReady"
	ErrWaitingData  = "ErrWaitingData"
	ErrExecuted     = "ErrExecuted"
	ErrTimeout      = "ErrTimeout"
	ErrPartitioned  = "ErrPartitioned"
	ErrShutdown     = "ErrShutdown"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Num   int
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ID  string
	Seq uint64
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Num int
	Key string
	// You'll have to add definitions here.
}

type GetReply struct {
	WrongLeader bool
	Err         Err
	Value       string
}

type PullArgs struct {
	ConfigNum int
	Shards    []int
}

type PullReply struct {
	WrongLeader bool
	Err         Err
	Shards      map[int]Shard
	Executed    map[string]uint64
}

type PullData struct {
	Shards   map[int]Shard
	Executed map[string]uint64
}
