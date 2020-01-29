package shardkv

import (
	"crypto/rand"
	"log"
	"math/big"
	"time"

	"github.com/Fallensouls/raft/labrpc"
	"github.com/Fallensouls/raft/raft"
	"github.com/Fallensouls/raft/shardmaster"
)

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardmaster to find out
// the assignment of shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

//
// which shard is a key in?
// please use this function,
// and please do not change it.
//
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardmaster.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardmaster.Clerk
	config   shardmaster.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	id  string
	seq uint64
}

//
// the tester calls MakeClerk.
//
// masters[] is needed to call shardmaster.MakeClerk().
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
//
func MakeClerk(masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(masters)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.id = raft.RandomID(8)
	ck.seq = 1
	ck.config = ck.sm.Query(-1)
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{Key: key}

	for {
		args.Num = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			// try each server for the shard.
		loop:
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply GetReply
				ok := srv.Call("ShardKV.Get", &args, &reply)
				log.Printf("ok: %v", ok)
				log.Printf("config num %v, get key: %s, reply: %v", ck.config.Num, key, reply)
				if ok {
					switch reply.Err {
					case OK, ErrNoKey:
						return reply.Value
					case ErrWrongGroup:
						break loop
						// case ErrPartitioned, ErrTimeout:
						// 	si--
					}
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}

	return ""
}

//
// shared by Put and Append.
// You will have to modify this function.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	args := PutAppendArgs{Key: key, Value: value, Op: op, ID: ck.id, Seq: ck.seq}

	for {
		args.Num = ck.config.Num
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		if servers, ok := ck.config.Groups[gid]; ok {
			for si := 0; si < len(servers); si++ {
				srv := ck.make_end(servers[si])
				var reply PutAppendReply
				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
				log.Printf("ok: %v", ok)
				log.Printf("put/append key: %s, reply: %v", key, reply)
				if ok {
					switch reply.Err {
					case OK, ErrExecuted:
						ck.seq++
						return
					case ErrWrongGroup:
						break
					// case ErrExecuted:
						// return
					}
				}
				// if ok && reply.WrongLeader == false && reply.Err == OK {
				// 	ck.seq++
				// 	return
				// }
				// if ok && (reply.Err == ErrWrongGroup || reply.Err == ErrExecuted) {
				// 	break
				// }
			}
		}
		time.Sleep(100 * time.Millisecond)
		// ask master for the latest configuration.
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
