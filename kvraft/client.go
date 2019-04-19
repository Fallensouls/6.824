package raftkv

import (
	"crypto/rand"
	"log"
	"math/big"

	"github.com/Fallensouls/raft/raft"

	"github.com/Fallensouls/raft/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	id         string // id of the client
	seq        uint64 // serial number to latest command
	lastLeader int    // server turned out to be the leader for the last rpc
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = raft.RandomID(8)
	ck.seq = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	req := GetRequest{Key: key}
	for {
		var res GetResponse
		// send rpc to the last leader first.
		ok := ck.servers[ck.lastLeader].Call("KVServer.Get", &req, &res)
		if !ok || res.WrongLeader {
			//log.Printf("retry: %v", retry)
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			//log.Println(res)
			continue
		}
		if res.Err == ErrPartitioned {
			continue
		}
		return res.Value
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	req := PutAppendRequest{Key: key, Value: value, Op: op, ID: ck.id, Seq: ck.seq}
	log.Printf("request: %v", req)
	for {
		var res PutAppendResponse
		ok := ck.servers[ck.lastLeader].Call("KVServer.PutAppend", &req, &res)
		if !ok || res.WrongLeader {
			ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
			continue
		}
		if res.Err == ErrExecuted {
			break
		}
		if res.Err != OK {
			continue
		}
	}
	ck.seq++
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
