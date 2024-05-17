package raftkv

import (
	"context"
	"crypto/rand"
	"math/big"
	"net"

	"MIT6.824/labrpc"
	"MIT6.824/raft"
)


type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	leader 	int // record the leader server index
	me 	int64 // clerk id
	cmdIndex 	int // clerk's command index

	Listen *net.UDPConn // UDP handle
	Context context.Context // Context for the clerk
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
	// You'll have to add code here.
	ck.leader = 0 // init leader
	ck.me = nrand()
	ck.cmdIndex = 0
	raft.InfoKV.Printf("Client:%20v  | Create new clerk!\n", ck.me)
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
	// You will have to modify this function.
	ck.cmdIndex++
	args := GetArgs{key, ck.me, ck.cmdIndex}
	leader := ck.leader
	raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Begin! Get:[%v] from server:%3d\n", ck.me, ck.cmdIndex, key, leader)


	for{
		reply := GetReply{}
		ok := ck.servers[leader].Call("KVServer.Get", &args, &reply)
		if ok && !reply.WrongLeader{
			ck.leader = leader
			if reply.Value == ErrNoKey{
				//kv DB no such key
				return ""
			}
			raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Successful! Get:[%v] from server:%3d value:[%v]\n", ck.me, ck.cmdIndex, key, leader, reply.Value)
			return reply.Value
		}
		// not leader, go to next server
		leader = (leader + 1) % len(ck.servers)
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
	// You will have to modify this function.
	ck.cmdIndex++
	args := PutAppendArgs{key, value, op, ck.me, ck.cmdIndex}
	leader := ck.leader
	raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Begin! %6s key:[%s] value:[%s] to server:%3d\n", ck.me, ck.cmdIndex, op, key, value, leader)

	for{
		reply := PutAppendReply{}
		if ok := ck.servers[leader].Call("KVServer.PutAppend", &args, &reply); ok && !reply.WrongLeader && reply.Err == OK{
			raft.InfoKV.Printf("Client:%20v cmdIndex:%4d| Successful! %6s key:[%s] value:[%s] to server:%3d\n", ck.me, ck.cmdIndex, op, key, value, leader)
			ck.leader = leader
			return
		}
		leader = (leader + 1) % len(ck.servers)
	}

}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

