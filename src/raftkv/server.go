package raftkv

import (
	"bytes"
	"log"
	"sync"
	"time"

	"MIT6.824/raft"

	"MIT6.824/labgob"
	"MIT6.824/labrpc"
)

const (
	Debug = 0
	WaitPeriod = time.Duration(1000) * time.Millisecond //Request response wait timeout
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}


type Op struct {
	Method 	string //Put or Append or Get
	Key 	string
	Value 	string
	Clerk 	int64 //Sent by which clerk
	Index 	int // Which command of this clerk
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	maxraftstate int // snapshot if log grows this big
	clerkLog map[int64]int 	//Record the command number executed by each clerk
	kvDB 	map[string]string //Key value
	msgCh 	map[int]chan int //Message channel
	persister 	*raft.Persister

}


func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	op := Op{"Get", args.Key, "", args.ClerkID, args.CmdIndex}
	reply.Err = ErrNoKey
	reply.WrongLeader = true
	index, term, isLeader := kv.rf.Start(op)
	if !isLeader{
		return
	}

	kv.mu.Lock()
	if ind, ok := kv.clerkLog[args.ClerkID]; ok && ind >= args.CmdIndex{
		//The instruction has been executed
		reply.Value = kv.kvDB[args.Key]
		kv.mu.Unlock()
		reply.WrongLeader = false
		reply.Err = OK
		return
	}

	raft.InfoKV.Printf(("KVServer:%2d | leader msgIndex:%4d\n"), kv.me, index)
	//Create a new ch and put it in msgCh
	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	select{
	case <- time.After(WaitPeriod):
		//Not submitted when timeout
		raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Timeout!\n", kv.me, index, term)
	case msgTerm := <- ch:
		if msgTerm == term {
			//Execute 
			kv.mu.Lock()
			raft.InfoKV.Printf("KVServer:%2d | Get {index:%4d term:%4d} OK!\n", kv.me, index, term)
			if val, ok := kv.kvDB[args.Key]; ok{
				reply.Value = val
				reply.Err = OK
			}
			kv.mu.Unlock()
			reply.WrongLeader = false
		}else{
			raft.InfoKV.Printf("KVServer:%2d |Get {index:%4d term:%4d} failed! Not leader any more!\n", kv.me, index, term)
		}
	}

	go func() {kv.closeCh(index)}()
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	op := Op{args.Op, args.Key, args.Value, args.ClerkID, args.CmdIndex}
	reply.Err = OK
	kv.mu.Lock()

	//The follower receives the put append request that has been executed and returns directly.
	if ind, ok := kv.clerkLog[args.ClerkID]; ok && ind >= args.CmdIndex{
		//Has been executed
		kv.mu.Unlock()
		reply.WrongLeader = false
		return
	}
	kv.mu.Unlock()

	index, term, isLeader := kv.rf.Start(op)
	if !isLeader{
		reply.WrongLeader = true
		return
	}

	kv.mu.Lock()
	raft.InfoKV.Printf(("KVServer:%2d | leader msgIndex:%4d\n"), kv.me, index)
	ch := make(chan int)
	kv.msgCh[index] = ch
	kv.mu.Unlock()

	reply.WrongLeader = true
	select{
	case <- time.After(WaitPeriod):
		//Not submitted when timeout
		raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, timeout!\n", kv.me, index, term)
	case msgTerm := <- ch:
		if msgTerm == term {
			//The command is executed, or has been executed
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} OK!\n", kv.me, index, term)
			reply.WrongLeader = false
		}else{
			raft.InfoKV.Printf("KVServer:%2d | Put {index:%4d term:%4d} Failed, not leader!\n", kv.me, index, term)
		}
	}
	go func() {kv.closeCh(index)}()
}


func (kv *KVServer) Kill() {
	kv.rf.Kill()
	kv.mu.Lock()
	raft.InfoKV.Printf("KVServer:%2d | KV server is died!\n", kv.me)
	kv.mu.Unlock()
}


func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// servers[] contains the ports of the set of servers that will cooperate via Raft to form the fault-tolerant key/value service.
	labgob.Register(Op{})
	kv := new(KVServer)
	kv.me = me // me is the index of the current server in servers[].
	kv.maxraftstate = maxraftstate

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvDB = make(map[string]string)
	kv.clerkLog = make(map[int64]int)
	kv.msgCh = make(map[int]chan int)
	kv.persister = persister // the k/v server should store snapshots through the underlying Raft implementation, which should call persister.SaveStateAndSnapshot() to atomically save the Raft state along with the snapshot.
	kv.loadSnapshot()

	raft.InfoKV.Printf("KVServer:%2d | Create New KV server!\n", kv.me)
	go kv.receiveNewMsg()


	return kv
}

func (kv *KVServer) receiveNewMsg(){
		for msg := range kv.applyCh {
			kv.mu.Lock()
			//Execute instructions
			index := msg.CommandIndex
			term := msg.CommitTerm

			if !msg.CommandValid{
				//snapshot
				op := msg.Command.([]byte)
				kv.decodedSnapshot(op)
				kv.mu.Unlock()
				continue
			}

			op := msg.Command.(Op)

			if ind, ok := kv.clerkLog[op.Clerk]; ok && ind >= op.Index {
				//If clerk exists and the command has already been executed, do nothing

			}else{
				//Execute instructions
				kv.clerkLog[op.Clerk] = op.Index
				switch op.Method {
				case "Put":
					kv.kvDB[op.Key] = op.Value

				case "Append":
					if _, ok := kv.kvDB[op.Key]; ok {
						kv.kvDB[op.Key] = kv.kvDB[op.Key] + op.Value
					} else {
						kv.kvDB[op.Key] = op.Value
					}
					
				case "Get":
					
				}
			}
			if ch, ok := kv.msgCh[index]; ok{
				ch <- term
			}

			//Check the status after the instruction is executed
			kv.checkState(index, term)
			kv.mu.Unlock()
		}
}

func (kv *KVServer) closeCh(index int){
	kv.mu.Lock()
	defer kv.mu.Unlock()
	close(kv.msgCh[index])
	delete(kv.msgCh, index)
}

func (kv *KVServer) decodedSnapshot(data []byte){
	//When this function is called, the caller defaults to holding kv.mu
	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var db	map[string]string
	var cl  map[int64]int

	if dec.Decode(&db) != nil || dec.Decode(&cl) != nil{
		raft.InfoKV.Printf("KVServer:%2d | KV Failed to recover by snapshot!\n", kv.me)
	}else{
		kv.kvDB = db
		kv.clerkLog = cl
		raft.InfoKV.Printf("KVServer:%2d | KV recover frome snapshot successful! \n", kv.me)
	}
}

func (kv *KVServer) checkState(index int, term int){
	//Check raft log length
	if kv.maxraftstate == -1{
		return
	}

	portion := 2. / 3
	if float64(kv.persister.RaftStateSize()) < portion * float64(kv.maxraftstate){
		return
	}
	//Because takeSnapshot requires rf.mu, so use goroutine to prevent rf.mu from blocking.
	rawSnapshot := kv.encodeSnapshot()
	go func() {kv.rf.TakeSnapshot(rawSnapshot, index, term)}()
}

func (kv *KVServer) encodeSnapshot() []byte {
	//By default the caller has kv.mu
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(kv.kvDB)
	enc.Encode(kv.clerkLog)
	data := w.Bytes()
	return data
}

func (kv *KVServer) loadSnapshot(){
	data := kv.persister.ReadSnapshot()
	if len(data) == 0 {
		return
	}
	kv.decodedSnapshot(data)
}