package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"math/rand"
	"sync"
	"time"

	"MIT6.824/labrpc"

	"MIT6.824/labgob"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool  // true if the ApplyMsg contains a newly committed log entry
	Command      interface{}
	CommandIndex int
	CommitTerm   int // the term of the Raft when the entry was committed
	Role         string // the role of the Raft when the entry was committed
}

//term
const(
	Leader = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"
	votedNull = -1 // no vote given to anyone in this round
	heartBeat = time.Duration(100) // leader's heartbeat time
	RPC_CALL_TIMEOUT = time.Duration(500) * time.Millisecond // rpc timeout time
)

type Entries struct{
	Term    int  // The term to which this log entry belongs
	Index   int  // The index of this log entry in the log
	Command interface{}
}
//
// A Go object implementing a single Raft peer.
//
// Each Raft peer is called a server, and is divided into three roles: leader, candidate, and follower. However, the internal state is the same for all roles.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	role       string // Leader or candidate or follower
	// int32 is used for persisting
	currentTerm int    // The term to which this server belongs
	votedFor    int    // The index of the server voted for, initialized to -1, indicating that this follower's vote is still available and has not been cast to anyone else

	logs        []Entries // Save the executed commands, starting from index 1

	commitIndex int // The index of the last committed log entry, starting from index 0
	lastApplied int // The index of the last log entry applied to the state machine, starting from index 0

	// Leader-specific, reinitialized after each election
	nextIndex  []int // Save the next log index to be sent to each follower. Initialized to the index of the last log entry of the leader + 1
	matchIndex []int // For each follower, the index of the last log entry known to be replicated to that follower, initialized to 0. It is also equivalent to the last committed log entry of the follower

	appendCh   chan bool // Used by followers to determine whether a heartbeat signal has been received within the election timeout period
	voteCh     chan bool // Restart the timer after voting
	exitCh     chan bool // Terminate the instance
	leaderCh   chan bool // Candidate competes for leader

	applyCh    chan ApplyMsg // Execute the command when a log is committed. In the experiment, executing the command means sending a message to applyCh

	lastIncludedIndex int // The index of the last log entry corresponding to the existing snapshot
	lastIncludedTerm  int // The term to which the last log entry corresponding to the existing snapshot belongs

}

// Get a random timeout duration for elections
func (rf *Raft) electionTimeout() time.Duration{
	rtime := 300 + rand.Intn(150)
	// Random time: basicTime + rand.Intn(randNum)
	timeout := time.Duration(rtime)  * time.Millisecond
	return timeout
}


// Return the currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	// Lock is used because a server's term may change due to timeout or obtaining a majority of votes
	rf.mu.Lock()
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	// DPrintf(rf.log, "WarnRaft", "Server:%3d role:%12s isleader:%t\n", rf.me, rf.role, isleader)
	rf.mu.Unlock()
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	enc.Encode(rf.lastIncludedIndex)
	enc.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
	InfoRaft.Printf("Raft:%2d term:%3d | Persist data! Size:%5v logs:%4v\n", rf.me, rf.currentTerm, len(data), len(rf.logs)-1)
}


//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	dec := labgob.NewDecoder(r)

	var term	int
	var votedFor	int
	var logs []Entries
	var lastIncludedIndex int
	var lastIncludedTerm int

	if dec.Decode(&term) != nil || dec.Decode(&votedFor) !=nil || dec.Decode(&logs) != nil || dec.Decode(&lastIncludedIndex) != nil || dec.Decode(&lastIncludedTerm) != nil{
		InfoRaft.Printf("Raft:%2d term:%3d | Failed to read persist data!\n")
	}else{
		rf.currentTerm = term
		rf.votedFor = votedFor
		rf.logs = logs
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		rf.lastApplied = rf.lastIncludedIndex
		rf.commitIndex = rf.lastIncludedIndex
		InfoRaft.Printf("Raft:%2d term:%3d | Read persist data{%5d bytes} successful! VotedFor:%3d len(Logs):%3d\n",
			rf.me, rf.currentTerm, len(data), rf.votedFor, len(rf.logs))
	}
}


// field names must start with capital letters!
type AppendEntriesArgs struct{
	Term         int // Leader's term
	LeaderId     int
	// PreLogIndex and PrevLogTerm are used to determine the previous synchronized information between the leader and the follower
	// for rollback or overwriting follower logs by the new leader
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogTerm  int // term of prevLogIndex entry
	Entries      []Entries // log entries to store (empty for heartbeat; may send more than one for efficiency)
	LeaderCommit int // leader's commitIndex
}

// field names must start with capital letters!
type AppendEntriesReply struct {
	Term          int  // The currentTerm of the follower that received the message, for the expired leader to update the information.
	Success       bool // true if the follower contained an entry matching prevLogIndex and PrevLogTerm

	// The index of the first log entry in the follower that has a different term from args.Term.
	// One append RPC can exclude a conflicting term.
	// If there are several terms between the follower and the leader,
	// as long as the last log entry in the leader with the same conflictTerm is found,
	// all missing logs in the follower can be added at once.
	ConflictIndex int // The index of the conflicting log entry
	ConflictTerm  int // The term of the conflicting log entry (term does not match or follower has fewer logs)
}

func min(a, b int) int {
	if a < b{
		return a
	}
	return b
}
func max(a, b int) int {
	if a < b{
		return b
	}
	return a
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()


	reply.Term = rf.currentTerm
	reply.Success = false
	reply.ConflictIndex = -1
	reply.ConflictTerm = -1

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		reply.Term = rf.currentTerm
		rf.convertRoleTo(Follower)
	}
	rf.dropAndSet(rf.appendCh)

	if rf.getLastLogIndex() < args.PrevLogIndex{
		reply.ConflictTerm = -1
		reply.ConflictIndex = rf.getLastLogIndex() + 1
		InfoRaft.Printf("Raft:%2d term:%3d | receive leader:[%3d] message but lost any message! curLen:%4d prevLoglen:%4d len(Entries):%4d\n",
			rf.me, rf.currentTerm, args.LeaderId, rf.getLastLogIndex(), args.PrevLogIndex, len(args.Entries))
		return
		}

	if rf.lastIncludedIndex > args.PrevLogIndex{
		reply.ConflictIndex = rf.lastIncludedIndex + 1
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogTerm != rf.logs[rf.subIdx(args.PrevLogIndex)].Term{
		reply.ConflictTerm = rf.logs[rf.subIdx(args.PrevLogIndex)].Term
		for i := args.PrevLogIndex; i > rf.lastIncludedIndex ; i--{
			if rf.logs[rf.subIdx(i)].Term != reply.ConflictTerm{
				break
			}
			reply.ConflictIndex = i
		}
		InfoRaft.Printf("Raft:%2d term:%3d | receive leader:[%3d] message but not match!\n", rf.me, rf.currentTerm, args.LeaderId)
		return
	}

	rf.currentTerm = args.Term
	reply.Success = true

	i := 0
	for  ; i < len(args.Entries); i++{
		ind := rf.subIdx(i + args.PrevLogIndex + 1)
		if ind < len(rf.logs) && rf.logs[ind].Term != args.Entries[i].Term{
			rf.logs = rf.logs[:ind]
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}else if ind >= len(rf.logs){
			rf.logs = append(rf.logs, args.Entries[i:]...)
			break
		}
	}


	if len(args.Entries) != 0{
		rf.persist()
		InfoRaft.Printf("Raft:%2d term:%3d | receive new command from leader:%3d, term:%3d, size:%3d curLogLen:%4d LeaderCommit:%4d rfCommit:%4d\n",
			rf.me, rf.currentTerm, args.LeaderId, args.Term, len(args.Entries), len(rf.logs)-1, args.LeaderCommit, rf.commitIndex)
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.addIdx(len(rf.logs)-1))
		rf.applyLogs()
		}



}


//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int // The id of the candidate who sends the vote, which is the index of the server here
	// LastLogIndex and LastLogTerm are used together to compare who is "newer" between the candidate and the follower
	LastLogIndex int // The index of the last log entry of the candidate who sends the vote
	LastLogTerm  int // The term of the last log entry of the candidate who sends the vote
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // The currentTerm of the receiver, usually for the expired leader to update the information.
	// If candidate term < receiver term ==> false
	// If receiver's votedFor == (null or candidateId)
	// and candidate's log is as "new" as the receiver's log ==> true, indicating that I have voted for you
	// The receiver will change its votedFor after voting.
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if rf.currentTerm < args.Term{
		rf.currentTerm = args.Term
		rf.convertRoleTo(Follower)
	}

	newerEntries := args.LastLogTerm > rf.getLastLogTerm() || (args.LastLogTerm == rf.getLastLogTerm() && args.LastLogIndex >= rf.getLastLogIndex())
	voteOrNot := rf.votedFor == votedNull || rf.votedFor == args.CandidateId

	if newerEntries && voteOrNot{
		rf.votedFor = args.CandidateId
			reply.VoteGranted = true

			rf.dropAndSet(rf.voteCh)
			WarnRaft.Printf("Raft:%2d term:%3d | vote to candidate %3d\n", rf.me, rf.currentTerm, args.CandidateId)
			rf.persist()
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.addIdx(len(rf.logs)-1)
}

func (rf *Raft) getLastLogTerm() int {
	return rf.logs[len(rf.logs)-1].Term
}

func (rf *Raft) getPrevLogIndex(server int) int{
	return rf.nextIndex[server] - 1
}

func (rf *Raft) getPrevLogTerm(server int) int{
	return rf.logs[rf.subIdx(rf.getPrevLogIndex(server))].Term
}

func (rf *Raft) subIdx(i int) int{
	return i - rf.lastIncludedIndex
}

func (rf *Raft) addIdx(i int) int{
	return i + rf.lastIncludedIndex
}

func (rf *Raft)checkState(role string, term int) bool{
	return rf.role == role && rf.currentTerm == term
}

func (rf *Raft)convertRoleTo(role string){
	defer rf.persist()
	switch role {
	case Leader:
		WarnRaft.Printf("Raft:%2d term:%3d | %12s convert role to Leader!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Leader
		for i := 0; i < len(rf.peers); i++{
			if i == rf.me{
				continue
			}
			rf.nextIndex[i] = len(rf.logs) + rf.lastIncludedIndex
			rf.matchIndex[i] = 0
		}
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		rf.votedFor = rf.me
		WarnRaft.Printf("Raft:%2d term:%3d | %12s convert role to Candidate!\n", rf.me, rf.currentTerm, rf.role)
		rf.role = Candidate
	case Follower:
		WarnRaft.Printf("Raft:%2d term:%3d | %12s convert role to Follower!\n", rf.me, rf.currentTerm, rf.role)
		rf.votedFor = votedNull
		rf.role = Follower
	}
}

func (rf *Raft) dropAndSet(ch chan bool){

	select {
	case <- ch:
	default:
	}
	ch <- true
}

func (rf *Raft) applyLogs(){
	InfoRaft.Printf("Raft:%2d term:%3d | start apply log curCommit:%3d total:%3d!\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++{
		rf.applyCh <- ApplyMsg{true, rf.logs[rf.subIdx(i)].Command, i, rf.currentTerm, rf.role}
	}
	InfoRaft.Printf("Raft:%2d term:%3d | apply log {%4d => %4d} Done!\n", rf.me, rf.currentTerm, rf.lastApplied, rf.commitIndex)
	InfoRaft.Printf("Raft:%2d term:%3d | index{%4d} cmd{%v}\n", rf.me, rf.currentTerm, rf.commitIndex, rf.logs[rf.subIdx(rf.commitIndex)].Command)
	rf.lastApplied = rf.commitIndex
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}
//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	//InfoKV.Printf("me:%2d | Start wait!\n", rf.me)
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()


	term = rf.currentTerm
	isLeader = rf.role == Leader
	if isLeader{
		index = len(rf.logs) + rf.lastIncludedIndex
		rf.logs = append(rf.logs, Entries{rf.currentTerm, index, command})
		InfoRaft.Printf("Raft:%2d term:%3d | Leader receive a new command:%4v cmdIndex:%4v\n", rf.me, rf.currentTerm, command, index)

		rf.persist()
		}

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
	//WarnRaft.Printf(">== Someone prepare to kill\n")
	rf.exitCh <- true
	//WarnRaft.Printf("=>= Send signal kill\n")
	//rf.mu.Lock()
//	WarnRaft.Printf("==> Sever index:[%3d]  Term:[%3d]  role:[%10s] has been killed.Turn off its log\n", rf.me, rf.currentTerm, rf.role)
	//rf.mu.Unlock()
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = -1
	rf.votedFor = votedNull

	rf.role = Follower

	rf.logs = make([]Entries, 1)

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendCh = make(chan bool, 1)
	rf.voteCh = make(chan bool, 1)
	rf.exitCh = make(chan bool, 1)
	rf.leaderCh = make(chan bool, 1)

	rf.applyCh = applyCh

	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.logs[0] = Entries{rf.lastIncludedTerm, rf.lastIncludedIndex, -1}

	rand.Seed(time.Now().UnixNano())

	InfoRaft.Printf("Create a new Raft:[%3d]! term:[%3d]! Log length:[%4d]\n", rf.me,rf.currentTerm, rf.getLastLogIndex())

	go func() {
		Loop:
			for{
				select{
				case <- rf.exitCh:
					break Loop
				default:
				}

				rf.mu.Lock()
				role := rf.role
				eTimeout := rf.electionTimeout()
				rf.mu.Unlock()

				switch role{
				case Leader:
					rf.broadcastEntries()
					time.Sleep(heartBeat * time.Millisecond)
				case Candidate:
					go rf.leaderElection()
					select{
					case <- rf.appendCh:
					case <- rf.voteCh:
					case <- rf.leaderCh:
					case <- time.After(eTimeout):
						rf.mu.Lock()
						rf.convertRoleTo(Candidate)
						rf.mu.Unlock()
					}
				case Follower:
					select{
					case <- rf.appendCh:
					case <- rf.voteCh:
					case <- time.After(eTimeout):
						rf.mu.Lock()
						rf.convertRoleTo(Candidate)
						rf.mu.Unlock()
					}
				}
			}

	}()

	return rf
}

func (rf *Raft) leaderElection(){
	//candidate competes for leader
	rf.mu.Lock()
	//candidate term has already been incremented in changeRole
	//The request parameters sent to each node are the same.
	requestArgs := &RequestVoteArgs{rf.currentTerm, rf.me, rf.getLastLogIndex(), rf.getLastLogTerm()}
	rf.mu.Unlock()

	voteCnt := 1 //number of votes obtained, self vote is definitely for self
	voteFlag := true //only notify the channel once when receiving more than half of the votes
	voteL := sync.Mutex{}

	for followerId, _ := range rf.peers{
		if followerId == rf.me{
			continue
		}

		rf.mu.Lock()
		if !rf.checkState(Candidate, requestArgs.Term){
			//When sending the majority of votes, it is found that I am no longer a candidate
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		go func(server int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, requestArgs, reply); ok{
				//ok only means receiving a reply,
				//ok==false means the message sent this time is lost, or the reply message is lost

				rf.mu.Lock()
				defer rf.mu.Unlock()
				if reply.Term > rf.currentTerm{
					//There is a higher term
					rf.convertRoleTo(Follower)
					return
				}

				if !rf.checkState(Candidate, requestArgs.Term){
					//When receiving a vote, I am no longer a candidate
					return
				}

				if reply.VoteGranted{
					//Received a vote
					voteL.Lock()
					defer voteL.Unlock()
					voteCnt = voteCnt + 1
					if voteFlag && voteCnt > len(rf.peers)/2{
						voteFlag = false
						rf.convertRoleTo(Leader)
						rf.dropAndSet(rf.leaderCh)
					}
				}

			}
		}(followerId)
	}
}

func (rf *Raft) broadcastEntries() {
	rf.mu.Lock()
	curTerm := rf.currentTerm
	rf.mu.Unlock()

	commitFlag := true // Only modify the leader's logs once for majority commit
	commitNum := 1     // Record the number of nodes that have committed a certain log
	commitL := sync.Mutex{}

	for followerId, _ := range rf.peers {
		if followerId == rf.me {
			continue
		}

		go func(server int) {
			for {
				rf.mu.Lock()
				if !rf.checkState(Leader, curTerm) {
					rf.mu.Unlock()
					return
				}

				next := rf.nextIndex[server]
				if next <= rf.lastIncludedIndex{
					rf.sendSnapshot(server)
					return
				}

				appendArgs := &AppendEntriesArgs{curTerm,
					rf.me,
					rf.getPrevLogIndex(server),
					rf.getPrevLogTerm(server),
					rf.logs[rf.subIdx(next):],
					rf.commitIndex}

				rf.mu.Unlock()
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(server, appendArgs, reply);

				if !ok{
					return
				}

				rf.mu.Lock()
				if reply.Term > curTerm {
					rf.currentTerm = reply.Term
					rf.convertRoleTo(Follower)
					InfoRaft.Printf("Raft:%2d term:%3d | leader done! become follower\n", rf.me, rf.currentTerm)
					rf.mu.Unlock()
					return
				}

				if !rf.checkState(Leader, curTerm) || len(appendArgs.Entries) == 0{
					rf.mu.Unlock()
					return
				}

				if reply.Success {
					curCommitLen := appendArgs.PrevLogIndex +  len(appendArgs.Entries)

					if curCommitLen < rf.commitIndex{
						rf.mu.Unlock()
						return
					}

					if curCommitLen >= rf.matchIndex[server]{
						rf.matchIndex[server] = curCommitLen
						rf.nextIndex[server] = rf.matchIndex[server] + 1
					}

					commitL.Lock()
					defer commitL.Unlock()

					commitNum = commitNum + 1
					if commitFlag && commitNum > len(rf.peers)/2 {
						commitFlag = false

						rf.commitIndex = curCommitLen
						rf.applyLogs()
					}

					rf.mu.Unlock()
					return

				} else {

					if reply.ConflictTerm == -1{
						rf.nextIndex[server] = reply.ConflictIndex
					}else{
						rf.nextIndex[server] = rf.addIdx(1)
						i := reply.ConflictIndex
						for ; i > rf.lastIncludedIndex; i--{
							if rf.logs[rf.subIdx(i)].Term == reply.ConflictTerm{
								rf.nextIndex[server] = i + 1
								break
							}
						}
						if i <= rf.lastIncludedIndex && rf.lastIncludedIndex != 0{
							rf.nextIndex[server] = rf.lastIncludedIndex
						}
					}

					InfoRaft.Printf("Raft:%2d term:%3d | Msg to %3d fail,decrease nextIndex to:%3d\n",
						rf.me, rf.currentTerm, server, rf.nextIndex[server])
					rf.mu.Unlock()
				}
			}

		}(followerId)

	}
}

//===============================================================================================================
//snapshot
//===============================================================================================================
func (rf *Raft) TakeSnapshot(rawSnapshot []byte, appliedId int, term int){
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if appliedId <= rf.lastIncludedIndex{
		return
	}

	logs := make([]Entries, 0)
	logs = append(logs, rf.logs[rf.subIdx(appliedId):]...)

	rf.logs = logs
	rf.lastIncludedTerm = term
	rf.lastIncludedIndex = appliedId
	rf.persistStateAndSnapshot(rawSnapshot)
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte){
	w := new(bytes.Buffer)
	enc := labgob.NewEncoder(w)
	enc.Encode(rf.currentTerm)
	enc.Encode(rf.votedFor)
	enc.Encode(rf.logs)
	enc.Encode(rf.lastIncludedIndex)
	enc.Encode(rf.lastIncludedTerm)
	raftState := w.Bytes()
	rf.persister.SaveStateAndSnapshot(raftState, snapshot)
}

type InstallSnapshotArgs struct {
	Term 	int //leader's term
	LeaaderId 	int
	LastIncludedIndex	int
	LastIncludedTerm	int
	Data 	[]byte //snapshot
}

type InstallSnapshotReply struct{
	Term 	int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply){
	InfoKV.Printf("Raft:%2d term:%3d | receive snapshot from leader:%2d ", rf.me, rf.currentTerm, args.LeaaderId)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.currentTerm > args.Term || args.LastIncludedIndex <= rf.lastIncludedIndex{
		InfoKV.Printf("Raft:%2d term:%3d | stale snapshot from leader:%2d | me:{index%4d term%4d} leader:{index%4d term%4d}",
			rf.me, rf.currentTerm, args.LeaaderId, rf.lastIncludedIndex, rf.lastIncludedTerm, args.LastIncludedIndex, args.LastIncludedTerm)
		return
	}


	if args.Term > rf.currentTerm{
		rf.currentTerm = args.Term
		rf.convertRoleTo(Follower)
	}

	rf.dropAndSet(rf.appendCh)

	logs := make([]Entries, 0)
	if args.LastIncludedIndex <= rf.getLastLogIndex() {
		logs = append(logs, rf.logs[rf.subIdx(args.LastIncludedIndex):]...)
	}else{
		logs = append(logs, Entries{args.LastIncludedTerm,args.LastIncludedIndex,-1})
	}
	rf.logs = logs

	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm


	rf.lastApplied = max(rf.lastIncludedIndex, rf.lastApplied)
	rf.commitIndex = max(rf.lastIncludedIndex, rf.commitIndex)

	rf.persistStateAndSnapshot(args.Data)

	msg := ApplyMsg{
		false,
		args.Data,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		rf.role,
	}

	rf.applyCh <- msg

	InfoKV.Printf("Raft:%2d term:%3d | Install snapshot Done!\n", rf.me, rf.currentTerm)

}

func (rf *Raft) sendSnapshot(server int) {
	InfoKV.Printf("Raft:%2d term:%3d | Leader send snapshot{index:%4d term:%4d} to follower %2d\n", rf.me, rf.currentTerm, rf.lastIncludedIndex, rf.lastIncludedTerm, server)
	arg := InstallSnapshotArgs{
		rf.currentTerm,
		rf.me,
		rf.lastIncludedIndex,
		rf.lastIncludedTerm,
		rf.persister.ReadSnapshot(),
	}
	rf.mu.Unlock()

	repCh := make(chan struct{})
	reply := InstallSnapshotReply{}

	go func() {
		if ok := rf.peers[server].Call("Raft.InstallSnapshot", &arg, &reply); ok{
			repCh <- struct{}{}
		}
	}()

	select{
	case <- time.After(RPC_CALL_TIMEOUT):
		InfoKV.Printf("Raft:%2d term:%3d | Timeout! Leader send snapshot to follower %2d failed\n", arg.LeaaderId, arg.Term, server)
		return
	case <- repCh:
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm{
		rf.currentTerm = reply.Term
		rf.convertRoleTo(Follower)
		return
	}

	if !rf.checkState(Leader, arg.Term){
		return
	}
	rf.nextIndex[server] = arg.LastIncludedIndex + 1
	rf.matchIndex[server] = arg.LastIncludedIndex

}