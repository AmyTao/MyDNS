package raftkv

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClerkID  int64 // clerk id
	CmdIndex int   // clerk's command index
}

type PutAppendReply struct {
	WrongLeader bool
	Err         Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClerkID  int64
	CmdIndex int
}

type GetReply struct {
	WrongLeader bool // true means the reply is not from the leader
	Err         Err
	Value       string
}
