package kvraft

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrClientOrRequest = "ErrClientOrRequest"
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
	// Client ID
	ClientId int64
	// Request ID
	RequestId int64
}

type PutAppendReply struct {
	Err         Err
	WrongLeader bool
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	// Client ID
	ClientId int64
	// Request ID
	RequestId int64
}

type GetReply struct {
	Err         Err
	Value       string
	WrongLeader bool
}
