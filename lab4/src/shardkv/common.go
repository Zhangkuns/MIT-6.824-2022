package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running Raft.
// Shardctrler decides which group serves each shard.
// Shardctrler may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK                 = "OK"
	ErrNoKey           = "ErrNoKey"
	ErrWrongGroup      = "ErrWrongGroup"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrClientId        = "ErrClientId"
	ErrRequestId       = "ErrRequestId"
	ErrWrongConfig     = "ErrWrongConfig"
	ErrTimeout         = "ErrTimeout"
	ErrConflict        = "ErrConflict"
	ErrLocked          = "ErrLocked"
	ErrAbort           = "ErrAbort"
	ErrNoLockFound     = "ErrNoLockFound"
	ErrLockMismatch    = "ErrLockMismatch"
	ErrUnknown         = "ErrUnknown"
	ErrClientOrRequest = "ErrClientOrRequest"
)

type Err string

// PutAppendArgs
// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
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

// --- RPC 参数和返回值 ---
type PrewriteArgs struct {
	Key        string
	Value      string
	TxnStartTs uint64
	PrimaryKey string
	IntentType OperationIntentType
	ClientId   int64
	RequestId  int64
	ConfigNum  int // 客户端携带其已知的配置号
}
type PrewriteReply struct {
	Err              Err
	Conflict         bool
	CurrentConfigNum int // 服务器当前的配置号，便于客户端更新认知
}

type CommitArgs struct {
	Key        string
	TxnStartTs uint64
	CommitTs   uint64
	IsPrimary  bool
	ClientId   int64
	RequestId  int64
	ConfigNum  int
}
type CommitReply struct {
	Err              Err
	CurrentConfigNum int
}

type RollbackArgs struct {
	Key        string
	TxnStartTs uint64
	ClientId   int64
	RequestId  int64
	ConfigNum  int
}
type RollbackReply struct {
	Err              Err
	CurrentConfigNum int
}

type ReadArgs struct {
	Key       string
	ReadTs    uint64
	ClientId  int64
	RequestId int64
	ConfigNum int
}
type ReadReply struct {
	Err              Err
	Value            string
	CommittedAtTs    uint64
	LockedByOtherTx  bool
	LockTxnStartTs   uint64
	LockPrimaryKey   string
	CurrentConfigNum int
}

type DeliverShardDataArgs struct {
	SourceGid     int
	ShardId       int
	TxnStartTs    uint64
	ConfigNum     int
	DataToInstall ShardFullData
	ClientId      int64
	RequestId     int64
}

type DeliverShardDataReply struct {
	Err Err
}
