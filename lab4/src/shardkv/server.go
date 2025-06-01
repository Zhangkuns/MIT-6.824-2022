package shardkv

import (
	"bytes"
	"fmt"
	"log"
	"mit6.5840/shardctrler"
	"sort"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"mit6.5840/labgob"
	"mit6.5840/labrpc"
	"mit6.5840/raft"
)

// 定义颜色常量（与 kvraft 一致）
const (
	// 新增的颜色常量，按 GID 大类，再按 kv.me 细分，再按 Leader/Follower 区分
	// GID % 5 == 0 (例如 GID 100, 105, ...) -> 绿色系 (基于原 colorS0)
	colorG0M0Follower = "\033[38;2;173;255;47m"  // GreenYellow (亮绿)
	colorG0M0Leader   = "\033[38;2;85;107;47m"   // DarkOliveGreen (暗橄榄绿)
	colorG0M1Follower = "\033[38;2;144;238;144m" // LightGreen (浅绿 - 原 colorS0Follower)
	colorG0M1Leader   = "\033[38;2;34;139;34m"   // ForestGreen (森林绿 - 原 colorS0Leader)
	colorG0M2Follower = "\033[38;2;60;179;113m"  // MediumSeaGreen (中海绿)
	colorG0M2Leader   = "\033[38;2;0;100;0m"     // DarkGreen (深绿)

	// GID % 5 == 1 (例如 GID 101, 106, ...) -> 红色系 (基于原 colorS1)
	colorG1M0Follower = "\033[38;2;255;228;225m" // MistyRose (薄雾玫瑰色 - 非常浅的粉红，增加区分度)
	colorG1M0Leader   = "\033[38;2;255;105;180m" // HotPink (亮粉)
	colorG1M1Follower = "\033[38;2;255;182;193m" // LightPink (浅红 - 原 colorS1Follower)
	colorG1M1Leader   = "\033[38;2;178;34;34m"   // Firebrick (耐火砖红 - 原 colorS1Leader)
	colorG1M2Follower = "\033[38;2;240;128;128m" // LightCoral (浅珊瑚色)
	colorG1M2Leader   = "\033[38;2;220;20;60m"   // Crimson (深红)

	// GID % 5 == 2 (例如 GID 102, 107, ...) -> 蓝色系 (基于原 colorS2)
	colorG2M0Follower = "\033[38;2;173;216;230m" // LightBlue (淡蓝)
	colorG2M0Leader   = "\033[38;2;30;144;255m"  // DodgerBlue (道奇蓝)
	colorG2M1Follower = "\033[38;2;135;206;235m" // SkyBlue (天蓝 - 原 colorS2Follower)
	colorG2M1Leader   = "\033[38;2;65;105;225m"  // RoyalBlue (皇家蓝 - 原 colorS2Leader)
	colorG2M2Follower = "\033[38;2;100;149;237m" // CornflowerBlue (矢车菊蓝)
	colorG2M2Leader   = "\033[38;2;0;0;205m"     // MediumBlue (中蓝)

	// GID % 5 == 3 (例如 GID 103, 108, ...) -> 黄色/橙色系 (基于原 colorS3)
	colorG3M0Follower = "\033[38;2;255;255;224m" // LightYellow (浅黄 - 原 colorS3Follower)
	colorG3M0Leader   = "\033[38;2;255;215;0m"   // Gold (金色)
	colorG3M1Follower = "\033[38;2;255;228;181m" // Moccasin (鹿皮鞋色/浅橙黄)
	colorG3M1Leader   = "\033[38;2;218;165;32m"  // Goldenrod (金麒麟黄 - 原 colorS3Leader)
	colorG3M2Follower = "\033[38;2;255;165;0m"   // Orange (橙色)
	colorG3M2Leader   = "\033[38;2;255;140;0m"   // DarkOrange (深橙色 - 原 colorTest1)

	// GID % 5 == 4 (例如 GID 104, 109, ...) -> 紫色系 (基于原 colorS4)
	colorG4M0Follower = "\033[38;2;221;160;221m" // Plum (李子紫)
	colorG4M0Leader   = "\033[38;2;218;112;214m" // Orchid (兰紫)
	colorG4M1Follower = "\033[38;2;230;190;255m" // LightPurple (浅紫 - 原 colorS4Follower)
	colorG4M1Leader   = "\033[38;2;128;0;128m"   // Purple (深紫 - 原 colorS4Leader)
	colorG4M2Follower = "\033[38;2;186;85;211m"  // MediumOrchid (中兰紫)
	colorG4M2Leader   = "\033[38;2;148;0;211m"   // DarkViolet (深紫罗兰)

	// 其他测试信息的颜色
	colorTest1 = "\033[38;2;255;140;0m"   // Dark Orange (深橙色)
	colorTest2 = "\033[38;2;139;69;19m"   // Saddle Brown (马鞍棕色)
	colorTest3 = "\033[38;2;192;192;192m" // Silver (银色)
	colorTest4 = "\033[38;2;169;169;169m" // Dark Gray (深灰色)
	colorTest5 = "\033[38;2;255;0;255m"   // Magenta (品红色)
	colorTest6 = "\033[38;2;0;255;255m"   // Cyan (青色)
)

var EnableKVStoreLogging = true
var EnableServerLogging = true

type OperationType string
type OperationIntentType string

const (
	IntentPut    OperationIntentType = "Put"
	IntentAppend OperationIntentType = "Append"
)
const (
	OpPrewrite          OperationType = "Prewrite"
	OpCommit            OperationType = "Commit"
	OpRollback          OperationType = "Rollback"
	OpRead              OperationType = "Read"
	OpConfigChange      OperationType = "ConfigChange"      // 用于同步配置更新和分片状态
	OpApplyMigratedData OperationType = "ApplyMigratedData" // 用于应用迁移来的数据
)

// ShardStatus 定义了分片对于当前 ShardKV 组的几种可能状态
type ShardStatus string

const (
	ShardStatus_Unknown        ShardStatus = "Unknown"        // 初始或未知状态
	ShardStatus_Serving        ShardStatus = "Serving"        // 本组拥有此分片，数据完整，正在服务
	ShardStatus_WaitingForData ShardStatus = "WaitingForData" // 本组已在新配置中获得此分片，但正等待从前一个所有者接收数据
	ShardStatus_NotOwned       ShardStatus = "NotOwned"       // 根据当前配置，本组不拥有此分片 (或已彻底迁出并清理完毕)
	// ShardStatus_StagingData ShardStatus = 5 // (更高级) 目标组已收到数据但未激活服务，用于两阶段迁移确认
)

type ValueVersion struct {
	Data       string
	CommitTs   uint64
	TxnStartTs uint64
}

type LockInfo struct {
	TxnStartTs   uint64
	PrimaryKey   string
	LockHolderId string
	AcquiredTime int64
}

type WriteIntentInfo struct {
	Data       string
	TxnStartTs uint64
	PrimaryKey string
	Operation  OperationIntentType
}

type KVSnapshot struct {
	KvStore          map[string][]ValueVersion
	Locks            map[string]LockInfo
	WriteIntents     map[string]WriteIntentInfo
	Config           shardctrler.Config
	Shards           map[int]ShardStatus
	LastApplied      map[int64]int64
	LastAppliedIndex int
}

// ShardFullData 用于封装一个分片的完整数据
type ShardFullData struct {
	KvStore      map[string][]ValueVersion
	Locks        map[string]LockInfo
	WriteIntents map[string]WriteIntentInfo
}

type Op struct {
	Type       OperationType
	Key        string // 对于Prewrite, Commit, Rollback, Read
	Value      string // 用于 Prewrite
	TxnStartTs uint64
	CommitTs   uint64
	IsPrimary  bool
	PrimaryKey string
	IntentType OperationIntentType

	// 用于 OpConfigChange
	NewConfig   shardctrler.Config // 当Type为OpConfigChange时使用
	OldConfig   shardctrler.Config // 当Type为OpConfigChange时使用
	ShardsToOwn map[int]bool       // 当Type为OpConfigChange, 标记本GID应负责的新分片

	// 用于 OpApplyMigratedData
	ShardId     int           // 要应用数据的分片ID
	SourceGid   int           // Source GID
	DataToApply ShardFullData // 迁移来的完整分片数据
	ConfigNum   int           // 应用此数据所依据的配置号

	ClientId  int64
	RequestId int64
}

type OpResponse struct {
	Err            Err
	Value          string
	CommittedAtTs  uint64
	Conflict       bool
	LockTxnStartTs uint64
	LockPrimaryKey string
	ClientId       int64
	RequestId      int64
}

type ShardKV struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	maxraftstate int
	make_end     func(string) *labrpc.ClientEnd
	gid          int
	ctrlers      []*labrpc.ClientEnd
	mck          *shardctrler.Clerk

	kvStore     map[string][]ValueVersion
	locks       map[string]LockInfo
	writeIntent map[string]WriteIntentInfo

	config           shardctrler.Config
	Shards           map[int]ShardStatus // 当前此GID实际负责（已准备好服务）的分片
	lastApplied      map[int64]int64
	lastAppliedIndex int
	notifyChs        map[int]chan OpResponse

	// 用于Snapshot
	persister         *raft.Persister
	lastSnapshotIndex int
	snapshotCooldown  int
	serverId          int64

	// 用于获取时间戳
	tso_end *labrpc.ClientEnd
	// 用于加速
	leaderHints map[int]int // GID -> server Index
}

func (kv *ShardKV) Prewrite(args *PrewriteArgs, reply *PrewriteReply) {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: RPC Prewrite received for Key '%s', TxnStartTs %d, Client %d, Req %d, ClientConfig %d"+colorReset, kv.serverId, kv.gid, args.Key, args.TxnStartTs, args.ClientId, args.RequestId, args.ConfigNum)
	}
	kv.mu.Lock()
	reply.CurrentConfigNum = kv.config.Num
	if kv.config.Num < args.ConfigNum && args.ConfigNum > 0 {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' REJECTED. Server config %d < client config %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		reply.Err = ErrWrongConfig
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)
	shardstatus := kv.Shards[shard]
	if shardstatus != ShardStatus_Serving {
		if shardstatus == ShardStatus_NotOwned {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' (shard %d) REJECTED. Shard not owned by GID %d in config %d. Owned: %v. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, kv.gid, kv.config.Num, kv.Shards, args.ClientId, args.RequestId)
		}
		if shardstatus == ShardStatus_WaitingForData {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' (shard %d) REJECTED. Shard is waiting for data. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, args.ClientId, args.RequestId)
		}
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:       OpPrewrite,
		Key:        args.Key,
		Value:      args.Value,
		TxnStartTs: args.TxnStartTs,
		PrimaryKey: args.PrimaryKey,
		IntentType: args.IntentType,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' REJECTED. Error wrong leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' is the leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' waiting for kv.mu.Lock. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
	kv.mu.Lock()
	ch := make(chan OpResponse, 1)
	kv.notifyChs[index] = ch
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' made channel for at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if ch, ok := kv.notifyChs[index]; ok {
			close(ch)
			delete(kv.notifyChs, index)
		}
		// 确保我们只删除和关闭我们自己创建的channel
		kv.mu.Unlock()
	}()

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Key '%s' Waiting for notification at at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)

	select {
	case response := <-ch:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			if response.Err == OK {
				reply.Err = OK
				reply.Conflict = response.Conflict
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite Op for Raft Index %d (ReqID: %d) done."+colorReset, kv.serverId, kv.gid, index, op.RequestId)
				}
			} else {
				reply.Err = response.Err
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite key=%s failed, error=%s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, response.Err, op.ClientId, op.RequestId)
				}
			}
		} else {
			// 如果 ClientId 或 RequestId 不匹配，返回错误
			if response.ClientId != op.ClientId {
				reply.Err = ErrClientId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite key=%s failed, wrong ClientId, op.ClientId %d != response.ClientId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, response.ClientId, op.ClientId, op.RequestId)
				}
			} else if response.RequestId != op.RequestId {
				reply.Err = ErrRequestId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite key=%s failed, wrong RequestId, op.RequestId %d != response.RequestId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.RequestId, response.RequestId, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = ErrUnknown // 这种情况不应该发生，但为了安全起见
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite key=%s failed with unknown error. clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			}
		}
	case <-time.After(800 * time.Millisecond):
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Prewrite key=%s TIMED OUT, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
		reply.Err = ErrTimeout // Or ErrWrongLeader, as timeout often implies leadership change or network partition
	}
}

func (kv *ShardKV) Commit(args *CommitArgs, reply *CommitReply) {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: RPC Commit received for Key '%s', TxnStartTs %d, Client %d, Req %d, ClientConfig %d"+colorReset, kv.serverId, kv.gid, args.Key, args.TxnStartTs, args.ClientId, args.RequestId, args.ConfigNum)
	}
	kv.mu.Lock()
	reply.CurrentConfigNum = kv.config.Num
	if kv.config.Num < args.ConfigNum && args.ConfigNum > 0 {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' REJECTED. Server config %d < client config %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		reply.Err = ErrWrongConfig
		kv.mu.Unlock()
		return
	}
	//shard := key2shard(args.Key)
	//if !kv.Shards[shard] {
	//	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' (shard %d) REJECTED. Shard not owned by GID %d in config %d. Owned: %v. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, kv.gid, kv.config.Num, kv.Shards, args.ClientId, args.RequestId)
	//	reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	kv.mu.Unlock()

	op := Op{
		Type:       OpCommit,
		Key:        args.Key,
		TxnStartTs: args.TxnStartTs,
		CommitTs:   args.CommitTs,
		IsPrimary:  args.IsPrimary,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' REJECTED. Error wrong leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' is the leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' waiting for kv.mu.Lock. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
	kv.mu.Lock()
	ch := make(chan OpResponse, 1)
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit write Key '%s' made channel for at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	kv.notifyChs[index] = ch
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if currentCh, ok := kv.notifyChs[index]; ok && currentCh == ch {
			delete(kv.notifyChs, index)
			close(ch)
		}
		kv.mu.Unlock()
	}()

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit Key '%s' Waiting for notification at at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	select {
	case response := <-ch:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			if response.Err == OK {
				reply.Err = OK
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s done, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = response.Err
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s failed, error=%s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, response.Err, op.ClientId, op.RequestId)
				}
			}
		} else {
			if response.ClientId != op.ClientId {
				reply.Err = ErrClientId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s failed, wrong ClientId, op.ClientId %d != response.ClientId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, response.ClientId, op.ClientId, op.RequestId)
				}
			} else if response.RequestId != op.RequestId {
				reply.Err = ErrRequestId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s failed, wrong RequestId, op.RequestId %d != response.RequestId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.RequestId, response.RequestId, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = ErrUnknown // 这种情况不应该发生，但为了安全起见
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s failed with unknown error. clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			}
		}
	case <-time.After(800 * time.Millisecond):
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Commit key=%s TIMED OUT, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) Rollback(args *RollbackArgs, reply *RollbackReply) {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: RPC Rollback received for Key '%s', TxnStartTs %d, Client %d, Req %d, ClientConfig %d"+colorReset, kv.serverId, kv.gid, args.Key, args.TxnStartTs, args.ClientId, args.RequestId, args.ConfigNum)
	}
	kv.mu.Lock()
	reply.CurrentConfigNum = kv.config.Num
	if kv.config.Num < args.ConfigNum && args.ConfigNum > 0 {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' REJECTED. Server config %d < client config %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		reply.Err = ErrWrongConfig
		kv.mu.Unlock()
		return
	}
	shard := key2shard(args.Key)
	shardstatus := kv.Shards[shard]
	if shardstatus != ShardStatus_Serving {
		if shardstatus == ShardStatus_NotOwned {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' (shard %d) REJECTED. Shard not owned by GID %d in config %d. Owned: %v. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, kv.gid, kv.config.Num, kv.Shards, args.ClientId, args.RequestId)
		}
		if shardstatus == ShardStatus_WaitingForData {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' (shard %d) REJECTED. Shard is waiting for data. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, args.ClientId, args.RequestId)
		}
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		Type:       OpRollback,
		Key:        args.Key,
		TxnStartTs: args.TxnStartTs,
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' REJECTED. Error wrong leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
		reply.Err = ErrWrongLeader
		return
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' is the leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' waiting for kv.mu.Lock. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
	kv.mu.Lock()
	ch := make(chan OpResponse, 1)
	kv.notifyChs[index] = ch
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' made channel for at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if currentCh, ok := kv.notifyChs[index]; ok && currentCh == ch {
			delete(kv.notifyChs, index)
			close(ch)
		}
		kv.mu.Unlock()
	}()

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback Key '%s' Waiting for notification at at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	select {
	case response := <-ch:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			if response.Err == OK {
				reply.Err = OK
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s done, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = response.Err
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s failed, error=%s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, response.Err, op.ClientId, op.RequestId)
				}
			}
		} else {
			if response.ClientId != op.ClientId {
				reply.Err = ErrClientId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s failed, wrong ClientId, op.ClientId %d != response.ClientId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, response.ClientId, op.ClientId, op.RequestId)
				}
			} else if response.RequestId != op.RequestId {
				reply.Err = ErrRequestId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s failed, wrong RequestId, op.RequestId %d != response.RequestId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.RequestId, response.RequestId, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = ErrUnknown // 这种情况不应该发生，但为了安全起见
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s failed with unknown error. clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			}
		}
	case <-time.After(800 * time.Millisecond):
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Rollback key=%s TIMED OUT, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
		reply.Err = ErrTimeout
	}
}

func (kv *ShardKV) Read(args *ReadArgs, reply *ReadReply) {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: RPC Read received for Key '%s', ReadTs %d, Client %d, Req %d, ClientConfig %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ReadTs, args.ClientId, args.RequestId, args.ConfigNum)
	}
	kv.mu.Lock()
	reply.CurrentConfigNum = kv.config.Num
	if kv.config.Num < args.ConfigNum && args.ConfigNum > 0 {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' REJECTED. Server config %d < client config %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		reply.Err = ErrWrongConfig
		kv.mu.Unlock()
		return
	}
	//shard := key2shard(args.Key)
	//if !kv.Shards[shard] {
	//	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' (shard %d) REJECTED. Shard not owned by GID %d in config %d. Owned: %v. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, shard, kv.gid, kv.config.Num, kv.Shards, args.ClientId, args.RequestId)
	//	reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	kv.mu.Unlock()

	op := Op{
		Type:       OpRead,
		Key:        args.Key,
		TxnStartTs: args.ReadTs, // ReadTs is effectively the transaction's start_ts
		ClientId:   args.ClientId,
		RequestId:  args.RequestId,
	}

	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' REJECTED. Error wrong leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
		return
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' is the leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' waiting for kv.mu.Lock. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, args.ClientId, args.RequestId)
	kv.mu.Lock()
	ch := make(chan OpResponse, 1)
	kv.notifyChs[index] = ch
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' made channel for at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if currentCh, ok := kv.notifyChs[index]; ok && currentCh == ch {
			delete(kv.notifyChs, index)
			close(ch)
		}
		kv.mu.Unlock()
	}()

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read Key '%s' Waiting for notification at at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.Key, index, args.ClientId, args.RequestId)
	select {
	case response := <-ch:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			if response.Err == OK {
				reply.Err = OK
				reply.Value = response.Value
				reply.CommittedAtTs = response.CommittedAtTs
				reply.LockedByOtherTx = response.LockTxnStartTs > 0
				reply.LockTxnStartTs = response.LockTxnStartTs
				reply.LockPrimaryKey = response.LockPrimaryKey
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s done, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = response.Err
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s failed, error: %s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, response.Err, op.ClientId, op.RequestId)
				}
			}
		} else {
			if response.ClientId != op.ClientId {
				reply.Err = ErrClientId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s failed, wrong ClientId, op.ClientId %d != response.ClientId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, response.ClientId, op.ClientId, op.RequestId)
				}
			} else if response.RequestId != op.RequestId {
				reply.Err = ErrRequestId
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s failed, wrong RequestId, op.RequestId %d != response.RequestId %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.RequestId, response.RequestId, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = ErrUnknown // 这种情况不应该发生，但为了安全起见
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s failed with unknown error. clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
				}
			}
		}
	case <-time.After(800 * time.Millisecond):
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Read key=%s TIMED OUT, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.Key, op.ClientId, op.RequestId)
		reply.Err = ErrTimeout
	}
}

// DeliverShardData
// New RPC Handler for receiving shard data
func (kv *ShardKV) DeliverShardData(args *DeliverShardDataArgs, reply *DeliverShardDataReply) {
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: RPC Deliver Shard %d from GID %d (Config %d) received, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, args.ConfigNum, args.ClientId, args.RequestId)

	kv.mu.Lock()
	// Destination group must be at least at the config number under which the source is sending
	if kv.config.Num < args.ConfigNum {
		reply.Err = ErrWrongConfig // Or ErrStaleConfig
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for Shard %d (MigID %d) REJECTED. My config %d older than migration config %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.RequestId, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		kv.mu.Unlock()
		return
	}
	if kv.config.Num > args.ConfigNum {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for Shard %d (MigID %d) IGNORED. My config %d newer than migration config %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.RequestId, kv.config.Num, args.ConfigNum, args.ClientId, args.RequestId)
		//reply.Err = ErrWrongConfig
		reply.Err = OK
		kv.mu.Unlock()
		return
	}
	// According to my current (up-to-date or newer) config, am I supposed to own this shard?
	if kv.config.Shards[args.ShardId] != kv.gid {
		reply.Err = ErrWrongGroup
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for Shard %d REJECTED not assigned to me (GID %d) in my config %d (owner is %d), clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, kv.gid, kv.config.Num, kv.config.Shards[args.ShardId], args.ClientId, args.RequestId)
		kv.mu.Unlock()
		return
	}
	// Check if shard is already serving (should be false or in a waiting state)
	//if kv.Shards[args.ShardId] == true {
	//	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d is already serving, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, args.ClientId, args.RequestId)
	//	// Depending on strictness, could reject or just allow re-application if idempotent OpApplyMigratedData.
	//	// For now, proceed to propose, OpApplyMigratedData should be idempotent.
	//	reply.Err = ErrWrongGroup
	//	kv.mu.Unlock()
	//	return
	//}
	kv.mu.Unlock()

	op := Op{
		Type:        OpApplyMigratedData,
		SourceGid:   args.SourceGid,  // Source GID
		ShardId:     args.ShardId,    // Shard ID
		TxnStartTs:  args.TxnStartTs, // Transaction start timestamp
		ConfigNum:   args.ConfigNum,
		DataToApply: args.DataToInstall,
		ClientId:    args.ClientId,
		RequestId:   args.RequestId,
	}

	index, _, isLeaderAgain := kv.rf.Start(op)
	if !isLeaderAgain { // Check leader status again, as it might have changed after unlock
		reply.Err = ErrWrongLeader
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d is not leader, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, args.ClientId, args.RequestId)
		return
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d is the leader. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, args.ClientId, args.RequestId)

	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d waiting for kv.mu.Lock. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, args.ClientId, args.RequestId)
	kv.mu.Lock()
	ch := make(chan OpResponse, 1)
	kv.notifyChs[index] = ch
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d made channel for at Index %d. Client %d, Req %d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, index, args.ClientId, args.RequestId)
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if currentCh, ok := kv.notifyChs[index]; ok && currentCh == ch {
			delete(kv.notifyChs, index)
			close(ch)
		}
		kv.mu.Unlock()
	}()

	select {
	case response := <-ch:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			if response.Err == OK {
				reply.Err = OK
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d done, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, op.ClientId, op.RequestId)
				}
			} else {
				reply.Err = response.Err
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d failed, error: %s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, response.Err, op.ClientId, op.RequestId)
				}
			}
		} else {
			reply.Err = ErrClientOrRequest
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.SourceGid, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(2 * time.Second): // Migration apply might take longer
		reply.Err = ErrTimeout
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Deliver Shard %d from GID %d TIMEOUT, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, args.ShardId, args.SourceGid, op.ClientId, op.RequestId)
	}
}

// initiateShardSendingTasks
// now receives pre-collected data
// shardsAndTargets map[int]int: shardId -> targetGid
// dataToSend map[int]ShardFullData: shardId -> actual data collected synchronously
// effectiveConfig shardctrler.Config: Config under which this send is initiated
func (kv *ShardKV) initiateShardSendingTasks(shardsThatNeedDataSent map[int]int, dataToSendToGoroutine map[int]ShardFullData, effectiveConfig shardctrler.Config) {
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Initiating Shard Sending Tasks for config %d"+colorReset, kv.serverId, kv.gid, effectiveConfig.Num)
	var wg sync.WaitGroup
	for shardId, targetGid := range shardsThatNeedDataSent {
		shardActualData := dataToSendToGoroutine[shardId]
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Initiating send for shard %d to GID %d (under config %d)"+colorReset, kv.serverId, kv.gid, shardId, targetGid, effectiveConfig.Num)
		wg.Add(1)
		clientId := ((kv.serverId & 0xFFFF) * 1000000) + (int64(effectiveConfig.Num) * 1000) + int64(shardId)
		txnstartTs := kv.gettimestamp()
		requestId := (kv.serverId&0xFFFF)*1000000 + int64(txnstartTs)
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d to GID %d get TimeStamp %d"+colorReset, kv.serverId, kv.gid, shardId, targetGid, txnstartTs)
		args := DeliverShardDataArgs{
			SourceGid:     kv.gid,
			ShardId:       shardId,
			TxnStartTs:    txnstartTs,
			ConfigNum:     effectiveConfig.Num,
			DataToInstall: shardActualData,
			ClientId:      clientId,
			RequestId:     requestId,
		}

		targetServers := effectiveConfig.Groups[targetGid]
		if len(targetServers) == 0 {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: No servers found for target GID %d in config %d for shard %d. Migration might stall."+colorReset, kv.serverId, kv.gid, targetGid, effectiveConfig.Num, shardId)
			continue
		}
		go func(shardId int, targetGid int, args DeliverShardDataArgs) {
			defer wg.Done()

		sendAttemptLoop: // Loop for retrying sending this specific shard's data
			for {
				kv.mu.Lock()
				hintedLeaderIdx, hintExists := kv.leaderHints[targetGid] // 使用 kv.leaderHints
				kv.mu.Unlock()
				if servers, ok := effectiveConfig.Groups[targetGid]; ok {
					if hintExists && hintedLeaderIdx >= 0 && hintedLeaderIdx < len(servers) {
						for {
							srvName := servers[hintedLeaderIdx]
							srv := kv.make_end(srvName)
							reply := DeliverShardDataReply{}
							log.Printf(kv.GetServerColor()+"Srv %d GID %d: Attempt to send shard %d (config %d) to GID %d server %s, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, effectiveConfig.Num, targetGid, srvName, args.ClientId, args.RequestId, args.TxnStartTs)
							rpcOk := srv.Call("ShardKV.DeliverShardData", &args, &reply)
							if rpcOk {
								if reply.Err == OK {
									log.Printf(kv.GetServerColor()+"Srv %d GID %d: Successfully delivered shard %d (config %d) to GID %d server %s, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, effectiveConfig.Num, targetGid, srvName, args.ClientId, args.RequestId, args.TxnStartTs)
									break sendAttemptLoop
								} else if reply.Err == ErrWrongLeader {
									kv.mu.Lock()
									delete(kv.leaderHints, targetGid)
									kv.mu.Unlock()
									log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d (config %d) to GID %d server %s got %s. Trying next server in target group, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, args.ConfigNum, targetGid, srvName, reply.Err, args.ClientId, args.RequestId, args.TxnStartTs)
									break
								} else if reply.Err == ErrWrongConfig {
									log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d (config %d) to GID %d server %s failed. Target has different config (Err: %s), clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, args.ConfigNum, targetGid, srvName, reply.Err, args.ClientId, args.RequestId, args.TxnStartTs)
								} else {
									log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d (config %d) to GID %d server %s failed/error: %s. RPC rpcOk: %t, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, args.ConfigNum, targetGid, srvName, reply.Err, rpcOk, args.ClientId, args.RequestId, args.TxnStartTs)
								}
							} else {
								// RPC failed, retry with the next server
								kv.mu.Lock()
								delete(kv.leaderHints, targetGid)
								kv.mu.Unlock()
								log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d (config %d) to GID %d server %s failed/error: %s. RPC failed, rpcOk: %t, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, args.ConfigNum, targetGid, srvName, reply.Err, rpcOk, args.ClientId, args.RequestId, args.TxnStartTs)
								break
							}
							time.Sleep(100 * time.Millisecond)
						}
					}
					for si := 0; si < len(servers); si++ {
						srv := kv.make_end(servers[si])
						reply := DeliverShardDataReply{}
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Attempt to send shard %d (config %d) to GID %d server %s, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, effectiveConfig.Num, targetGid, servers[si], args.ClientId, args.RequestId, args.TxnStartTs)
						rpcOk := srv.Call("ShardKV.DeliverShardData", &args, &reply)
						if rpcOk && reply.Err == OK {
							log.Printf(kv.GetServerColor()+"Srv %d GID %d: Successfully delivered shard %d (migID %d) to GID %d server %s, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, requestId, targetGid, servers[si], args.ClientId, args.RequestId, args.TxnStartTs)
							kv.mu.Lock()
							kv.leaderHints[targetGid] = si
							kv.mu.Unlock()
							break sendAttemptLoop
						} else if rpcOk && (reply.Err == ErrWrongLeader || reply.Err == ErrTimeout) {
							log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d to GID %d server %s got %s. Trying next server in target group, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, targetGid, servers[si], reply.Err, args.ClientId, args.RequestId, args.TxnStartTs)
							continue // Try next server in the target group
						} else if rpcOk && reply.Err == ErrWrongConfig {
							log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d to GID %d server %s failed. Target has different config (Err: %s), clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, targetGid, servers[si], reply.Err, args.ClientId, args.RequestId, args.TxnStartTs)
							// break sendAttemptLoop // Stop trying for this shard, let next pollConfig re-evaluate
							continue
						} else {
							log.Printf(kv.GetServerColor()+"Srv %d GID %d: DeliverShardData for shard %d to GID %d server %s failed/error: %s. RPC rpcOk: %t, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, targetGid, servers[si], reply.Err, rpcOk, args.ClientId, args.RequestId, args.TxnStartTs)
							// Try next server or retry if RPC itself failed
						}
					}
				}
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Finished attempting all servers in GID %d for shard %d (config %d), clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, targetGid, shardId, effectiveConfig.Num, args.ClientId, args.RequestId, args.TxnStartTs)
				time.Sleep(150 * time.Millisecond) // Sleep before retrying the whole group for this shard
			} // End sendAttemptLoop for a shard
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Finished send attempts for shard %d to GID %d, clientId=%d, requestId=%d, txn=%d"+colorReset, kv.serverId, kv.gid, shardId, targetGid, args.ClientId, args.RequestId, args.TxnStartTs)
		}(shardId, targetGid, args) // Pass shardId and targetGid to the goroutine
	}
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: All shard sending goroutines for config %d have been LAUNCHED."+colorReset, kv.serverId, kv.gid, effectiveConfig.Num)
	wg.Wait() // Wait for all goroutines to finish
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Finished all shard sending tasks for config %d."+colorReset, kv.serverId, kv.gid, effectiveConfig.Num)
}

// --- 状态机应用逻辑 ---
func (kv *ShardKV) applyPercolatorOp(op Op) OpResponse {
	response := OpResponse{Err: OK} // Default to OK
	response.RequestId = op.RequestId
	response.ClientId = op.ClientId
	// Double check shard ownership for data operations at the moment of applying.
	// This is crucial because config could have changed between proposal and application.
	isDataOperation := op.Type == OpPrewrite || op.Type == OpCommit || op.Type == OpRollback || op.Type == OpRead
	if isDataOperation {
		shard := key2shard(op.Key)
		if kv.Shards[shard] != ShardStatus_Serving {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Op %v for Key '%s' (shard %d) failed at apply. Shard status %s != serving, config = %d"+colorReset, kv.serverId, kv.gid, op.Type, op.Key, shard, kv.Shards[shard], kv.config.Num)
			response.Err = ErrWrongGroup // Or ErrNotServingShard if more specific
			return response
		}
	}

	switch op.Type {
	case OpPrewrite:
		versions := kv.kvStore[op.Key]
		for _, v := range versions {
			if v.CommitTs > op.TxnStartTs {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply PREWRITE Key '%s' CONFLICT (WW). Last Value CommitTs %d > TxnStartTs %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, v.CommitTs, op.TxnStartTs, op.ClientId, op.RequestId)
				response.Err = ErrConflict
				response.Conflict = true
				return response
			}
		}
		if existingLock, locked := kv.locks[op.Key]; locked {
			if existingLock.TxnStartTs != op.TxnStartTs {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply PREWRITE Key '%s' CONFLICT (Locked by TxnStartTs %d). Current TxnStartTs %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, existingLock.TxnStartTs, op.TxnStartTs, op.ClientId, op.RequestId)
				response.Err = ErrLocked
				response.Conflict = true
				response.LockTxnStartTs = existingLock.TxnStartTs
				response.LockPrimaryKey = existingLock.PrimaryKey
				return response
			}
			// If same TxnStartTs, it's a retry. Update WriteIntent and Lock's AcquiredTime.
		}
		kv.locks[op.Key] = LockInfo{TxnStartTs: op.TxnStartTs, PrimaryKey: op.PrimaryKey, AcquiredTime: time.Now().UnixNano()}
		kv.writeIntent[op.Key] = WriteIntentInfo{Data: op.Value, TxnStartTs: op.TxnStartTs, PrimaryKey: op.PrimaryKey, Operation: op.IntentType}
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply PREWRITTEN Key '%s' Value '%s' TxnStartTs %d Operation %s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.Value, op.TxnStartTs, op.IntentType, op.ClientId, op.RequestId)

	case OpCommit:
		lock, locked := kv.locks[op.Key]
		// 如果没有锁，或者锁不属于当前事务，则处理潜在的幂等性或错误情况
		if !locked || lock.TxnStartTs != op.TxnStartTs {
			isCommitted := false
			for _, v := range kv.kvStore[op.Key] {
				// 这个条件分支意味着： a. !locked: 当前键 op.Key 上没有锁。 b. lock.TxnStartTs != op.TxnStartTs: 当前键 op.Key 上有锁，但该锁不属于发起此 Commit 操作的事务 （事务由其 TxnStartTs唯一标识）。
				// 检查是否该事务已经提交过了（为了保证 Commit 操作的幂等性）
				if v.TxnStartTs == op.TxnStartTs { // Check if this StartTs was committed
					if v.CommitTs == op.CommitTs { // And specifically with this CommitTs
						isCommitted = true
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Commit Key '%s' is finished, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId)
						break
					}
					// If v.CommitTs != op.CommitTs, it implies this TxnStartTs was committed but with a different CommitTs. This shouldn't happen with a sound TSO
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Commit Key '%s' CommitTs %d is finished but with different CommitTs %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.CommitTs, v.CommitTs, op.ClientId, op.RequestId)
				}
			}
			if isCommitted {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMIT Key '%s' for TxnStartTs %d (CommitTs %d) - Idempotent: Already committed, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.TxnStartTs, op.CommitTs, op.ClientId, op.RequestId)
				// response.Err is already OK
			} else {
				if !locked {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMIT Key '%s'failed. No lock found on key, clientId=%d, requestId=%d, Txn %d"+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs)
					response.Err = ErrNoLockFound
				} else { // lock.TxnStartTs != op.TxnStartTs
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMIT Key '%s' (C%d R%d, Txn %d) failed. Lock mismatch: key currently locked by Txn %d. Lock: %+v"+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs, lock.TxnStartTs, lock)
					response.Err = ErrLockMismatch
				}
			}
			return response
		}
		// 如果锁存在且匹配当前事务 (lock.TxnStartTs == op.TxnStartTs)
		intent, hasIntent := kv.writeIntent[op.Key]
		if !hasIntent || intent.TxnStartTs != op.TxnStartTs {
			// 如果没有找到写意图，或者写意图的 TxnStartTs 与当前事务不匹配，
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMIT Key '%s' for TxnStartTs %d failed. Write intent not found/mismatch, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.TxnStartTs, op.ClientId, op.RequestId)
			response.Err = ErrAbort
			delete(kv.locks, op.Key) // Clean up inconsistent lock
			return response
		}
		var finalValueToCommit string
		if intent.Operation == IntentAppend {
			currentCommittedValue := ""
			if versions, ok := kv.kvStore[op.Key]; ok && len(versions) > 0 {
				// versions are sorted by CommitTs descending, versions[0] is the latest.
				currentCommittedValue = versions[0].Data
			}
			finalValueToCommit = currentCommittedValue + intent.Data // intent.Data is value_to_append
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMITTING (Append) Key '%s': Current '%s' + ToAppend '%s' -> New '%s' (C%d R%d, Txn %d)"+colorReset, kv.serverId, kv.gid, op.Key, currentCommittedValue, intent.Data, finalValueToCommit, op.ClientId, op.RequestId, op.TxnStartTs)
		} else { // IntentPut or default
			if intent.Operation == IntentPut {
				finalValueToCommit = intent.Data // intent.Data is the new value for Put
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMITTING (Put) Key '%s': New Value '%s' (clientId=%d requestId=%d, Txn %d)"+colorReset, kv.serverId, kv.gid, op.Key, finalValueToCommit, op.ClientId, op.RequestId, op.TxnStartTs)
			} else {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMITTING Key '%s': Unknown intent operation '%s' (clientId=%d requestId=%d, Txn %d)"+colorReset, kv.serverId, kv.gid, op.Key, intent.Operation, op.ClientId, op.RequestId, op.TxnStartTs)
			}
		}
		// 执行提交：将写意图转换为永久的、已提交的版本
		newVersion := ValueVersion{Data: finalValueToCommit, CommitTs: op.CommitTs, TxnStartTs: op.TxnStartTs}
		kv.kvStore[op.Key] = append(kv.kvStore[op.Key], newVersion)
		// 保持版本列表按 CommitTs 降序排序，便于读取时快速找到最新可见版本
		sort.Slice(kv.kvStore[op.Key], func(i, j int) bool { return kv.kvStore[op.Key][i].CommitTs > kv.kvStore[op.Key][j].CommitTs })
		// 清理工作：删除写意图和锁
		delete(kv.writeIntent, op.Key)
		delete(kv.locks, op.Key)
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply COMMITTED Key '%s' Value '%s' for TxnStartTs %d at CommitTs %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, intent.Data, op.TxnStartTs, op.CommitTs, op.ClientId, op.RequestId)

	case OpRollback:
		// 1. 尝试获取当前操作键 op.Key 的锁信息
		lock, locked := kv.locks[op.Key]

		// 2. 检查锁是否存在并且是否属于当前请求回滚的事务
		if locked && lock.TxnStartTs == op.TxnStartTs {
			// 如果找到了锁，并且这个锁确实是当前事务的（TxnStartTs匹配）：
			// 清理写意图 (Write Intent)
			if intent, hasIntent := kv.writeIntent[op.Key]; hasIntent && intent.TxnStartTs == op.TxnStartTs {
				delete(kv.writeIntent, op.Key)
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply ROLLBACK Key '%s' (Client %d Req %d, Txn %d): Write intent deleted"+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs)
			} else if hasIntent {
				// 如果找到了写意图，但它不属于当前事务，这是一个潜在的不一致情况，记录下来。
				// 但主要的回滚逻辑还是基于锁。
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply ROLLBACK Key '%s' (Client %d Req %d, Txn %d): Found write intent for different Txn %d. Lock Txn: %d. Ignoring intent for this rollback."+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs, intent.TxnStartTs, lock.TxnStartTs)
			}
			// 删除锁
			delete(kv.locks, op.Key)
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply ROLLED BACK Key '%s' (Client %d Req %d, Txn %d): Lock deleted."+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs)
			// response.Err 默认为 OK，表示回滚操作（清理）已执行。
		} else {
			// 3. 如果没有找到匹配的锁（或者根本没有锁）
			//    a. 该事务的这个键可能已经被回滚过了。
			//    b. 该事务的这个键可能已经被成功提交了（这种情况下锁和写意图也应该不存在了）。
			//    c. 该事务从未在这个键上成功执行过 Prewrite 操作。
			//    d. 存在一个锁，但属于不同的事务。
			//    在这些情况下，对于当前的回滚请求，服务器无事可做或认为操作已隐式完成。
			//    Rollback 操作本身是幂等的。
			if locked { // 有锁，但不匹配 TxnStartTs
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply ROLLBACK Key '%s' (Client %d Req %d, Txn %d) - No action: Lock found but for different Txn %d (Expected %d). Lock: %+v"+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs, lock.TxnStartTs, op.TxnStartTs, lock)
			} else { // 完全没有锁
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply ROLLBACK Key '%s' (Client %d Req %d, Txn %d) - No action: No lock found. Assumed already processed or never locked by this Txn."+colorReset, kv.serverId, kv.gid, op.Key, op.ClientId, op.RequestId, op.TxnStartTs)
			}
			// response.Err 默认为 OK，因为回滚操作是幂等的。
			// 即使没有找到特定的锁或意图去“清理”，也认为针对此事务的回滚请求状态是“已回滚”或“无需回滚”。
		}

	case OpRead: // op.TxnStartTs for OpRead is the ReadTs
		if lock, locked := kv.locks[op.Key]; locked {
			if lock.TxnStartTs < op.TxnStartTs {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply READ Key '%s' (ReadTs %d) CONFLICT. Locked by earlier TxnStartTs %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.TxnStartTs, lock.TxnStartTs, op.ClientId, op.RequestId)
				response.Err = ErrLocked
				response.LockTxnStartTs = lock.TxnStartTs
				response.LockPrimaryKey = lock.PrimaryKey
				return response
			}
		}
		versions := kv.kvStore[op.Key]
		foundValue := false
		for _, v := range versions { // Assumes sorted by CommitTs descending
			if v.CommitTs < op.TxnStartTs {
				response.Value = v.Data
				response.CommittedAtTs = v.CommitTs
				foundValue = true
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply READ Key '%s' (ReadTs %d) - Found version value '%s' committed at %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.TxnStartTs, v.Data, v.CommitTs, op.ClientId, op.RequestId)
				break
			}
		}
		if !foundValue {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply READ Key '%s' (ReadTs %d) - No suitable version found, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.TxnStartTs, op.ClientId, op.RequestId)
			response.Err = ErrNoKey // Key exists, but no version visible at this timestamp
		}

	case OpApplyMigratedData:
		if op.ConfigNum != kv.config.Num {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Deliver Shard %d from GID %d REJECTED(opConf %d != Current config %d), clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.SourceGid, op.ConfigNum, kv.config.Num, op.ClientId, op.RequestId)
			response.Err = ErrWrongConfig
			return response
		}
		if kv.config.Shards[op.ShardId] != kv.gid {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Deliver Shard %d from GID %d REJECTED. Shard not owned by GID %d in current config %d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.SourceGid, kv.gid, kv.config.Num, op.ClientId, op.RequestId)
			response.Err = ErrWrongConfig
			return response
		}
		// Check if shard is already serving (should be false or in a waiting state)
		if kv.Shards[op.ShardId] == ShardStatus_Serving {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Deliver Shard %d from GID %d is already serving, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.SourceGid, op.ClientId, op.RequestId)
			// Depending on strictness, could reject or just allow re-application if idempotent OpApplyMigratedData.
			response.Err = ErrWrongGroup
			return response
		}
		// Check if shard is in a waiting state
		if kv.Shards[op.ShardId] != ShardStatus_WaitingForData {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply Deliver Shard %d (opConf %d) from GID %d REJECTED. Shard in %s != WaitingForData state, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.ConfigNum, op.SourceGid, kv.Shards[op.ShardId], op.ClientId, op.RequestId)
			response.Err = ErrWrongGroup
			return response
		}
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Trying to Apply Deliver Shard %d (opConf %d) from GID %d. Data: %d KVs, %d Locks, %d Intents, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.ConfigNum, op.SourceGid, len(op.DataToApply.KvStore), len(op.DataToApply.Locks), len(op.DataToApply.WriteIntents), op.ClientId, op.RequestId)
		// Clean existing data for this shard before applying new
		// (should ideally be empty if state was WaitingForData)
		for k := range kv.kvStore {
			if key2shard(k) == op.ShardId {
				delete(kv.kvStore, k)
			}
		}
		for k := range kv.locks {
			if key2shard(k) == op.ShardId {
				delete(kv.locks, k)
			}
		}
		for k := range kv.writeIntent {
			if key2shard(k) == op.ShardId {
				delete(kv.writeIntent, k)
			}
		}
		// 开始写入数据
		for k, vList := range op.DataToApply.KvStore {
			kv.kvStore[k] = vList
		}
		for k, lInfo := range op.DataToApply.Locks {
			kv.locks[k] = lInfo
		}
		for k, wiInfo := range op.DataToApply.WriteIntents {
			kv.writeIntent[k] = wiInfo
		}
		kv.mu.Lock()
		kv.Shards[op.ShardId] = ShardStatus_Serving // Start serving
		kv.mu.Unlock()
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d (opConf %d) from GID %d Start Serving, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.ShardId, op.ConfigNum, op.SourceGid, op.ClientId, op.RequestId)
		kv.printKVStore()

	case OpConfigChange:
		if op.NewConfig.Num <= kv.config.Num { // Stale config update proposal
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply OpConfigChange for config %d IGNORED. Current config %d is newer or same."+colorReset, kv.serverId, kv.gid, op.NewConfig.Num, kv.config.Num)
			return response // OK, but no state change
		}
		// Check if shard is already serving (should be false or in a waiting state)
		for i := 0; i < shardctrler.NShards; i++ {
			if kv.Shards[i] == ShardStatus_WaitingForData {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Apply OpConfigChange for config %d Failed. Shard %d status is %s, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.NewConfig.Num, i, kv.Shards[i], op.ClientId, op.RequestId)
				response.Err = ErrWrongGroup
				return response
			}
		}
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Try to Apply OpConfigChange from config %d to config %d."+colorReset, kv.serverId, kv.gid, kv.config.Num, op.NewConfig.Num)

		lastConfig := op.OldConfig  // 保存全局的上一个旧配置
		previousConfig := kv.config // 保存本地的上一个旧配置
		newConfig := op.NewConfig

		newShardServingStatus := make(map[int]ShardStatus) // 构建新的 kv.Shards 状态
		shardsThatNeedDataSent := make(map[int]int)        // 对于 Leader: ShardID -> new GID (记录需要发送数据的分片)
		shardsThatNeedDataReceived := make(map[int]int)    // 对于 Leader: ShardID -> old GID (记录需要等待数据的分片)

		// 2. 根据新旧配置，更新每个分片的服务状态 (kv.Shards)
		for i := 0; i < shardctrler.NShards; i++ {
			oldOwnerGid := previousConfig.Shards[i] // 原来的本地Config中的分片主人
			newOwnerGid := newConfig.Shards[i]      // 新的本地Config中的分片主人
			isfromzero := previousConfig.Num == 0 && previousConfig.Shards[i] == 0
			isNowMyResponsibility := (newOwnerGid == kv.gid) // 现在归我管
			wasMyResponsibility := (oldOwnerGid == kv.gid)   // 之前归我管
			isBelongZero := (oldOwnerGid == 0)               // 之前归 GID 0 管
			if isNowMyResponsibility && wasMyResponsibility && (!isBelongZero) {
				// 现在归我并且原来归我，一定是保留原本状态
				newShardServingStatus[i] = kv.Shards[i] // 保留之前的状态
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d RETAINED by GID %d. Serving status: %s"+colorReset, kv.serverId, kv.gid, i, kv.gid, newShardServingStatus[i])
			}
			if (!isNowMyResponsibility) && wasMyResponsibility && (!isBelongZero) {
				// 现在不归我，原来归我，一定是发送给制定的新GID
				newShardServingStatus[i] = ShardStatus_NotOwned
				shardsThatNeedDataSent[i] = newOwnerGid // 记录需要发送给哪个新GID
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d LOST by GID %d (to GID %d). Status: NotOwned. Data send pending."+colorReset, kv.serverId, kv.gid, i, kv.gid, newOwnerGid)
			}
			if isNowMyResponsibility && (!wasMyResponsibility) && (!isBelongZero) {
				// 现在归我，原来不归我，我不知道是否有数据需要迁移，只能进行等待
				newShardServingStatus[i] = ShardStatus_WaitingForData
				shardsThatNeedDataReceived[i] = oldOwnerGid // 记录需要从哪个旧GID接收数据
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d GAINED by GID %d (from GID %d). Status: WaitingForData."+colorReset, kv.serverId, kv.gid, i, kv.gid, oldOwnerGid)
			}
			if (!isNowMyResponsibility) && (!wasMyResponsibility) && (!isBelongZero) {
				// 现在不归我，原来不归我，与我无关
				newShardServingStatus[i] = ShardStatus_NotOwned
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d UNCHANGED (not owned by GID %d). Status: NotOwned."+colorReset, kv.serverId, kv.gid, i, kv.gid)
			}
			if (!isNowMyResponsibility) && (!wasMyResponsibility) && isBelongZero {
				// 现在不归我，原来不归我，之前是 GID 0 (未分配状态)，与我无关
				newShardServingStatus[i] = ShardStatus_NotOwned
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d UNCHANGED (not owned by GID %d). Status: NotOwned."+colorReset, kv.serverId, kv.gid, i, kv.gid)
			}
			if isNowMyResponsibility && (!wasMyResponsibility) && isBelongZero {
				// 现在归我，原来不归我，之前是 GID 0 (未分配状态)，无法判断
				if isfromzero {
					// 现在归我，原来是GID0(未分配状态)，我也是唯一的GID这是个全新的、空的分片。不需要等待数据，应该立即开始服务。
					newShardServingStatus[i] = ShardStatus_Serving
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d GAINED by GID %d (from GID 0/Unassigned). Status: Serving."+colorReset, kv.serverId, kv.gid, i, kv.gid)
				} else {
					newShardServingStatus[i] = ShardStatus_WaitingForData
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard %d GAINED by GID %d (from GID 0/Unassigned). Status: WaitingForData."+colorReset, kv.serverId, kv.gid, i, kv.gid)
				}
			}
		}
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shard serving status updated. New config: %+v, Old config: %+v"+colorReset, kv.serverId, kv.gid, kv.config, lastConfig)
		kv.mu.Lock()
		kv.Shards = newShardServingStatus // 原子更新所有分片的服务状态
		kv.config = op.NewConfig          // 关键：采纳新的全局配置
		kv.mu.Unlock()
		// 3. 处理数据迁移
		if _, isLeader := kv.rf.GetState(); isLeader {
			// 捕获当前配置，因为 kv.config 后续可能改变
			if len(shardsThatNeedDataSent) > 0 {
				// effectiveConfig 指的是触发这些发送任务的配置
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Collect Data and Start Shard (config %d) sending"+colorReset, kv.serverId, kv.gid, kv.config.Num)
				effectiveConfig := kv.config
				dataToSendToGoroutine := make(map[int]ShardFullData)
				for shardIdToCollect, _ := range shardsThatNeedDataSent {
					// 异步地从 kv.kvStore, kv.locks, kv.writeIntent 中收集 shardIdToCollect 的数据
					collectedData := kv.collectShardData(shardIdToCollect)
					dataToSendToGoroutine[shardIdToCollect] = collectedData
				}
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Finish Shard Collecting Tasks for config %d, shardsThatNeedDataSent: %v"+colorReset, kv.serverId, kv.gid, effectiveConfig.Num, shardsThatNeedDataSent)
				go kv.initiateShardSendingTasks(shardsThatNeedDataSent, dataToSendToGoroutine, effectiveConfig)
			}
		}

	default:
		log.Fatalf("Unknown op type: %v", op.Type)
	}
	return response
}

func (kv *ShardKV) applyLoop() {
	for !kv.killed() {
		msg, ok := <-kv.applyCh
		var opRes OpResponse // 存储操作的响应结果
		// var opAppliedOp Op   // 记录实际应用的 Raft 日志操作
		if !ok {
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: applyCh closed, exiting applyLoop"+colorReset, kv.serverId, kv.gid)
			}
			return
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			Commandindex := msg.CommandIndex

			if Commandindex <= kv.lastAppliedIndex {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d discards outdated message Index %v because LastAppliedIndex is %v, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, kv.lastAppliedIndex, op.ClientId, op.RequestId)
				}
				continue
			}

			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Update LastAppliedIndex at Commandindex %d, type=%s, client=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.Type, op.ClientId, op.RequestId)
			}
			kv.lastAppliedIndex = Commandindex

			isDuplicate := false
			if lastReqId, ok := kv.lastApplied[op.ClientId]; ok {
				if op.RequestId <= lastReqId {
					isDuplicate = true
					opRes.Err = OK // Duplicate request, return OK
					opRes.RequestId = op.RequestId
					opRes.ClientId = op.ClientId
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Duplicated request, requestId %d <= last applied %d for client %d"+colorReset, kv.serverId, kv.gid, op.RequestId, lastReqId, op.ClientId)
					}
				}
			}
			isWriteOrConfigOp := op.Type == OpPrewrite || op.Type == OpCommit || op.Type == OpRollback || op.Type == OpConfigChange || op.Type == OpApplyMigratedData
			if !isWriteOrConfigOp {
				// Read Op
				opRes = kv.applyPercolatorOp(op)
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore after Get Key %s Value %s, client=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.Key, op.Value, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
			} else if !isDuplicate {
				opRes = kv.applyPercolatorOp(op)
				if opRes.Err == OK {
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore after %s Key %s Value %s, client=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, op.IntentType, op.Key, op.Value, op.ClientId, op.RequestId)
						kv.printKVStore()
					}
					kv.lastApplied[op.ClientId] = op.RequestId
				}
			}

			needSnapshot := kv.needSnapshot(Commandindex)
			if needSnapshot {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Creating snapshot, kvstore size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, len(kv.kvStore), Commandindex, op.ClientId, op.RequestId)
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore before snapshot, size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, len(kv.kvStore), Commandindex, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
				kv.takeSnapshot(Commandindex)
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore after snapshot, size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.serverId, kv.gid, len(kv.kvStore), Commandindex, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
			}

			// 通知等待的 RPC 处理程序
			kv.mu.Lock()
			notifyCh, ok := kv.notifyChs[Commandindex]
			kv.mu.Unlock()
			if ok {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Found waiting RPC Channel for Commandindex %d, client=%d requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.ClientId, op.RequestId)
				}
				select {
				case notifyCh <- opRes:
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Notified RPC for Commandindex %d, client=%d requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.ClientId, op.RequestId)
					}
				default:
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Notification dropped for Commandindex %d, client=%d requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.ClientId, op.RequestId)
					}
				}
			} else {
				if op.Type == OpConfigChange {
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: Finish OpConfigChange at Commandindex %d, client=%d requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.ClientId, op.RequestId)
					}
				} else {
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Srv %d GID %d: No waiting RPC Channel for Commandindex %d, client=%d requestId=%d"+colorReset, kv.serverId, kv.gid, Commandindex, op.ClientId, op.RequestId)
					}
				}
			}
		} else if msg.SnapshotValid {
			kv.mu.Lock()
			if msg.SnapshotIndex <= kv.lastAppliedIndex {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d: Discarding outdated snapshot at index %d term %d <= kv.lastAppliedIndex %d"+colorReset, kv.serverId, kv.gid, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				}
				kv.mu.Unlock()
				continue
			}
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Applying snapshot at index %d term %d, previous lastAppliedIndex %d"+colorReset, kv.serverId, kv.gid, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				kv.printKVStore()
			}
			kv.restoreSnapshot(msg.Snapshot)
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Applied snapshot at index %d term %d, updated lastAppliedIndex to %d"+colorReset, kv.serverId, kv.gid, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				kv.printKVStore()
			}
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Applied snapshot at index %d term %d"+colorReset, kv.serverId, kv.gid, msg.SnapshotIndex, msg.SnapshotTerm)
			}
			kv.mu.Unlock()
		} else {
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Unexpected message type %v"+colorReset, kv.serverId, kv.gid, msg)
			}
		}
	}
}

func (kv *ShardKV) pollConfig() {
	for !kv.killed() {
		// 1. 只有 Leader 才执行轮询和配置变更提议
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(100 * time.Millisecond) // 非 Leader 短暂休眠
			continue
		}
		// 2. 从 shardctrler 查询最新配置
		// Query(-1) 获取最新配置。mck 是 kv.mck，即 ShardCtrler 的客户端
		// latestConfig := kv.mck.Query(-1)
		latestConfig, err := kv.queryShardCtrlerWithTimeout(-1, 500*time.Millisecond)
		if err != nil {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Error querying latest config, error %v."+colorReset, kv.serverId, kv.gid, err)
			time.Sleep(100 * time.Millisecond) // 查询失败，等待下次轮询
			continue
		}

		kv.mu.Lock()
		currentConfigNum := kv.config.Num
		serverId := kv.serverId
		kv.mu.Unlock() // 尽早释放锁，后续 Raft Start 不应在锁内

		// 3. 如果检测到新配置
		if latestConfig.Num > currentConfigNum {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Detected new config %d (current %d). Proposing OpConfigChange."+colorReset, kv.serverId, kv.gid, latestConfig.Num, currentConfigNum)
			if latestConfig.Num-currentConfigNum > 1 {
				// latestConfig = kv.mck.Query(currentConfigNum + 1)
				latestConfig, err = kv.queryShardCtrlerWithTimeout(currentConfigNum+1, 500*time.Millisecond)
				if err != nil {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Error querying latest config, error %v."+colorReset, kv.serverId, kv.gid, err)
					time.Sleep(100 * time.Millisecond) // 查询失败，等待下次轮询
					continue
				}
			}
			// 准备 OpConfigChange 操作
			var globalPreviousConfig shardctrler.Config
			if latestConfig.Num == 0 {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) latestGlobalConfig is Config 0, Config -1 as its global predecessor."+colorReset, kv.serverId, kv.gid)
				globalPreviousConfig = shardctrler.Config{
					Num:    0,
					Shards: [shardctrler.NShards]int{},
					Groups: map[int][]string{},
				} // 所有分片都源自 GID 0
			} else if latestConfig.Num == 1 {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) latestGlobalConfig is Config 1, Config 0 as its global predecessor."+colorReset, kv.serverId, kv.gid)
				// globalPreviousConfig = kv.mck.Query(0)
				globalPreviousConfig, err = kv.queryShardCtrlerWithTimeout(0, 500*time.Millisecond)
				if err != nil {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Error querying latest config, error %v."+colorReset, kv.serverId, kv.gid, err)
					time.Sleep(100 * time.Millisecond) // 查询失败，等待下次轮询
					continue
				}
			} else { // 对于 Config N (N > 1)，其全局前一个配置是 Config N-1。
				previousConfigNumToQuery := latestConfig.Num - 1
				log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) latestGlobalConfig is Config %d, Config %d as its global predecessor."+colorReset, kv.serverId, kv.gid, latestConfig.Num, previousConfigNumToQuery)
				// globalPreviousConfig = kv.mck.Query(previousConfigNumToQuery)
				globalPreviousConfig, err = kv.queryShardCtrlerWithTimeout(previousConfigNumToQuery, 500*time.Millisecond)
				if err != nil {
					log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Error querying latest config, error %v."+colorReset, kv.serverId, kv.gid, err)
					time.Sleep(100 * time.Millisecond) // 查询失败，等待下次轮询
					continue
				}
			}
			requstid := (kv.serverId&0xFFFF)*1000000 + int64(latestConfig.Num)
			op := Op{
				Type:      OpConfigChange,
				NewConfig: latestConfig,         // 将获取到的最新配置包含在 Op 中
				OldConfig: globalPreviousConfig, // 旧配置
				ClientId:  serverId,             // 对于系统级操作，可以使用 GID 作为 ClientId
				RequestId: requstid,             // 使用配置号作为 RequestId，便于幂等处理
			}
			log.Printf(kv.GetServerColor()+"Srv %d GID %d:(Leader) Detected new config %d (current %d). Commit OpConfigChange to Raft."+colorReset, kv.serverId, kv.gid, latestConfig.Num, currentConfigNum)
			kv.rf.Start(op)
			// Leader 在这里不需要等待这个 Op 应用完成，applyLoop 会处理。
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (kv *ShardKV) queryShardCtrlerWithTimeout(configNumToQuery int, timeoutDuration time.Duration) (shardctrler.Config, error) {
	// 创建一个channel用于从goroutine接收查询结果
	queryResultChan := make(chan shardctrler.Config, 1)

	// 启动一个新的goroutine来执行可能会阻塞的 Query 调用
	go func() {
		configResult := kv.mck.Query(configNumToQuery)
		if !kv.killed() { // 检查 ShardKV 服务是否已被终止
			queryResultChan <- configResult
		}
	}()

	// 使用 select 来等待查询结果或超时
	select {
	case resultConfig := <-queryResultChan:
		// 查询操作在超时前完成了
		if resultConfig.Num < 0 { // 假设 Config.Num < 0 表示 clerk 查询失败或返回了无效配置
			return shardctrler.Config{}, fmt.Errorf("shardctrler query returned invalid config (Num: %d) for query %d", resultConfig.Num, configNumToQuery)
		}
		return resultConfig, nil // 查询成功

	case <-time.After(timeoutDuration):
		// 查询操作超时
		return shardctrler.Config{}, fmt.Errorf("shardctrler query for config %d timed out after %v", configNumToQuery, timeoutDuration)
	}
}

func (kv *ShardKV) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshotData := KVSnapshot{
		KvStore:          make(map[string][]ValueVersion),
		Locks:            make(map[string]LockInfo),
		WriteIntents:     make(map[string]WriteIntentInfo),
		Config:           kv.config,
		Shards:           make(map[int]ShardStatus),
		LastApplied:      make(map[int64]int64),
		LastAppliedIndex: index,
	}
	// Deep copy maps and slices
	// 复制KVstore
	for k, vList := range kv.kvStore {
		clonedList := make([]ValueVersion, len(vList))
		copy(clonedList, vList)
		snapshotData.KvStore[k] = clonedList
	}
	// 复制锁信息
	for k, v := range kv.locks {
		snapshotData.Locks[k] = v
	}
	// 复制写意图
	for k, v := range kv.writeIntent {
		snapshotData.WriteIntents[k] = v
	}
	//// 1. 复制 KvStore (只复制当前负责且状态为 Serving 的分片数据)
	//for k, vList := range kv.kvStore {
	//	shardId := key2shard(k) // 假设有 key2shard 函数
	//	if kv.config.Shards[shardId] == kv.gid {
	//		clonedList := make([]ValueVersion, len(vList))
	//		copy(clonedList, vList)
	//		snapshotData.KvStore[k] = clonedList
	//	}
	//}
	//
	//// 2. 复制 Locks (同样只复制当前负责且 Serving 的分片的锁)
	//for k, lInfo := range kv.locks {
	//	shardId := key2shard(k)
	//	if kv.config.Shards[shardId] == kv.gid {
	//		snapshotData.Locks[k] = lInfo
	//	}
	//}
	//
	//// 3. 复制 WriteIntents (同样只复制当前负责且 Serving 的分片的写意图)
	//for k, wiInfo := range kv.writeIntent {
	//	shardId := key2shard(k)
	//	if kv.config.Shards[shardId] == kv.gid {
	//		snapshotData.WriteIntents[k] = wiInfo
	//	}
	//}
	// 复制分片信息
	for k, v := range kv.Shards {
		snapshotData.Shards[k] = v
	}
	// 复制客户端信息
	for k, v := range kv.lastApplied {
		snapshotData.LastApplied[k] = v
	}

	if err := e.Encode(snapshotData); err != nil {
		log.Fatalf(kv.GetServerColor()+"Srv %d GID %d: Failed to encode snapshot: %v"+colorReset, kv.serverId, kv.gid, err)
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Creating snapshot, kvstore size=%d"+colorReset, kv.serverId, kv.gid, len(kv.kvStore))
	}
	kv.rf.Snapshot(kv.lastAppliedIndex, w.Bytes())
	kv.lastSnapshotIndex = kv.lastAppliedIndex
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Took snapshot at index %d, RaftSize: %d, SnapshotSize: %d"+colorReset, kv.serverId, kv.gid, kv.lastAppliedIndex, kv.persister.RaftStateSize(), kv.persister.SnapshotSize())
	}
}

func (kv *ShardKV) needSnapshot(index int) bool {
	if kv.maxraftstate == -1 {
		return false
	}
	// 检查分片状态
	for i := 0; i < shardctrler.NShards; i++ {
		if kv.Shards[i] == ShardStatus_WaitingForData {
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Skipping snapshot at index %d, shard %d is waiting for data"+colorReset, kv.serverId, kv.gid, index, i)
			}
			return false
		}
	}
	statesize := kv.persister.RaftStateSize()
	if statesize <= kv.maxraftstate*3/2 {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Skipping snapshot at index %d, size=%d < maxraftstate=%d*3/2"+colorReset, kv.serverId, kv.gid, index, statesize, kv.maxraftstate)
		}
		return false
	}
	if index-kv.lastSnapshotIndex < kv.snapshotCooldown {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Skipping snapshot at index %d, within cooldown (lastSnapshotIndex=%d)"+colorReset, kv.serverId, kv.gid, index, kv.lastSnapshotIndex)
		}
		return false
	}
	if index > kv.lastAppliedIndex {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Skipping snapshot at index %d, exceeds LastAppliedIndex %d"+colorReset, kv.serverId, kv.gid, index, kv.lastAppliedIndex)
		}
		return false
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Need snapshot at index %d, size=%d > maxraftstate=%d"+colorReset, kv.serverId, kv.gid, index, kv.persister.RaftStateSize(), kv.maxraftstate)
	}
	return true
}

func (kv *ShardKV) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Empty snapshot, nothing to restore"+colorReset, kv.serverId, kv.gid)
		}
		return
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Try to restore snapshot, size=%d"+colorReset, kv.serverId, kv.gid, len(snapshot))
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var snapshotData KVSnapshot
	if err := d.Decode(&snapshotData); err != nil {
		log.Fatalf("Srv %d GID %d: Failed to decode snapshot: %v", kv.serverId, kv.gid, err)
	}
	kv.kvStore = snapshotData.KvStore
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, kvstore =%s"+colorReset, kv.serverId, kv.gid, kv.kvStore)
	kv.locks = snapshotData.Locks
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, locks =%s"+colorReset, kv.serverId, kv.gid, kv.locks)
	kv.writeIntent = snapshotData.WriteIntents
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, writeIntents =%s"+colorReset, kv.serverId, kv.gid, kv.writeIntent)
	kv.config = snapshotData.Config
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, config =%d"+colorReset, kv.serverId, kv.gid, kv.config.Num)
	kv.Shards = snapshotData.Shards
	log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, shards =%s"+colorReset, kv.serverId, kv.gid, kv.Shards)
	kv.lastApplied = snapshotData.LastApplied
	kv.lastAppliedIndex = snapshotData.LastAppliedIndex
	kv.lastSnapshotIndex = snapshotData.LastAppliedIndex
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restored snapshot, kvstore size=%d"+colorReset, kv.serverId, kv.gid, len(kv.kvStore))
	}
}

// StartServer
// servers[] contains the ports of the servers in this group.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
// gid is this group's GID, for interacting with the shardctrler.
// pass ctrlers[] to shardctrler.MakeClerk() so you can send
// RPCs to the shardctrler.
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
// look at client.go for examples of how to use ctrlers[]
// and make_end() to send RPCs to the group owning a specific shard.
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, network *labrpc.Network, me int, persister *raft.Persister, maxraftstate int, gid int, ctrlers []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(ValueVersion{})
	labgob.Register(LockInfo{})
	labgob.Register(WriteIntentInfo{})
	labgob.Register(KVSnapshot{})
	labgob.Register(shardctrler.Config{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.serverId = nrand()
	kv.make_end = make_end
	kv.gid = gid
	kv.ctrlers = ctrlers

	// Your initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.kvStore = make(map[string][]ValueVersion)
	kv.locks = make(map[string]LockInfo)
	kv.writeIntent = make(map[string]WriteIntentInfo)
	kv.lastApplied = make(map[int64]int64)
	kv.notifyChs = make(map[int]chan OpResponse)
	kv.leaderHints = make(map[int]int)
	kv.lastAppliedIndex = 0
	kv.lastSnapshotIndex = 0
	kv.snapshotCooldown = 10

	kv.mck = shardctrler.MakeClerk(ctrlers)
	kv.config = shardctrler.Config{Num: 0}
	kv.Shards = make(map[int]ShardStatus)
	kv.tso_end = MakeTSOClientEnd(network, GlobalTSOServerName)

	// Use something like this to talk to the shardctrler:
	// kv.mck = shardctrler.MakeClerk(kv.ctrlers)
	// 初始化配置，获取分片归属
	// config := kv.mck.Query(-1)
	config := kv.mck.Query(0) // 获取初始配置，通常是 Config 0
	kv.config = config
	for shard := 0; shard < shardctrler.NShards; shard++ {
		if config.Shards[shard] == kv.gid {
			kv.Shards[shard] = ShardStatus_Serving
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Responsible for shard %d"+colorReset, kv.serverId, kv.gid, shard)
			}
		} else {
			kv.Shards[shard] = ShardStatus_NotOwned
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Srv %d GID %d: Not responsible for shard %d"+colorReset, kv.serverId, kv.gid, shard)
			}
		}
		//kv.Shards[shard] = ShardStatus_Unknown
	}

	// 恢复快照
	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Restoring snapshot, size=%d"+colorReset, kv.serverId, kv.gid, len(snapshot))
		}
		kv.restoreSnapshot(snapshot)
	} else {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: No snapshot found, starting fresh"+colorReset, kv.serverId, kv.gid)
		}
	}

	go kv.applyLoop()
	go kv.pollConfig()
	return kv
}

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// 功能函数

func (kv *ShardKV) gettimestamp() uint64 {
	args := GetTimestampArgs{ClientId: kv.serverId}
	var reply GetTimestampReply
	for {
		if kv.tso_end.Call("TSOServer.GetTimestamp", &args, &reply) {
			return reply.Timestamp
		}
		if EnableClientLog {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: Failed to get timestamp from TSO, retrying..."+colorReset, kv.serverId, kv.gid)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Kill
// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Kill"+colorReset, kv.serverId)
	}
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *ShardKV) collectShardData(shardId int) ShardFullData {
	data := ShardFullData{
		KvStore:      make(map[string][]ValueVersion),
		Locks:        make(map[string]LockInfo),
		WriteIntents: make(map[string]WriteIntentInfo),
	}
	for k, vList := range kv.kvStore {
		if key2shard(k) == shardId {
			clonedList := make([]ValueVersion, len(vList))
			copy(clonedList, vList)
			data.KvStore[k] = clonedList
		}
	}
	for k, lInfo := range kv.locks {
		if key2shard(k) == shardId {
			data.Locks[k] = lInfo
		}
	}
	for k, wiInfo := range kv.writeIntent {
		if key2shard(k) == shardId {
			data.WriteIntents[k] = wiInfo
		}
	}
	// log.Printf(kv.GetServerColor()+"Srv %d GID %d: Collected data for shard %d: %d KV versions, %d locks, %d intents."+colorReset, kv.serverId, kv.gid, shardId, len(data.KvStore), len(data.Locks), len(data.WriteIntents))
	return data
}

func (kv *ShardKV) printKVStore() {
	if EnableKVStoreLogging {
		// 1. 打印 kv.kvStore (按键排序)
		if len(kv.kvStore) == 0 {
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore is empty"+colorReset, kv.me, kv.gid)
		} else {
			// 提取所有键
			keys := make([]string, 0, len(kv.kvStore))
			for k := range kv.kvStore {
				keys = append(keys, k)
			}
			// 按字典序对键进行排序
			sort.Strings(keys)

			// 构建有序的输出字符串
			var sb strings.Builder
			sb.WriteString("map[")
			for i, k := range keys {
				if i > 0 {
					sb.WriteString(" ") // map元素之间的标准分隔符
				}
				// 使用 fmt.Fprintf 将键和对应的值（版本列表）格式化后写入 Builder
				// ValueVersion 切片的默认 %v 输出可能很长，您可以考虑为 ValueVersion 定义 String() 方法来定制输出
				fmt.Fprintf(&sb, "%s:%v", k, kv.kvStore[k])
			}
			sb.WriteString("]")
			log.Printf(kv.GetServerColor()+"Srv %d GID %d: kvstore=%s"+colorReset, kv.me, kv.gid, sb.String())
		}
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: locks=%v"+colorReset, kv.serverId, kv.gid, kv.locks)
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: writeIntent=%v"+colorReset, kv.serverId, kv.gid, kv.writeIntent)
		log.Printf(kv.GetServerColor()+"Srv %d GID %d: Shards=%v"+colorReset, kv.serverId, kv.gid, kv.Shards)
	}
}

// GetServerColor
// 根据服务器的 GID 和索引返回对应的颜色字符串
func (kv *ShardKV) GetServerColor() string {
	gidFamilyIndex := kv.gid % 5
	serverIndexInGroup := kv.me % 3
	isLeader := false
	switch gidFamilyIndex {
	case 0: // 绿色系 (对应 GID 100, 105, ...)
		switch serverIndexInGroup {
		case 0:
			if isLeader {
				return colorG0M0Leader
			}
			return colorG0M0Follower
		case 1:
			if isLeader {
				return colorG0M1Leader
			}
			return colorG0M1Follower
		case 2:
			if isLeader {
				return colorG0M2Leader
			}
			return colorG0M2Follower
		default: // 不应该发生，但作为后备
			if isLeader {
				return colorG0M1Leader
			}
			return colorG0M1Follower
		}
	case 1: // 红色系 (对应 GID 101, 106, ...)
		switch serverIndexInGroup {
		case 0:
			if isLeader {
				return colorG1M0Leader
			}
			return colorG1M0Follower
		case 1:
			if isLeader {
				return colorG1M1Leader
			}
			return colorG1M1Follower
		case 2:
			if isLeader {
				return colorG1M2Leader
			}
			return colorG1M2Follower
		default:
			if isLeader {
				return colorG1M1Leader
			}
			return colorG1M1Follower
		}
	case 2: // 蓝色系 (对应 GID 102, 107, ...)
		switch serverIndexInGroup {
		case 0:
			if isLeader {
				return colorG2M0Leader
			}
			return colorG2M0Follower
		case 1:
			if isLeader {
				return colorG2M1Leader
			}
			return colorG2M1Follower
		case 2:
			if isLeader {
				return colorG2M2Leader
			}
			return colorG2M2Follower
		default:
			if isLeader {
				return colorG2M1Leader
			}
			return colorG2M1Follower
		}
	case 3: // 黄色/橙色系 (对应 GID 103, 108, ...)
		switch serverIndexInGroup {
		case 0:
			if isLeader {
				return colorG3M0Leader
			}
			return colorG3M0Follower
		case 1:
			if isLeader {
				return colorG3M1Leader
			}
			return colorG3M1Follower
		case 2:
			if isLeader {
				return colorG3M2Leader
			}
			return colorG3M2Follower
		default:
			if isLeader {
				return colorG3M1Leader
			}
			return colorG3M1Follower
		}
	case 4: // 紫色系 (对应 GID 104, 109, ...)
		switch serverIndexInGroup {
		case 0:
			if isLeader {
				return colorG4M0Leader
			}
			return colorG4M0Follower
		case 1:
			if isLeader {
				return colorG4M1Leader
			}
			return colorG4M1Follower
		case 2:
			if isLeader {
				return colorG4M2Leader
			}
			return colorG4M2Follower
		default:
			if isLeader {
				return colorG4M1Leader
			}
			return colorG4M1Follower
		}
	}
	return "\033[38;2;169;169;169m"
}

///////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// original Get Put Append
//func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
//	// Your code here.
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Trying to start Get, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, args.ClientId, args.RequestId)
//	}
//
//	// 检查分片归属
//	kv.mu.Lock()
//	shard := key2shard(args.Key)
//	if !kv.Shards[shard] {
//		reply.Err = ErrWrongGroup
//		kv.mu.Unlock()
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: Get request failed, wrong group for shard %d, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, shard, args.Key, args.ClientId, args.RequestId)
//		}
//		return
//	}
//	log.Printf(kv.GetServerColor()+"Server %d: Get key=%s, shard %d correct, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, shard, args.ClientId, args.RequestId)
//	kv.mu.Unlock()
//
//	// 构造操作并提交到 Raft
//	op := Op{
//		Type:      "Get",
//		Key:       args.Key,
//		Value:     "",
//		ClientId:  args.ClientId,
//		RequestId: args.RequestId,
//	}
//
//	// 检查是否为领导者
//	index, _, isLeader := kv.rf.Start(op)
//	if !isLeader {
//		reply.Err = ErrWrongLeader
//		reply.WrongLeader = true
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: Get key=%s request failed, not the leader after Start, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//		}
//		return
//	}
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s request, is the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//	}
//
//	// 创建通知通道
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s, waiting for kv.mu.Lock, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//	}
//	kv.mu.Lock()
//	notifyCh := make(chan Op, 1)
//	kv.notifyChs[index] = notifyCh
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s, made channel for index %d, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, index, op.ClientId, op.RequestId)
//	}
//	kv.mu.Unlock()
//
//	defer func() {
//		kv.mu.Lock()
//		if ch, ok := kv.notifyChs[index]; ok {
//			close(ch)
//			delete(kv.notifyChs, index)
//		}
//		kv.mu.Unlock()
//	}()
//
//	// 等待 Raft 应用
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, kv.me, index, op.ClientId, op.RequestId)
//	}
//	select {
//	case response := <-notifyCh:
//		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
//			reply.Value = response.Value
//			reply.Err = OK
//			if EnableServerLogging {
//				log.Printf(kv.GetServerColor()+"Server %d: Get key=%s succeeded, value=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, reply.Value, op.ClientId, op.RequestId)
//			}
//		} else {
//			reply.Err = ErrClientOrRequest
//			reply.WrongLeader = true
//			if EnableServerLogging {
//				log.Printf(kv.GetServerColor()+"Server %d: Get key=%s failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//			}
//		}
//	case <-time.After(500 * time.Millisecond):
//		reply.WrongLeader = true
//		reply.Err = ErrWrongLeader
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: Get key=%s failed, timeout, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//		}
//	}
//}
//
//func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
//	// Your code here.
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Trying to start PutAppend, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, args.ClientId, args.RequestId)
//	}
//
//	// 检查分片归属
//	kv.mu.Lock()
//	shard := key2shard(args.Key)
//	if !kv.Shards[shard] {
//		reply.Err = ErrWrongGroup
//		kv.mu.Unlock()
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: PutAppend request failed, wrong group for shard %d, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, shard, args.Key, args.ClientId, args.RequestId)
//		}
//		return
//	}
//	log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s, shard %d correct, clientId=%d, requestId=%d"+colorReset, kv.me, shard, args.Key, args.ClientId, args.RequestId)
//	kv.mu.Unlock()
//
//	// 构造操作并提交到 Raft
//	op := Op{
//		Type:      args.Op,
//		Key:       args.Key,
//		Value:     args.Value,
//		ClientId:  args.ClientId,
//		RequestId: args.RequestId,
//	}
//
//	// 检查是否为领导者
//	index, _, isLeader := kv.rf.Start(op)
//	if !isLeader {
//		reply.Err = ErrWrongLeader
//		reply.WrongLeader = true
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s request failed, not the leader after Start, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//		}
//		return
//	}
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s request, is the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//	}
//
//	// 创建通知通道
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s, waiting for kv.mu.Lock, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//	}
//	kv.mu.Lock()
//	notifyCh := make(chan Op, 1)
//	kv.notifyChs[index] = notifyCh
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s, made channel for index %d, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, index, op.ClientId, op.RequestId)
//	}
//	kv.mu.Unlock()
//
//	defer func() {
//		kv.mu.Lock()
//		if ch, ok := kv.notifyChs[index]; ok {
//			close(ch)
//			delete(kv.notifyChs, index)
//		}
//		kv.mu.Unlock()
//	}()
//
//	// 等待 Raft 应用
//	if EnableServerLogging {
//		log.Printf(kv.GetServerColor()+"Server %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, kv.me, index, op.ClientId, op.RequestId)
//	}
//	select {
//	case response := <-notifyCh:
//		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
//			reply.Err = OK
//			if EnableServerLogging {
//				log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s succeeded, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//			}
//		} else {
//			reply.Err = ErrClientOrRequest
//			reply.WrongLeader = true
//			if EnableServerLogging {
//				log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//			}
//		}
//	case <-time.After(500 * time.Millisecond):
//		reply.WrongLeader = true
//		reply.Err = ErrWrongLeader
//		if EnableServerLogging {
//			log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s failed, timeout, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
//		}
//	}
//}
