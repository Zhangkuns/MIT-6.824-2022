package shardctrler

import (
	"bytes"
	"log"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"mit6.5840/labgob"
	"mit6.5840/labrpc"
	"mit6.5840/raft"
)

const (
	// Server 0 的颜色 (青色系)
	colorCtrlServer0 = "\033[38;2;0;128;128m"   // Teal (深青)
	colorCtrlServer5 = "\033[38;2;173;216;230m" // Light Cyan (浅青)

	// Server 1 的颜色 (粉红色系)
	colorCtrlServer1 = "\033[38;2;255;192;203m" // Blush Pink (浅粉)
	colorCtrlServer6 = "\033[38;2;199;21;133m"  // Deep Pink (深粉)

	// Server 2 的颜色 (蓝色系)
	colorCtrlServer2 = "\033[38;2;176;224;230m" // Light Blue (浅蓝)
	colorCtrlServer7 = "\033[38;2;0;0;128m"     // Navy Blue (深蓝)

	// Server 3 的颜色 (橙色系)
	colorCtrlServer3 = "\033[38;2;255;218;185m" // Peach (浅橙)
	colorCtrlServer8 = "\033[38;2;204;85;0m"    // Burnt Orange (深橙)

	// Server 4 的颜色 (紫红色系)
	colorCtrlServer4 = "\033[38;2;216;191;216m" // Lavender (浅紫红)
	colorCtrlServer9 = "\033[38;2;139;0;139m"   // Magenta (深紫红)

	// 其他测试信息的颜色
	colorTest1  = "\033[38;2;255;165;0m"   // Orange (橙色，用于 connect)
	colorTest2  = "\033[38;2;165;42;42m"   // Brown (棕色，用于 disconnect)
	colorTest3  = "\033[38;2;211;211;211m" // Light Gray (浅灰，用于 cmd agreement)
	colorTest4  = "\033[38;2;128;128;128m" // Gray (灰色，用于 iteration)
	colorTest5  = "\033[38;2;255;20;147m"  // Hot Pink (亮粉，用于 crash)
	colorTest6  = "\033[38;2;0;255;255m"   // Cyan (青色，用于 restart)
	colorNewLog = "\033[38;2;0;255;255m"   // Bright Cyan (亮青，用于新日志)
	colorReset  = "\033[0m"                // 重置颜色
)

var EnableCtrlServerLogging = false

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	// Your data here.

	configs           []Config        // indexed by config num
	lastApplied       map[int64]int64 // 客户端最后应用的请求 ID
	lastAppliedIndex  int             // 最后应用的日志索引
	notifyChs         map[int]chan Op // 通知通道
	persister         *raft.Persister
	lastSnapshotIndex int // 上一次快照的索引
	maxraftstate      int // 快照触发阈值
}

type Op struct {
	// Your data here.
	Type      string           // "Join", "Leave", "Move", "Query"
	Servers   map[int][]string // Join
	GIDs      []int            // Leave
	Shard     int              // Move
	GID       int              // Move
	ConfigNum int              // Query
	Config    Config           // Query 结果
	ClientId  int64
	RequestId int64
}

func (sc *ShardCtrler) GetServerColor() string {
	switch sc.me % 10 {
	case 0:
		return colorCtrlServer0
	case 1:
		return colorCtrlServer1
	case 2:
		return colorCtrlServer2
	case 3:
		return colorCtrlServer3
	case 4:
		return colorCtrlServer4
	case 5:
		return colorCtrlServer5
	case 6:
		return colorCtrlServer6
	case 7:
		return colorCtrlServer7
	case 8:
		return colorCtrlServer8
	case 9:
		return colorCtrlServer9
	default:
		return colorTest1
	}
}

// The Join RPC is used by an administrator to add new replica groups.
// Its argument is a set of mappings from unique, non-zero replica group identifiers (GIDs) to lists of server names.
// The shardctrler should react by creating a new configuration that includes the new replica groups.
// The new configuration should divide the shards as evenly as possible among the full set of groups,
// and should move as few shards as possible to achieve that goal.
// The shardctrler should allow re-use of a GID if it's not part of the current configuration
// (i.e. a GID should be allowed to Join, then Leave, then Join again).
func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	op := Op{
		Type:      "Join",
		Servers:   args.Servers,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Trying to start Join, servers=%v, clientId=%d, requestId=%d"+colorReset, sc.me, args.Servers, op.ClientId, op.RequestId)
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Join request failed, not the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
		return
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Join request, is the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
	}

	sc.mu.Lock()
	notifyCh := make(chan Op, 1)
	sc.notifyChs[index] = notifyCh
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Join, make the channel index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if ch, ok := sc.notifyChs[index]; ok {
			close(ch)
			delete(sc.notifyChs, index)
		}
		sc.mu.Unlock()
	}()

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	select {
	case response := <-notifyCh:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			reply.Err = OK
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Join succeeded, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Join failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Join failed, timeout, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
	}
}

// The Leave RPC's argument is a list of GIDs of previously joined groups.
// The shardctrler should create a new configuration that does not include those groups,
// and that assigns those groups' shards to the remaining groups.
// The new configuration should divide the shards as evenly as possible among the groups,
// and should move as few shards as possible to achieve that goal.
func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	op := Op{
		Type:      "Leave",
		GIDs:      args.GIDs,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Trying to start Leave, gids=%v, clientId=%d, requestId=%d"+colorReset, sc.me, args.GIDs, op.ClientId, op.RequestId)
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave request failed, not the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
		return
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave request, is the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
	}

	sc.mu.Lock()
	notifyCh := make(chan Op, 1)
	sc.notifyChs[index] = notifyCh
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave, make the channel index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if ch, ok := sc.notifyChs[index]; ok {
			close(ch)
			delete(sc.notifyChs, index)
		}
		sc.mu.Unlock()
	}()

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	select {
	case response := <-notifyCh:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			reply.Err = OK
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave succeeded, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Leave failed, timeout, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
	}
}

// The Move RPC's arguments are a shard number and a GID.
// The shardctrler should create a new configuration in which the shard is assigned to the group.
// The purpose of Move is to allow us to test your software.
// A Join or Leave following a Move will likely un-do the Move, since Join and Leave re-balance.
func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	op := Op{
		Type:      "Move",
		Shard:     args.Shard,
		GID:       args.GID,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Trying to start Move, shard=%d, gid=%d, clientId=%d, requestId=%d"+colorReset, sc.me, args.Shard, args.GID, op.ClientId, op.RequestId)
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Move request failed, not the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
		return
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Move request, is the leader, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
	}

	sc.mu.Lock()
	notifyCh := make(chan Op, 1)
	sc.notifyChs[index] = notifyCh
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Move, make the channel index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if ch, ok := sc.notifyChs[index]; ok {
			close(ch)
			delete(sc.notifyChs, index)
		}
		sc.mu.Unlock()
	}()

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	select {
	case response := <-notifyCh:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			reply.Err = OK
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Move succeeded, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Move failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Move failed, timeout, clientId=%d, requestId=%d"+colorReset, sc.me, op.ClientId, op.RequestId)
		}
	}
}

// The Query RPC's argument is a configuration number.
// The shardctrler replies with the configuration that has that number.
// If the number is -1 or bigger than the biggest known configuration number,
// the shardctrler should reply with the latest configuration.
// The result of Query(-1) should reflect every Join, Leave, or Move RPC
// that the shardctrler finished handling before it received the Query(-1) RPC.
func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		Type:      "Query",
		ConfigNum: args.Num,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Trying to start Query, configNum=%d, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
	}

	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d request failed, not the leader, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
		}
		return
	}

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d request, is the leader, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
	}

	sc.mu.Lock()
	notifyCh := make(chan Op, 1)
	sc.notifyChs[index] = notifyCh
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d, make the channel index %d, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, index, op.ClientId, op.RequestId)
	}
	sc.mu.Unlock()

	defer func() {
		sc.mu.Lock()
		if ch, ok := sc.notifyChs[index]; ok {
			close(ch)
			delete(sc.notifyChs, index)
		}
		sc.mu.Unlock()
	}()

	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, sc.me, index, op.ClientId, op.RequestId)
	}
	select {
	case response := <-notifyCh:
		if response.ClientId == op.ClientId && response.RequestId == op.RequestId {
			reply.Config = response.Config
			reply.Err = OK
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d succeeded, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d failed, timeout, clientId=%d, requestId=%d"+colorReset, sc.me, args.Num, op.ClientId, op.RequestId)
		}
	}
}

// Kill
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sc *ShardCtrler) Kill() {
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Kill\n", sc.me)
	}
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	z := atomic.LoadInt32(&sc.dead)
	return z == 1
}

// Raft
// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

func (sc *ShardCtrler) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := struct {
		Configs          []Config
		LastApplied      map[int64]int64
		LastAppliedIndex int
	}{
		Configs:          sc.configs,
		LastApplied:      sc.lastApplied,
		LastAppliedIndex: sc.lastAppliedIndex,
	}
	if err := e.Encode(snapshot); err != nil {
		log.Fatalf(sc.GetServerColor()+"CtrlServer %d: Failed to encode snapshot: %v"+colorReset, sc.me, err)
	}
	return w.Bytes()
}

func (sc *ShardCtrler) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	snapshot := struct {
		Configs          []Config
		LastApplied      map[int64]int64
		LastAppliedIndex int
	}{
		Configs:          sc.configs,
		LastApplied:      sc.lastApplied,
		LastAppliedIndex: sc.lastAppliedIndex,
	}
	if err := e.Encode(snapshot); err != nil {
		log.Fatalf(sc.GetServerColor()+"CtrlServer %d: Failed to encode snapshot: %v"+colorReset, sc.me, err)
	}
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Creating snapshot, configs size=%d"+colorReset, sc.me, len(sc.configs))
	}
	sc.rf.Snapshot(sc.lastAppliedIndex, w.Bytes())
	sc.lastSnapshotIndex = sc.lastAppliedIndex
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Took snapshot at index %d, size=%d"+colorReset, sc.me, sc.lastAppliedIndex, sc.persister.RaftStateSize())
	}
}

func (sc *ShardCtrler) needSnapshot(index int) bool {
	if sc.maxraftstate == -1 {
		return false
	}
	statesize := sc.persister.RaftStateSize()
	if statesize <= sc.maxraftstate*3/2 {
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Skipping snapshot at index %d, size=%d < maxraftstate=%d*3/2"+colorReset, sc.me, index, statesize, sc.maxraftstate)
		}
		return false
	}
	if index > sc.lastAppliedIndex {
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Skipping snapshot at index %d, exceeds LastAppliedIndex %d"+colorReset, sc.me, index, sc.lastAppliedIndex)
		}
		return false
	}
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Need snapshot at index %d, size=%d > maxraftstate=%d"+colorReset, sc.me, index, statesize, sc.maxraftstate)
	}
	return true
}

func (sc *ShardCtrler) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		if EnableCtrlServerLogging {
			log.Printf(sc.GetServerColor()+"CtrlServer %d: Empty snapshot, nothing to restore"+colorReset, sc.me)
		}
		return
	}
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Try to restore snapshot, size=%d"+colorReset, sc.me, len(snapshot))
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var snap struct {
		Configs          []Config
		LastApplied      map[int64]int64
		LastAppliedIndex int
	}
	if err := d.Decode(&snap); err != nil {
		log.Fatalf("CtrlServer %d: Failed to decode snapshot: %v", sc.me, err)
	}
	sc.configs = snap.Configs
	sc.lastApplied = snap.LastApplied
	sc.lastAppliedIndex = snap.LastAppliedIndex
	if EnableCtrlServerLogging {
		log.Printf(sc.GetServerColor()+"CtrlServer %d: Restored snapshot, configs size=%d"+colorReset, sc.me, len(sc.configs))
	}
}

func contains(slice []int, item int) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

func (sc *ShardCtrler) applyLoop() {
	for !sc.killed() {
		msg, ok := <-sc.applyCh
		if !ok {
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: applyCh closed, exiting applyLoop"+colorReset, sc.me)
			}
			return
		}

		if msg.CommandValid {
			op := msg.Command.(Op)
			Commandindex := msg.CommandIndex

			var needSnapshot bool

			if Commandindex <= sc.lastAppliedIndex {
				if EnableCtrlServerLogging {
					log.Printf(sc.GetServerColor()+"CtrlServer %d discards outdated message Index %v because LastAppliedIndex is %v, clientId=%d, requestId=%d"+colorReset, sc.me, Commandindex, sc.lastAppliedIndex, op.ClientId, op.RequestId)
				}
				continue
			}

			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Update LastAppliedIndex at Commandindex %d, type=%s, client=%d, requestId=%d"+colorReset, sc.me, Commandindex, op.Type, op.ClientId, op.RequestId)
			}
			sc.lastAppliedIndex = Commandindex

			isDuplicate := false
			if lastReqId, ok := sc.lastApplied[op.ClientId]; ok {
				if op.RequestId <= lastReqId {
					isDuplicate = true
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Duplicated request, client %d, requestId %d"+colorReset, sc.me, op.ClientId, op.RequestId)
					}
				}
			}

			var config Config
			if !isDuplicate {
				switch op.Type {
				case "Join":
					// TODO: 实现 Join 逻辑（创建新配置，重新平衡分片）
					// 复制最新配置
					oldConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{
						Num:    oldConfig.Num + 1,
						Shards: [NShards]int{},
						Groups: make(map[int][]string),
					}
					// 深拷贝 Groups
					for gid, servers := range oldConfig.Groups {
						newConfig.Groups[gid] = append([]string{}, servers...)
					}
					// 添加新 GID 和服务器
					var newGids []int // 记录新加入的 GID
					for gid, servers := range op.Servers {
						newConfig.Groups[gid] = append([]string{}, servers...)
						newGids = append(newGids, gid)
					}
					sort.Ints(newGids)

					// 收集所有有效 GID（只包含 newConfig.Groups 中的组，不包括 GID 0）
					var gids []int
					for gid := range newConfig.Groups {
						gids = append(gids, gid)
					}
					sort.Ints(gids)
					// 打印旧配置的分片分配
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Before Join, old config num=%d, shards=%v, groups=%v"+colorReset,
							sc.me, oldConfig.Num, oldConfig.Shards, oldConfig.Groups)
					}
					// 如果没有 GID，所有分片分配给 GID 0
					if len(gids) == 0 {
						for i := 0; i < NShards; i++ {
							newConfig.Shards[i] = 0
						}
					} else {
						// 均匀分配分片
						// Join: 加入新组后，每次选择一个拥有shard最多的组和一个拥有shard最少的组，
						// shard逐个挪动，直到shard数量差值小于等于1, 而且GID 0组没有shard为止。
						// 计算每个组的分片数量
						shardCount := make(map[int]int)
						for _, gid := range gids {
							shardCount[gid] = 0
						}
						shardCount[0] = 0 // GID 0 可能有分片
						for i := 0; i < NShards; i++ {
							gid := newConfig.Shards[i]
							shardCount[gid]++
						}

						// 优先将 GID 0 的分片转移到有效组
						if shardCount[0] > 0 {
							for i := 0; i < NShards && shardCount[0] > 0; i++ {
								if newConfig.Shards[i] != 0 {
									continue
								}
								// 找到分片最少的有效组（优先选择 GID 小的）
								minShards := NShards + 1
								minGid := -1
								for _, gid := range gids {
									if shardCount[gid] < minShards {
										minShards = shardCount[gid]
										minGid = gid
									} else if shardCount[gid] == minShards && gid < minGid {
										minGid = gid
									}
								}
								if minGid != -1 {
									newConfig.Shards[i] = minGid
									shardCount[0]--
									shardCount[minGid]++
								}
							}
						}

						// 逐个转移分片，直到分片数量差值 ≤ 1
						for {
							// 计算当前分片数量
							maxShards := 0
							minShards := NShards + 1
							maxGid := -1
							minGid := -1
							for _, gid := range gids {
								count := shardCount[gid]
								if count > maxShards {
									maxShards = count
									maxGid = gid
								} else if count == maxShards && gid < maxGid {
									maxGid = gid
								}
								if count < minShards {
									minShards = count
									minGid = gid
								} else if count == minShards && gid < minGid {
									minGid = gid
								}
							}

							// 如果差值 ≤ 1，停止转移
							if maxShards-minShards <= 1 {
								break
							}

							// 从 maxGid 转移一个分片到 minGid
							for i := 0; i < NShards; i++ {
								if newConfig.Shards[i] == maxGid {
									newConfig.Shards[i] = minGid
									shardCount[maxGid]--
									shardCount[minGid]++
									break // 每次只转移一个分片
								}
							}
						}
					}
					// 追加新配置
					sc.configs = append(sc.configs, newConfig)
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Applied Join, new config num=%d, gids=%v, shards=%v"+colorReset, sc.me, newConfig.Num, gids, newConfig.Shards)
						// 打印分片变化（哪些分片从哪个 GID 变到哪个 GID）
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard changes after Join (old -> new):"+colorReset, sc.me)
						for i := 0; i < NShards; i++ {
							if oldConfig.Shards[i] != newConfig.Shards[i] {
								log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard %d: %d -> %d"+colorReset,
									sc.me, i, oldConfig.Shards[i], newConfig.Shards[i])
							}
						}
					}
				case "Leave":
					// TODO: 实现 Leave 逻辑
					oldConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{
						Num:    oldConfig.Num + 1,
						Shards: [NShards]int{},
						Groups: make(map[int][]string),
					}
					// 复制未移除的 Groups
					for gid, servers := range oldConfig.Groups {
						if !contains(op.GIDs, gid) {
							newConfig.Groups[gid] = append([]string{}, servers...)
						}
					}
					// 收集剩余 GID（只包含 newConfig.Groups 中的组）
					var gids []int
					for gid := range newConfig.Groups {
						gids = append(gids, gid)
					}
					sort.Ints(gids)
					// 打印旧配置的分片分配
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Before Leave, old config num=%d, shards=%v, groups=%v, removing gids=%v"+colorReset,
							sc.me, oldConfig.Num, oldConfig.Shards, oldConfig.Groups, op.GIDs)
					}

					// 分配分片
					if len(gids) == 0 {
						for i := 0; i < NShards; i++ {
							newConfig.Shards[i] = 0
						}
					} else {
						// 计算每个组的分片数量
						shardCount := make(map[int]int)
						for _, gid := range gids {
							shardCount[gid] = 0
						}
						shardCount[0] = 0 // GID 0 可能有分片
						for i := 0; i < NShards; i++ {
							gid := newConfig.Shards[i]
							if contains(op.GIDs, gid) {
								newConfig.Shards[i] = 0 // 被移除组的分片临时分配给 GID 0
								shardCount[0]++
							} else if _, exists := shardCount[gid]; exists {
								shardCount[gid]++
							}
						}

						// 将 GID 0 的分片（包括被移除组的分片）转移到有效组
						if shardCount[0] > 0 {
							for i := 0; i < NShards && shardCount[0] > 0; i++ {
								if newConfig.Shards[i] != 0 {
									continue
								}
								// 找到分片最少的有效组（优先选择 GID 小的）
								minShards := NShards + 1
								minGid := -1
								for _, gid := range gids {
									if shardCount[gid] < minShards {
										minShards = shardCount[gid]
										minGid = gid
									} else if shardCount[gid] == minShards && gid < minGid {
										minGid = gid
									}
								}
								if minGid != -1 {
									newConfig.Shards[i] = minGid
									shardCount[0]--
									shardCount[minGid]++
								}
							}
						}

						// 逐个转移分片，直到分片数量差值 ≤ 1
						transferStep := 1
						for {
							// 计算当前分片数量
							maxShards := 0
							minShards := NShards + 1
							maxGid := -1
							minGid := -1
							for _, gid := range gids {
								count := shardCount[gid]
								if count > maxShards {
									maxShards = count
									maxGid = gid
								} else if count == maxShards && gid < maxGid {
									maxGid = gid
								}
								if count < minShards {
									minShards = count
									minGid = gid
								} else if count == minShards && gid < minGid {
									minGid = gid
								}
							}

							// 如果差值 ≤ 1，停止转移
							if maxShards-minShards <= 1 {
								break
							}

							// 从 maxGid 转移一个分片到 minGid
							for i := 0; i < NShards; i++ {
								if newConfig.Shards[i] == maxGid {
									newConfig.Shards[i] = minGid
									shardCount[maxGid]--
									shardCount[minGid]++
									transferStep++
									break // 每次只转移一个分片
								}
							}
						}
					}
					sc.configs = append(sc.configs, newConfig)
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Applied Leave, new config num=%d, gids=%v, shards=%v"+colorReset, sc.me, newConfig.Num, gids, newConfig.Shards)
						// 打印分片变化（哪些分片从哪个 GID 变到哪个 GID）
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard changes after Leave (old -> new):"+colorReset, sc.me)
						for i := 0; i < NShards; i++ {
							if oldConfig.Shards[i] != newConfig.Shards[i] {
								log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard %d: %d -> %d"+colorReset,
									sc.me, i, oldConfig.Shards[i], newConfig.Shards[i])
							}
						}
					}
				case "Move":
					// TODO: 实现 Move 逻辑
					oldConfig := sc.configs[len(sc.configs)-1]
					newConfig := Config{
						Num:    oldConfig.Num + 1,
						Shards: oldConfig.Shards,
						Groups: make(map[int][]string),
					}
					for gid, servers := range oldConfig.Groups {
						newConfig.Groups[gid] = append([]string{}, servers...)
					}
					// 打印旧配置的分片分配
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Before Move, old config num=%d, shards=%v, groups=%v, moving shard=%d to gid=%d"+colorReset,
							sc.me, oldConfig.Num, oldConfig.Shards, oldConfig.Groups, op.Shard, op.GID)
					}

					newConfig.Shards[op.Shard] = op.GID
					sc.configs = append(sc.configs, newConfig)
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Applied Move, shard=%d to gid=%d, new config num=%d"+colorReset, sc.me, op.Shard, op.GID, newConfig.Num)
						// 打印分片变化（哪些分片从哪个 GID 变到哪个 GID）
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard changes after Move (old -> new):"+colorReset, sc.me)
						for i := 0; i < NShards; i++ {
							if oldConfig.Shards[i] != newConfig.Shards[i] {
								log.Printf(sc.GetServerColor()+"CtrlServer %d: Shard %d: %d -> %d"+colorReset,
									sc.me, i, oldConfig.Shards[i], newConfig.Shards[i])
							}
						}
					}
				case "Query":
					if op.ConfigNum == -1 || op.ConfigNum >= len(sc.configs) {
						if EnableCtrlServerLogging {
							log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d, using latest config num=%d"+colorReset, sc.me, op.ConfigNum, len(sc.configs)-1)
						}
						config = sc.configs[len(sc.configs)-1]
					} else {
						if EnableCtrlServerLogging {
							log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d, using config num=%d"+colorReset, sc.me, op.ConfigNum, op.ConfigNum)
						}
						config = sc.configs[op.ConfigNum]
					}
				}
				if op.Type != "Query" {
					sc.lastApplied[op.ClientId] = op.RequestId
				}
			} else if op.Type == "Query" {
				if op.ConfigNum == -1 || op.ConfigNum >= len(sc.configs) {
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d, using latest config num=%d"+colorReset, sc.me, op.ConfigNum, len(sc.configs)-1)
					}
					config = sc.configs[len(sc.configs)-1]
				} else {
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Query configNum=%d, using config num=%d"+colorReset, sc.me, op.ConfigNum, op.ConfigNum)
					}
					config = sc.configs[op.ConfigNum]
				}
			}

			needSnapshot = sc.needSnapshot(Commandindex)
			if needSnapshot {
				if EnableCtrlServerLogging {
					log.Printf(sc.GetServerColor()+"CtrlServer %d: Creating snapshot, configs size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, sc.me, len(sc.configs), Commandindex, op.ClientId, op.RequestId)
				}
				sc.takeSnapshot(Commandindex)
			}

			sc.mu.Lock()
			notifyCh, ok := sc.notifyChs[Commandindex]
			sc.mu.Unlock()
			if ok {
				if EnableCtrlServerLogging {
					log.Printf(sc.GetServerColor()+"CtrlServer %d: Found waiting RPC Channel for Commandindex %d, client=%d requestId=%d"+colorReset, sc.me, Commandindex, op.ClientId, op.RequestId)
				}
				op.Config = config
				select {
				case notifyCh <- op:
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Notified RPC for Commandindex %d, client=%d requestId=%d"+colorReset, sc.me, Commandindex, op.ClientId, op.RequestId)
					}
				default:
					if EnableCtrlServerLogging {
						log.Printf(sc.GetServerColor()+"CtrlServer %d: Notification dropped for Commandindex %d, client=%d requestId=%d"+colorReset, sc.me, Commandindex, op.ClientId, op.RequestId)
					}
				}
			} else {
				if EnableCtrlServerLogging {
					log.Printf(sc.GetServerColor()+"CtrlServer %d: No waiting RPC Channel for Commandindex %d， client=%d requestId=%d"+colorReset, sc.me, Commandindex, op.ClientId, op.RequestId)
				}
			}
		} else if msg.SnapshotValid {
			sc.mu.Lock()
			if msg.SnapshotIndex <= sc.lastAppliedIndex {
				if EnableCtrlServerLogging {
					log.Printf(sc.GetServerColor()+"CtrlServer %d: Discarding outdated snapshot at index %d term %d <= sc.lastAppliedIndex %d"+colorReset, sc.me, msg.SnapshotIndex, msg.SnapshotTerm, sc.lastAppliedIndex)
				}
				sc.mu.Unlock()
				continue
			}
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Applying snapshot at index %d term %d, previous lastAppliedIndex %d"+colorReset, sc.me, msg.SnapshotIndex, msg.SnapshotTerm, sc.lastAppliedIndex)
			}
			sc.restoreSnapshot(msg.Snapshot)
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Applied snapshot at index %d term %d, updated lastAppliedIndex to %d"+colorReset, sc.me, msg.SnapshotIndex, msg.SnapshotTerm, sc.lastAppliedIndex)
			}
			sc.mu.Unlock()
		} else {
			if EnableCtrlServerLogging {
				log.Printf(sc.GetServerColor()+"CtrlServer %d: Unexpected message type %v"+colorReset, sc.me, msg)
			}
		}
	}
}

// StartServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me
	sc.maxraftstate = -1 // 默认不启用快照，除非明确指定

	sc.configs = make([]Config, 1)
	sc.configs[0] = Config{
		Num:    0,
		Shards: [NShards]int{}, // All zeros (GID 0)
		Groups: map[int][]string{},
	}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	sc.lastApplied = make(map[int64]int64)
	sc.notifyChs = make(map[int]chan Op)
	sc.lastAppliedIndex = 0
	sc.lastSnapshotIndex = 0
	sc.persister = persister

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		sc.restoreSnapshot(snapshot)
	}

	go sc.applyLoop()
	return sc
}
