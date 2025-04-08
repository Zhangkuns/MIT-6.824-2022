package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"mit6.5840/labgob"
	"mit6.5840/labrpc"
	"mit6.5840/raft"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// 定义颜色常量
const (
	// Server 0 的颜色 (绿色系)
	colorS0Follower = "\033[38;2;144;238;144m" // Light Green (浅绿)
	colorS0Leader   = "\033[38;2;34;139;34m"   // Dark Green (森林绿)

	// Server 1 的颜色 (红色系)
	colorS1Follower = "\033[38;2;255;182;193m" // Light Pink (浅红)
	colorS1Leader   = "\033[38;2;178;34;34m"   // Firebrick (深红)

	// Server 2 的颜色 (蓝色系)
	colorS2Follower = "\033[38;2;135;206;235m" // Sky Blue (天蓝)
	colorS2Leader   = "\033[38;2;65;105;225m"  // Royal Blue (皇家蓝)

	// Server 3 的颜色 (黄色系)
	colorS3Follower = "\033[38;2;255;255;224m" // Light Yellow (浅黄)
	colorS3Leader   = "\033[38;2;218;165;32m"  // Goldenrod (金黄)

	// Server 4 的颜色 (紫色系)
	colorS4Follower = "\033[38;2;230;190;255m" // Light Purple (浅紫)
	colorS4Leader   = "\033[38;2;128;0;128m"   // Purple (深紫)

	// 其他测试信息的颜色
	// 其他测试信息的颜色
	// connect color
	colorTest1 = "\033[38;2;255;140;0m" // Dark Orange (深橙色) connect color
	// disconnect color
	colorTest2 = "\033[38;2;139;69;19m" // Saddle Brown (马鞍棕色) disconnect color
	// Start agreement on cmd color
	colorTest3 = "\033[38;2;192;192;192m" // Silver (银色)
	// End or Start Iteration color
	colorTest4 = "\033[38;2;169;169;169m" // Dark Gray (深灰色)
	// Crash color
	colorTest5 = "\033[38;2;255;0;255m" // Magenta (品红色)
	// Restart color
	colorTest6  = "\033[38;2;0;255;255m" // Cyan (青色)
	colorNewLog = "\033[38;2;0;255;255m" // Bright Cyan
	colorReset  = "\033[0m"
)

var EnableKVStoreLogging = false // 默认KV Store禁用日志打印
var EnableServerLogging = false  // 默认Server禁用日志打印
type Op struct {
	// Command Type and Value
	Type  string
	Key   string
	Value string
	// Client ID
	ClientId int64
	// Request ID
	RequestId int64
}

type KVServer struct {
	//mu      sync.Mutex
	mu      sync.RWMutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvstore           map[string]string // Key-Value Store
	lastApplied       map[int64]int64   // Last Applied ID
	lastAppliedIndex  int
	notifyChs         map[int]chan Op // Notify channels for each request
	persister         *raft.Persister
	lastSnapshotIndex int // 记录上一次快照的索引
	snapshotCooldown  int // 快照冷却期（日志条目数）
}

type KVSnapshot struct {
	Kvstore          map[string]string
	LastApplied      map[int64]int64
	LastAppliedIndex int
}

func (kv *KVServer) GetServerColor() string {
	switch kv.me {
	case 0:
		return colorS0Follower
	case 1:
		return colorS1Follower
	case 2:
		return colorS2Follower
	case 3:
		return colorS3Follower
	case 4:
		return colorS4Follower
	default:
		return ""
	}
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		Type:      "Get",
		Key:       args.Key,
		Value:     "",
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}
	// commit the ops to Raft
	//index, term, isLeader := kv.rf.Start(op)
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Trying to start Get, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	// log the get
	index, _, isLeader := kv.rf.Start(op)
	// not log the get
	// _, isLeader := kv.rf.GetState()
	// if not the leader, return error
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: Get key=%s request failed, not the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
		}
		return
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s request , is the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	// create message notification channel
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s, waiting for kv.mu.Lock, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	// log the get
	kv.mu.Lock()
	notifyCh := make(chan Op, 1)
	kv.notifyChs[index] = notifyCh
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Get key=%s, make the channel index %d, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, index, op.ClientId, op.RequestId)
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if ch, ok := kv.notifyChs[index]; ok {
			close(ch) // 关闭通道
			delete(kv.notifyChs, index)
		}
		kv.mu.Unlock()
	}()

	// wait the ops finish or timeout
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, kv.me, index, op.ClientId, op.RequestId)
	}
	select {
	case Response := <-notifyCh:
		// check if the committed operation is the same as the one we sent
		if Response.ClientId == op.ClientId && Response.RequestId == op.RequestId {
			reply.Value = Response.Value
			reply.Err = OK
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Get key=%s succeeded，value = %v, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, Response.Value, Response.ClientId, Response.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Get key=%s failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		// timeout
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: Get key=%s failed, timeout, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
		}
	}
	// not log the get
	//kv.mu.Lock()
	//value, exists := kv.kvstore[args.Key]
	//kv.mu.Unlock()
	//if exists {
	//	reply.Value = value
	//	reply.Err = OK
	//	log.Printf(kv.GetServerColor()+"Server %d: Get key=%s succeeded, value=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, value, args.ClientId, args.RequestId)
	//} else {
	//	reply.Value = ""
	//	reply.Err = ErrNoKey
	//	log.Printf(kv.GetServerColor()+"Server %d: Get key=%s failed, no such key, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, args.ClientId, args.RequestId)
	//}
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// op
	op := Op{
		Type:      args.Op,
		Key:       args.Key,
		Value:     args.Value,
		ClientId:  args.ClientId,
		RequestId: args.RequestId,
	}

	// commit the ops to Raft
	//index, term, isLeader := kv.rf.Start(op)
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Trying to start PutAppend, key=%s, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	index, _, isLeader := kv.rf.Start(op)

	// if not the leader, return error
	if !isLeader {
		reply.Err = ErrWrongLeader
		reply.WrongLeader = true
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s request failed, not the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
		}
		return
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s request , is the leader, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	// create message notification channel
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s, waiting for kv.mu.Lock, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
	}
	kv.mu.Lock()
	notifyCh := make(chan Op, 1)
	kv.notifyChs[index] = notifyCh
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s, make the channel index %d, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, index, op.ClientId, op.RequestId)
	}
	kv.mu.Unlock()

	defer func() {
		kv.mu.Lock()
		if ch, ok := kv.notifyChs[index]; ok {
			close(ch) // 关闭通道
			delete(kv.notifyChs, index)
		}
		kv.mu.Unlock()
	}()

	// wait the ops finish or timeout
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Waiting for notification at index %d, clientId=%d, requestId=%d"+colorReset, kv.me, index, op.ClientId, op.RequestId)
	}
	select {
	case Response := <-notifyCh:
		// check if the committed operation is the same as the one we sent
		if Response.ClientId == op.ClientId && Response.RequestId == op.RequestId {
			reply.Err = OK
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s succeeded, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
			}
		} else {
			reply.Err = ErrClientOrRequest
			reply.WrongLeader = true
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s failed, wrong ClientId or RequestId, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
			}
		}
	case <-time.After(500 * time.Millisecond):
		// timeout
		reply.WrongLeader = true
		reply.Err = ErrWrongLeader
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: PutAppend key=%s failed, timeout, clientId=%d, requestId=%d"+colorReset, kv.me, args.Key, op.ClientId, op.RequestId)
		}
	}

}

// Kill
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Kill\n", kv.me)
	}
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

func (kv *KVServer) printKVStore() {
	if EnableKVStoreLogging {
		log.Printf(kv.GetServerColor()+"Server %d: kvstore=%v, lastAppliedIndex=%d"+colorReset, kv.me, kv.kvstore, kv.lastAppliedIndex)
	}
}

func (kv *KVServer) createSnapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kvsnapshot := KVSnapshot{
		Kvstore:          kv.kvstore,
		LastApplied:      kv.lastApplied,
		LastAppliedIndex: kv.lastAppliedIndex,
	}
	if err := e.Encode(kvsnapshot); err != nil {
		log.Fatalf(kv.GetServerColor()+"Server %d: Failed to encode kvsnapshot: %v"+colorReset, kv.me, err)
	}
	return w.Bytes()
}

func (kv *KVServer) takeSnapshot(index int) {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	kvsnapshot := KVSnapshot{
		Kvstore:          kv.kvstore,
		LastApplied:      kv.lastApplied,
		LastAppliedIndex: kv.lastAppliedIndex,
	}
	if err := e.Encode(kvsnapshot); err != nil {
		log.Fatalf(kv.GetServerColor()+"Server %d: Failed to encode kvsnapshot: %v"+colorReset, kv.me, err)
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Creating snapshot, kvstore size=%d"+colorReset, kv.me, len(kv.kvstore))
	}
	kv.rf.Snapshot(kv.lastAppliedIndex, w.Bytes())
	kv.lastSnapshotIndex = kv.lastAppliedIndex
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Take snapshot at index %d, size=%d"+colorReset, kv.me, kv.lastAppliedIndex, kv.persister.RaftStateSize())
	}
}

func (kv *KVServer) needSnapshot(index int) bool {
	// 没有启用snapshot
	if kv.maxraftstate == -1 {
		// log.Printf(kv.GetServerColor()+"Server %d: Skipping snapshot at index %d, not enable snapshot"+colorReset, kv.me, index)
		return false
	}

	statesize := kv.persister.RaftStateSize()
	if statesize <= kv.maxraftstate*3/2 {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: Skipping snapshot at index %d, size=%d < maxraftstate=%d*3/2"+colorReset, kv.me, index, statesize, kv.maxraftstate)
		}
		return false
	}
	// 检查冷却期
	if index-kv.lastSnapshotIndex < kv.snapshotCooldown {
		if EnableServerLogging {
			log.Printf("Server %d: Skipping snapshot at index %d, within cooldown (lastSnapshotIndex=%d)", kv.me, index, kv.lastSnapshotIndex)
		}
		return false
	}
	// 确保快照索引不超过已应用的索引，避免 Raft 处理未来日志
	if index > kv.lastAppliedIndex {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: Skipping snapshot at index %d, exceeds LastAppliedIndex %d"+colorReset, kv.me, index, kv.lastAppliedIndex)
		}
		return false
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Need snapshot at index %d, size=%d > maxraftstate=%d"+colorReset, kv.me, index, kv.persister.RaftStateSize(), kv.maxraftstate)
	}
	return true
}

func (kv *KVServer) restoreSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		if EnableServerLogging {
			log.Printf(kv.GetServerColor()+"Server %d: Empty snapshot, nothing to restore"+colorReset, kv.me)
		}
		return
	}
	if EnableServerLogging {
		log.Printf(kv.GetServerColor()+"Server %d: Try to Restoring snapshot, size=%d"+colorReset, kv.me, len(snapshot))
	}
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var snap KVSnapshot
	if err := d.Decode(&snap); err != nil {
		log.Fatalf("Server %d: Failed to decode snapshot: %v", kv.me, err)
	}
	kv.kvstore = snap.Kvstore
	kv.lastApplied = snap.LastApplied
	kv.lastAppliedIndex = snap.LastAppliedIndex
	if EnableServerLogging {
		log.Printf("Server %d: Restored snapshot, kvstore size=%d", kv.me, len(kv.kvstore))
	}
}

// 应用提交的操作到状态机的循环
func (kv *KVServer) applyLoop() {
	for !kv.killed() {
		msg, ok := <-kv.applyCh

		if !ok { // 检查通道关闭
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: applyCh closed, exiting applyLoop"+colorReset, kv.me)
			}
			return
		}

		if msg.CommandValid {
			// 是普通的命令
			// 转换命令为 Op
			op := msg.Command.(Op)
			Commandindex := msg.CommandIndex

			var needSnapshot bool

			// 检查命令顺序是否正确
			if Commandindex <= kv.lastAppliedIndex {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %v discards outdated message Index %v because LastAppliedIndex is %v, clientId=%d, requestId=%d"+colorReset, kv.me, Commandindex, kv.lastAppliedIndex, op.ClientId, op.RequestId)
				}
				continue
			}

			// Update LastAppliedIndex before processing the operation
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Update LastAppliedIndex at Commandindex %d, type=%s, client=%d, requestId=%d"+colorReset, kv.me, Commandindex, op.Type, op.ClientId, op.RequestId)
			}
			kv.lastAppliedIndex = Commandindex

			// 检查是否是重复的操作
			isDuplicate := false
			if lastReqId, ok := kv.lastApplied[op.ClientId]; ok {
				if op.RequestId <= lastReqId {
					isDuplicate = true
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Server %d: Duplicated request, client %d, requestId %d"+colorReset, kv.me, op.ClientId, op.RequestId)
					}
				}
			}

			// 只应用非重复的操作
			if op.Type == "Get" {
				op.Value = kv.kvstore[op.Key]
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d : kvstore after Get Key %s Value %s, client=%d, requestId=%d"+colorReset, kv.me, op.Key, op.Value, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
			} else if !isDuplicate {
				// 根据操作类型修改状态
				switch op.Type {
				case "Put":
					kv.kvstore[op.Key] = op.Value
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Server %d : kvstore after Put Key %s Value %s, client=%d, requestId=%d"+colorReset, kv.me, op.Key, op.Value, op.ClientId, op.RequestId)
						kv.printKVStore()
					}
				case "Append":
					kv.kvstore[op.Key] = kv.kvstore[op.Key] + op.Value
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Server %d : kvstore after Append Key %s Value %s, client=%d, requestId=%d"+colorReset, kv.me, op.Key, op.Value, op.ClientId, op.RequestId)
						kv.printKVStore()
					}
				}
				// 更新去重表
				kv.lastApplied[op.ClientId] = op.RequestId
			}

			// 检查是否需要快照
			needSnapshot = kv.needSnapshot(Commandindex)

			if needSnapshot {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: Creating snapshot, kvstore size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.me, len(kv.kvstore), Commandindex, op.ClientId, op.RequestId)
					log.Printf(kv.GetServerColor()+"Server %d: kvstore before snapshot, size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.me, len(kv.kvstore), Commandindex, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
				kv.takeSnapshot(Commandindex)
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: kvstore after snapshot, size=%d, Commandindex=%d, clientId=%d, requestId=%d"+colorReset, kv.me, len(kv.kvstore), Commandindex, op.ClientId, op.RequestId)
					kv.printKVStore()
				}
			}

			// 通知等待的 RPC 处理程序
			kv.mu.Lock()
			notifyCh, ok := kv.notifyChs[Commandindex]
			kv.mu.Unlock() // 释放锁
			if ok {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: Found waiting RPC Channel for Commandindex %d, client=%d requestId=%d"+colorReset, kv.me, Commandindex, op.ClientId, op.RequestId)
				}
				select {
				case notifyCh <- op:
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Server %d: Notified RPC for Commandindex %d, client=%d requestId=%d"+colorReset, kv.me, Commandindex, op.ClientId, op.RequestId)
					}
				default:
					if EnableServerLogging {
						log.Printf(kv.GetServerColor()+"Server %d: Notification dropped for Commandindex %d, client=%d requestId=%d"+colorReset, kv.me, Commandindex, op.ClientId, op.RequestId)
					}
				}
			} else {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: No waiting RPC Channel for Commandindex %d， client=%d requestId=%d"+colorReset, kv.me, Commandindex, op.ClientId, op.RequestId)
				}
			}
		} else if msg.SnapshotValid {
			// 是快照
			kv.mu.Lock()
			// 1. 检查快照索引是否有效
			if msg.SnapshotIndex <= kv.lastAppliedIndex {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: Discarding outdated snapshot at index %d term %d <= kv.lastAppliedIndex %d"+colorReset, kv.me, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				}
				kv.mu.Unlock()
				continue
			}
			// 2. 恢复快照
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Applying snapshot at index %d term %d, previous lastAppliedIndex %d"+colorReset, kv.me, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				kv.printKVStore()
			}
			kv.restoreSnapshot(msg.Snapshot)
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Applied snapshot at index %d term %d, updated lastAppliedIndex to %d"+colorReset, kv.me, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				kv.printKVStore()
			}
			// 3. 更新 LastAppliedIndex
			if msg.SnapshotIndex != kv.lastAppliedIndex {
				if EnableServerLogging {
					log.Printf(kv.GetServerColor()+"Server %d: Error LastAppliedIndex, msg.SnapshotIndex %d term %d != kv.storedsnapshotIndex %d"+colorReset, kv.me, msg.SnapshotIndex, msg.SnapshotTerm, kv.lastAppliedIndex)
				}
			}
			if EnableServerLogging {
				log.Printf("Server %d: Applied snapshot at index %d term %d", kv.me, msg.SnapshotIndex, msg.SnapshotTerm)
			}
			kv.mu.Unlock()
		} else {
			// 其他类型的消息
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Unexpected message type %v"+colorReset, kv.me, msg)
			}
		}
	}
}

// StartKVServer
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})
	labgob.Register(KVSnapshot{})
	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister
	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.kvstore = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.notifyChs = make(map[int]chan Op)
	kv.lastAppliedIndex = 0
	kv.lastSnapshotIndex = 0
	kv.snapshotCooldown = 10

	snapshot := persister.ReadSnapshot()
	if len(snapshot) > 0 {
		r := bytes.NewBuffer(snapshot)
		d := labgob.NewDecoder(r)
		var snap KVSnapshot
		if err := d.Decode(&snap); err == nil {
			kv.kvstore = snap.Kvstore
			kv.lastApplied = snap.LastApplied
			kv.lastAppliedIndex = snap.LastAppliedIndex
			if EnableServerLogging {
				log.Printf(kv.GetServerColor()+"Server %d: Restored snapshot, kvstore size=%d"+colorReset, kv.me, len(kv.kvstore))
				kv.printKVStore()
			}
		} else {
			if EnableServerLogging {
				log.Fatalf(kv.GetServerColor()+"Server %d: Failed to decode snapshot: %v", kv.me, err)
			}
		}
	}

	// 启动一个 goroutine 来处理应用日志的循环
	go kv.applyLoop()
	//go kv.monitorChannels()
	return kv
}

// 添加一个定期运行的 goroutine
func (kv *KVServer) monitorChannels() {
	for !kv.killed() {
		time.Sleep(1 * time.Second)
		kv.mu.Lock()
		log.Printf(kv.GetServerColor()+"Server %d: Current notification channels: %v"+colorReset, kv.me, kv.notifyChs)
		kv.mu.Unlock()
	}
}
