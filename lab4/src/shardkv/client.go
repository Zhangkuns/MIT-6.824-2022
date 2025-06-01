package shardkv

//
// client code to talk to a sharded key/value service.
//
// the client first talks to the shardctrler to find out
// the assignment of Shards (keys) to groups, and then
// talks to the group that holds the key's shard.
//

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"sort"
	"strings"
	"sync"
	"time"

	"mit6.5840/labrpc"
	"mit6.5840/shardctrler"
)

// 客户端颜色常量（与 kvraft 保持一致）
const (
	// colorReset = "\033[0m"
	colorClient0  = "\033[38;2;255;255;255m"
	colorClient10 = "\033[38;2;85;107;47m"
	colorClient1  = "\033[38;2;147;112;219m"
	colorClient2  = "\033[38;2;255;127;127m"
	colorClient3  = "\033[38;2;0;191;255m"
	colorClient4  = "\033[38;2;210;105;30m"
	colorClient5  = "\033[38;2;124;252;0m"
	colorClient6  = "\033[38;2;0;105;148m"
	colorClient7  = "\033[38;2;188;0;77m"
	colorClient8  = "\033[38;2;255;165;0m"
	colorClient9  = "\033[38;2;112;128;144m"
	colorNewLog   = "\033[38;2;0;255;255m" // Bright Cyan
	colorReset    = "\033[0m"
)

var EnableClientLog = true

// which shard is a key in?
// please use this function,
// and please do not change it.
func key2shard(key string) int {
	shard := 0
	if len(key) > 0 {
		shard = int(key[0])
	}
	shard %= shardctrler.NShards
	return shard
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type Clerk struct {
	sm       *shardctrler.Clerk
	config   shardctrler.Config
	make_end func(string) *labrpc.ClientEnd
	// You will have to modify this struct.
	tso_end        *labrpc.ClientEnd // 用于获取时间戳
	clientId       int64
	requestCounter int64
	mu             sync.Mutex // Protects config, rpcRequestCounter, leaderHints
	// 用于加速
	leaderHints map[int]int // GID -> server Index
}

// MakeClerk
// the tester calls MakeClerk.
// ctrlers[] is needed to call shardctrler.MakeClerk().
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs.
func MakeClerk(ctrlers []*labrpc.ClientEnd, network *labrpc.Network, make_end func(string) *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.sm = shardctrler.MakeClerk(ctrlers)
	ck.make_end = make_end
	// You'll have to add code here.
	ck.tso_end = MakeTSOClientEnd(network, GlobalTSOServerName)
	ck.clientId = nrand()
	ck.requestCounter = 0
	ck.leaderHints = make(map[int]int)
	ck.config = ck.sm.Query(-1)
	return ck
}

func (ck *Clerk) GetTimestamp() uint64 {
	args := GetTimestampArgs{ClientId: ck.clientId}
	var reply GetTimestampReply
	for {
		if ck.tso_end.Call("TSOServer.GetTimestamp", &args, &reply) {
			return reply.Timestamp
		}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Failed to get timestamp from TSO, retrying..."+colorReset, ck.clientId)
		}
		time.Sleep(50 * time.Millisecond)
	}
}

// Generates a unique RequestId for an RPC call.
func (ck *Clerk) newRpcRequestID() int64 {
	ck.mu.Lock()
	defer ck.mu.Unlock()
	ck.requestCounter++
	// Construct a unique ID. High bits from client, low bits from counter.
	return (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
}

type TxnWriteOp struct { // Structure to hold buffered writes/appends
	Value      string // For Put: the value. For Append: value to append.
	IntentType OperationIntentType
}

type Txn struct {
	clerk      *Clerk
	startTs    uint64
	commitTs   uint64 // Set during commit phase
	ops        map[string]TxnWriteOp
	primaryKey string // One of the keys in writes, or empty if no writes
}

func (ck *Clerk) BeginTxn() *Txn {
	startTs := ck.GetTimestamp()
	if EnableClientLog {
		log.Printf(ck.GetClientColor()+"Client %d: BeginTxn. StartTs: %d"+colorReset, ck.clientId, startTs)
	}
	return &Txn{
		clerk:   ck,
		startTs: startTs,
		ops:     make(map[string]TxnWriteOp),
	}
}

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
// You will have to modify this function.
func (txn *Txn) Get(key string) (string, Err) {
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Get Key '%s'"+colorReset, txn.clerk.clientId, txn.startTs, key)
	}
	args := ReadArgs{
		Key:       key,
		ReadTs:    txn.startTs, // Read at transaction's start_ts
		ClientId:  txn.clerk.clientId,
		RequestId: txn.clerk.newRpcRequestID(),
	}

	for {
		args.ConfigNum = txn.clerk.config.Num // Use current known config
		shard := key2shard(key)
		gid := txn.clerk.config.Shards[shard]
		hintedLeaderIdx, hintExists := txn.clerk.leaderHints[gid]

		if servers, ok := txn.clerk.config.Groups[gid]; ok {
			// Try hinted leader first if available
			if hintExists && hintedLeaderIdx >= 0 && hintedLeaderIdx < len(servers) {
				srvName := servers[hintedLeaderIdx]
				srv := txn.clerk.make_end(srvName)
				var reply ReadReply
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending Read for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, srvName, gid)
				}
				rpcOk := srv.Call("ShardKV.Read", &args, &reply)
				if rpcOk {
					if reply.Err == OK {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' SUCCESS. Value: '%s'"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Value)
						}
						return reply.Value, OK
					}
					if reply.Err == ErrNoKey {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' SUCCESS. ErrNoKey."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
						}
						return "", ErrNoKey // Or return a specific ErrNoKey if preferred
					}
					if reply.Err == ErrWrongLeader {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Hinted leader %s for GID %d was WRONG."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, srvName, gid)
						}
						txn.clerk.mu.Lock()
						delete(txn.clerk.leaderHints, gid) // 清除错误的 hint
						txn.clerk.mu.Unlock()
					}
					if reply.Err == ErrWrongConfig || reply.Err == ErrWrongGroup {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' failed. ErrWrongGroup from GID %d. Breaking to refresh config."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, gid)
						}
					}
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' failed with error %s"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Err)
					}
				} else { // RPC failed
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, srvName)
					}
				}
			}
			// Try all servers in the group
			for si := 0; si < len(servers); si++ {
				srv := txn.clerk.make_end(servers[si])
				var reply ReadReply
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending Read for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si], gid)
				}
				rpcOk := srv.Call("ShardKV.Read", &args, &reply)

				if rpcOk {
					// txn.clerk.queryConfigIfNeeded(reply.CurrentConfigNum) // Update config if server has newer
					if reply.Err == OK {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' SUCCESS. Value: '%s'"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Value)
						}
						txn.clerk.mu.Lock()
						txn.clerk.leaderHints[gid] = si
						txn.clerk.mu.Unlock()
						return reply.Value, OK
					}
					if reply.Err == ErrNoKey {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' SUCCESS. ErrNoKey."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
						}
						return "", ErrNoKey // Or return a specific ErrNoKey if preferred
					}
					if reply.Err == ErrWrongGroup || reply.Err == ErrWrongConfig {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' failed. ErrWrongGroup from GID %d. Breaking to refresh config."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, gid)
						}
						break // Break from server loop to refresh config
					}
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' failed with %s from srv %s. Trying next server or refreshing."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Err, servers[si])
					}
				} else { // RPC failed
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Read Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si])
					}
				}
			} // end server loop
		}
		time.Sleep(100 * time.Millisecond)
		// txn.clerk.mu.Lock()
		txn.clerk.config = txn.clerk.sm.Query(-1) // Refresh config
		if EnableClientLog {
			log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Refreshed config to num %d for Key '%s'"+colorReset, txn.clerk.clientId, txn.startTs, txn.clerk.config.Num, key)
		}
		// txn.clerk.mu.Unlock()
		// args.RequestId = txn.clerk.newRpcRequestID() // New RequestID for retry
	}
	return "", ErrWrongLeader
}

// Put buffers a write for the transaction.
// 仅用来构成一个Txn的缓冲
func (txn *Txn) Put(key string, value string) {
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Buffering Put Key '%s', Value '%s'"+colorReset, txn.clerk.clientId, txn.startTs, key, value)
	}
	txn.ops[key] = TxnWriteOp{Value: value, IntentType: IntentPut}
}

func (txn *Txn) Append(key string, valueToAppend string) {
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Buffering Append for Key '%s', ValueToAppend '%s'"+colorReset, txn.clerk.clientId, txn.startTs, key, valueToAppend)
	}
	txn.ops[key] = TxnWriteOp{Value: valueToAppend, IntentType: IntentAppend}
}

// Commit attempts to commit the transaction using 2PC.
func (txn *Txn) Commit() error {
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Starting Commit. Writes: %d"+colorReset, txn.clerk.clientId, txn.startTs, len(txn.ops))
	}
	if len(txn.ops) == 0 {
		if EnableClientLog {
			log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): No writes, commit is trivial."+colorReset, txn.clerk.clientId, txn.startTs)
		}
		txn.commitTs = txn.clerk.GetTimestamp() // Still get a commit_ts for ordering
		return nil
	}

	// 1. Select Primary Key
	// For simplicity, pick the lexicographically smallest key as primary
	var keys []string
	for k := range txn.ops {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	txn.primaryKey = keys[0]
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Selected PrimaryKey '%s'"+colorReset, txn.clerk.clientId, txn.startTs, txn.primaryKey)
	}

	// 2. Prewrite Phase
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Starting Prewrite Phase."+colorReset, txn.clerk.clientId, txn.startTs)
	}
	prewriteSuccess := true
	// Prewrite Primary first, then secondaries (can be parallelized)
	for _, key := range keys {
		opInfo := txn.ops[key]
		args := PrewriteArgs{
			Key:        key,
			Value:      opInfo.Value,
			TxnStartTs: txn.startTs,
			PrimaryKey: txn.primaryKey,
			IntentType: opInfo.IntentType,
			ClientId:   txn.clerk.clientId,
			RequestId:  txn.clerk.newRpcRequestID(),
		}

	prewriteRetryLoop:
		for {
			args.ConfigNum = txn.clerk.config.Num
			shard := key2shard(key)
			gid := txn.clerk.config.Shards[shard]
			hintedLeaderIdx, hintExists := txn.clerk.leaderHints[gid]
			if EnableClientLog {
				log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): prewrite RequestId %d."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId)
			}
			if servers, ok := txn.clerk.config.Groups[gid]; ok {
				// Try hinted leader first if available
				if hintExists && hintedLeaderIdx >= 0 && hintedLeaderIdx < len(servers) {
					srvName := servers[hintedLeaderIdx]
					srv := txn.clerk.make_end(srvName)
					var reply PrewriteReply
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending Prewrite for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, srvName, gid)
					}
					rpcOk := srv.Call("ShardKV.Prewrite", &args, &reply)

					if rpcOk {
						if reply.Err == OK {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
							}
							prewriteSuccess = true
							break prewriteRetryLoop // Success for this key
						}
						if reply.Err == ErrWrongLeader {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Hinted leader %s for GID %d was WRONG."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, srvName, gid)
							}
							txn.clerk.mu.Lock()
							delete(txn.clerk.leaderHints, gid) // 清除错误的 hint
							txn.clerk.mu.Unlock()
						}
						if reply.Err == ErrConflict {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
							}
							prewriteSuccess = false
							break prewriteRetryLoop
						}
						if reply.Err == ErrWrongConfig || reply.Err == ErrWrongGroup {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
							}
						}
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed with error %s"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Err)
						}
					} else { // RPC failed
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, srvName)
						}
					}
				}
				for si := 0; si < len(servers); si++ {
					srv := txn.clerk.make_end(servers[si])
					var reply PrewriteReply
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending Prewrite for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si], gid)
					}
					rpcOk := srv.Call("ShardKV.Prewrite", &args, &reply)

					if rpcOk {
						if reply.Err == OK {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
							}
							prewriteSuccess = true
							txn.clerk.mu.Lock()
							txn.clerk.leaderHints[gid] = si
							txn.clerk.mu.Unlock()
							break prewriteRetryLoop // Success for this key
						} else { // reply.Err != OK
							if reply.Err == ErrWrongGroup || reply.Err == ErrWrongConfig {
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed. ErrWrongGroup from GID %d. Breaking to refresh config."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, gid)
								}
								break // Break from server loop to refresh config
							} else if reply.Err == ErrConflict {
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
								}
								prewriteSuccess = false
								break prewriteRetryLoop
							} else {
								// Other errors (WrongLeader, Timeout), try next server or refresh
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed with error %s from srv %s."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Err, servers[si])
								}
							}
						}
					} else { // RPC failed
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si])
						}
					}
				} // end server loop
			} else {
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Prewrite Key '%s' failed. No servers found for GID %d."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, gid)
				}
			}
			if !prewriteSuccess { // If conflict occurred for this key
				break prewriteRetryLoop
			}
			time.Sleep(100 * time.Millisecond)
			txn.clerk.config = txn.clerk.sm.Query(-1)
			if EnableClientLog {
				log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Refreshed config to num %d for Key '%s'"+colorReset, txn.clerk.clientId, txn.startTs, txn.clerk.config.Num, key)
			}
		} // end prewriteRetryLoop for one key

		if !prewriteSuccess {
			break // Break from Prewrite loop for all keys
		}
	} // end Prewrite phase for all keys

	if !prewriteSuccess {
		if EnableClientLog {
			log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Prewrite phase failed. Initiating Abort."+colorReset, txn.clerk.clientId, txn.startTs)
		}
		txn.Abort() // Best effort abort
		return fmt.Errorf("prewrite failed, transaction aborted")
	}

	// 3. Commit Phase
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Prewrite phase SUCCESS. Starting Commit Phase."+colorReset, txn.clerk.clientId, txn.startTs)
	}
	txn.commitTs = txn.clerk.GetTimestamp()
	if txn.commitTs <= txn.startTs { // Ensure commit_ts > start_ts
		txn.commitTs = txn.startTs + 1
		// A real TSO guarantees this, but our mock TSO might need this nudge if calls are too fast.
	}

	commitPeimarySuccess := true
	// Commit Primary Key first
	argsCommitPrimary := CommitArgs{
		Key:        txn.primaryKey,
		TxnStartTs: txn.startTs,
		CommitTs:   txn.commitTs,
		IsPrimary:  true,
		ClientId:   txn.clerk.clientId,
		RequestId:  txn.clerk.newRpcRequestID(),
	}

commitPrimaryRetryLoop:
	for {
		argsCommitPrimary.ConfigNum = txn.clerk.config.Num
		shard := key2shard(txn.primaryKey)
		gid := txn.clerk.config.Shards[shard]
		hintedLeaderIdx, hintExists := txn.clerk.leaderHints[gid]
		var reply CommitReply
		if servers, ok := txn.clerk.config.Groups[gid]; ok {
			if hintExists && hintedLeaderIdx >= 0 && hintedLeaderIdx < len(servers) {
				srvName := servers[hintedLeaderIdx]
				srv := txn.clerk.make_end(srvName)
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending CommitPrimary for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key, srvName, gid)
				}
				rpcOk := srv.Call("ShardKV.Commit", &argsCommitPrimary, &reply)

				if rpcOk {
					if reply.Err == OK {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key)
						}
						commitPeimarySuccess = true
						break commitPrimaryRetryLoop // Success for this key
					}
					if reply.Err == ErrWrongLeader {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Hinted leader %s for GID %d was WRONG."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, srvName, gid)
						}
						txn.clerk.mu.Lock()
						delete(txn.clerk.leaderHints, gid) // 清除错误的 hint
						txn.clerk.mu.Unlock()
					}
					if reply.Err == ErrWrongConfig || reply.Err == ErrWrongGroup {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key)
						}
					}
					if reply.Err == ErrConflict {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key)
						}
						commitPeimarySuccess = false
					}
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed with error %s"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key, reply.Err)
					}
				} else { // RPC failed
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key, srvName)
					}
				}
			}
			for si := 0; si < len(servers); si++ {
				srv := txn.clerk.make_end(servers[si])
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending CommitPrimary for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, txn.primaryKey, servers[si], gid)
				}
				rpcOk := srv.Call("ShardKV.Commit", &argsCommitPrimary, &reply)
				if rpcOk {
					if reply.Err == OK {
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, txn.primaryKey)
						}
						commitPeimarySuccess = true
						txn.clerk.mu.Lock()
						txn.clerk.leaderHints[gid] = si
						txn.clerk.mu.Unlock()
						break commitPrimaryRetryLoop // Primary committed successfully
					} else { // reply.Err != OK
						if reply.Err == ErrWrongGroup || reply.Err == ErrWrongConfig {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed. Error %s"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key, reply.Err)
							}
							break // Break from server loop to refresh config
						} else if reply.Err == ErrConflict {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key)
							}
							commitPeimarySuccess = false
							break
						} else {
							// Other errors (WrongLeader, Timeout), try next server or refresh
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' failed with error %s from srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, argsCommitPrimary.Key, reply.Err, servers[si])
							}
						}
					}
				} else { // RPC failed
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, txn.primaryKey, servers[si])
					}
				}
			} // end server loop for primary
		}
		// If primary commit failed after trying all servers in group
		if commitPeimarySuccess == false {
			if EnableClientLog {
				log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitPrimary Key '%s' RPC failed after trying all servers."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitPrimary.RequestId, txn.primaryKey)
			}
			break commitPrimaryRetryLoop
		}
		txn.clerk.config = txn.clerk.sm.Query(-1)
		time.Sleep(100 * time.Millisecond)
	} // end commitPrimaryRetryLoop

	if !commitPeimarySuccess {
		if EnableClientLog {
			log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): CommitPrimary failed. Initiating Abort."+colorReset, txn.clerk.clientId, txn.startTs)
		}
		txn.Abort() // Best effort abort
		return fmt.Errorf("commit primary failed, transaction aborted")
	}

	// Commit Secondary Keys (can be done asynchronously and best-effort)
	// For simplicity here, we do it synchronously.
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): CommitPrimary SUCCESS. Committing Secondaries."+colorReset, txn.clerk.clientId, txn.startTs)
	}
	for _, key := range keys {
		if key == txn.primaryKey {
			continue
		}
		argsCommitSecondary := CommitArgs{
			Key:        key,
			TxnStartTs: txn.startTs,
			CommitTs:   txn.commitTs,
			IsPrimary:  false,
			ClientId:   txn.clerk.clientId,
		}
	commitSecondaryRetryLoop:
		for {
			argsCommitSecondary.RequestId = txn.clerk.newRpcRequestID()
			argsCommitSecondary.ConfigNum = txn.clerk.config.Num
			shard := key2shard(key)
			gid := txn.clerk.config.Shards[shard]
			hintedLeaderIdx, hintExists := txn.clerk.leaderHints[gid]
			var reply CommitReply
			if servers, ok := txn.clerk.config.Groups[gid]; ok {
				if hintExists && hintedLeaderIdx >= 0 && hintedLeaderIdx < len(servers) {
					srvName := servers[hintedLeaderIdx]
					srv := txn.clerk.make_end(srvName)
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending CommitSecondary for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, argsCommitSecondary.Key, srvName, gid)
					}
					rpcOk := srv.Call("ShardKV.Commit", &argsCommitSecondary, &reply)

					if rpcOk {
						if reply.Err == OK {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, argsCommitSecondary.Key)
							}
							break commitSecondaryRetryLoop // Success for this key
						}
						if reply.Err == ErrWrongLeader {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Hinted leader %s for GID %d was WRONG."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, srvName, gid)
							}
							txn.clerk.mu.Lock()
							delete(txn.clerk.leaderHints, gid) // 清除错误的 hint
							txn.clerk.mu.Unlock()
						}
						if reply.Err == ErrWrongConfig || reply.Err == ErrWrongGroup {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key)
							}
						}
						if reply.Err == ErrConflict {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key)
							}
						}
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed with error %s"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key, reply.Err)
						}
					} else { // RPC failed
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key, srvName)
						}
					}
				}
				for si := 0; si < len(servers); si++ {
					srv := txn.clerk.make_end(servers[si])
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending CommitSecondary for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key, servers[si], gid)
					}
					rpcOk := srv.Call("ShardKV.Commit", &argsCommitSecondary, &reply)
					if rpcOk {
						if reply.Err == OK {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key)
							}
							txn.clerk.mu.Lock()
							txn.clerk.leaderHints[gid] = si
							txn.clerk.mu.Unlock()
							break commitSecondaryRetryLoop
						} else { // reply.Err != OK
							if reply.Err == ErrWrongGroup || reply.Err == ErrWrongConfig {
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed. Error %s"+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, argsCommitSecondary.Key, reply.Err)
								}
								break // Break from server loop to refresh config
							} else if reply.Err == ErrConflict {
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed with ErrConflict. Retrying."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, argsCommitSecondary.Key)
								}
								break
							} else {
								// Other errors (WrongLeader, Timeout), try next server or refresh
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' failed with error %s from srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, argsCommitSecondary.Key, reply.Err, servers[si])
								}
							}
						}
					} else { // RPC failed
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: CommitSecondary Key '%s' RPC failed to srv %s."+colorReset, txn.clerk.clientId, txn.startTs, argsCommitSecondary.RequestId, key, servers[si])
						}
					}
				} // end server loop
			}
			// If this secondary commit still failing after trying all servers in group,
			// in a real system it's okay, the lock will eventually be cleaned up.
			txn.clerk.config = txn.clerk.sm.Query(-1)
			time.Sleep(100 * time.Millisecond)
		} // end commitSecondaryRetryLoop for one key
	} // end loop for secondary keys

	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Transaction Commit process finished. CommitTs: %d."+colorReset, txn.clerk.clientId, txn.startTs, txn.commitTs)
	}
	return nil
}

// Abort attempts to roll back the transaction (best-effort).
func (txn *Txn) Abort() error {
	if EnableClientLog {
		log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Aborting transaction. Writes: %v"+colorReset, txn.clerk.clientId, txn.startTs, txn.ops)
	}
	if len(txn.ops) == 0 {
		return nil // Nothing to abort
	}

	// Rollback all keys involved in writes
	for key := range txn.ops {
		args := RollbackArgs{
			Key:        key,
			TxnStartTs: txn.startTs,
			ClientId:   txn.clerk.clientId,
			RequestId:  txn.clerk.newRpcRequestID(),
		}
	rollbackRetryLoop:
		for {
			args.ConfigNum = txn.clerk.config.Num
			shard := key2shard(key)
			gid := txn.clerk.config.Shards[shard]
			var reply RollbackReply
			if servers, ok := txn.clerk.config.Groups[gid]; ok {
				for si := 0; si < len(servers); si++ {
					srv := txn.clerk.make_end(servers[si])
					if EnableClientLog {
						log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Sending Rollback for Key '%s' to srv %s (GID %d)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si], gid)
					}
					rpcOk := srv.Call("ShardKV.Rollback", &args, &reply)
					if rpcOk {
						if reply.Err == OK {
							if EnableClientLog {
								log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Rollback Key '%s' SUCCESS."+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key)
							}
							break rollbackRetryLoop // Success for this key
						} else { // reply.Err != OK
							if reply.Err == ErrWrongGroup || reply.Err == ErrWrongConfig {
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Rollback Key '%s' failed. Error %s"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, args.Key, reply.Err)
								}
								break // Break from server loop to refresh config
							} else {
								// Other errors, log and try next or refresh. Abort is best effort.
								if EnableClientLog {
									log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Rollback Key '%s' failed with %s from srv %s. (Best Effort)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, reply.Err, servers[si])
								}
							}
						}
					} else { // RPC failed
						if EnableClientLog {
							log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d) ReqID %d: Rollback Key '%s' RPC failed to srv %s. (Best Effort)"+colorReset, txn.clerk.clientId, txn.startTs, args.RequestId, key, servers[si])
						}
					}
				} // end server loop
			}
			// If this rollback still failing after trying all servers in group, move on.
			if reply.Err != OK && reply.Err != ErrWrongGroup {
				if EnableClientLog {
					log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Persistent error rolling back Key '%s'. Moving on (Best Effort)."+colorReset, txn.clerk.clientId, txn.startTs, key)
				}
				break rollbackRetryLoop
			}
			if reply.Err == OK {
				break rollbackRetryLoop
			}

			time.Sleep(100 * time.Millisecond)
			txn.clerk.config = txn.clerk.sm.Query(-1)
			if EnableClientLog {
				log.Printf(txn.clerk.GetClientColor()+"Client %d Txn (StartTs %d): Refreshed config to num %d for Rollback Key '%s'"+colorReset, txn.clerk.clientId, txn.startTs, txn.clerk.config.Num, key)
			}
		} // end rollbackRetryLoop for one key
	} // end loop for all keys in writeset
	return nil // Abort is best-effort
}

func (ck *Clerk) Get(key string) string {
	if EnableClientLog {
		log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Get Key '%s'"+colorReset, ck.clientId, key)
	}
	for {
		txn := ck.BeginTxn()
		value, err := txn.Get(key)
		if err == OK {
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Get Key '%s' (TxnStartTs %d) SUCCESS. Value: '%s'"+colorReset, ck.clientId, key, txn.startTs, value)
			}
			return value
		}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Get Key '%s' (TxnStartTs %d) failed with error '%v'. Retrying entire Get operation..."+colorReset, ck.clientId, key, txn.startTs, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	if EnableClientLog {
		log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' Value '%s'"+colorReset, ck.clientId, key, value)
	}
	for {
		// This Put will retry indefinitely.
		txn := ck.BeginTxn() // 1. 开始一个新的事务 (获取 start_ts)
		txn.Put(key, value)  // 2. 调用 Txn.Put，将这一个 key-value 写入当前事务的缓冲 writes 中
		err := txn.Commit()  // 3. **关键步骤**: 调用 Txn.Commit()。
		if err == nil {      // 如果 txn.Commit() 成功返回 OK
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' SUCCESS."+colorReset, ck.clientId, key)
			}
			return // 操作成功，退出函数
		}
		// 如果 txn.Commit() 失败 (例如，发生冲突导致 ErrAbort，或者网络/服务器错误)
		// 则会重试整个 Put 操作 (开始一个新的事务)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' failed with error '%v', retrying entire Put operation."+colorReset, ck.clientId, key, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (ck *Clerk) Append(key string, value string) {
	if EnableClientLog {
		log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' Value '%s'"+colorReset, ck.clientId, key, value)
	}
	for {
		// This Put will retry indefinitely.
		txn := ck.BeginTxn()   // 1. 开始一个新的事务 (获取 start_ts)
		txn.Append(key, value) // 2. 调用 Txn.Put，将这一个 key-value 写入当前事务的缓冲 writes 中
		err := txn.Commit()    // 3. **关键步骤**: 调用 Txn.Commit()。
		if err == nil {        // 如果 txn.Commit() 成功返回 OK
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' SUCCESS."+colorReset, ck.clientId, key)
			}
			return // 操作成功，退出函数
		}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn Put Key '%s' failed with error '%v', retrying entire Put operation."+colorReset, ck.clientId, key, err)
		}
		time.Sleep(200 * time.Millisecond)
	}
}

func (ck *Clerk) PutBatch(puts map[string]string) Err {
	if EnableClientLog {
		var putsDescriptions []string
		for k, v := range puts {
			putsDescriptions = append(putsDescriptions, fmt.Sprintf("'%s':'%s'", k, v))
		}
		// 使用 strings.Join 将所有描述连接成一个字符串，用逗号和空格分隔
		allPutsString := strings.Join(putsDescriptions, ", ")
		log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn PutBatch with %d operations: {%s}"+colorReset, ck.clientId, len(puts), allPutsString)
	}
	for { // 无限重试整个事务
		txn := ck.BeginTxn()
		for key, value := range puts {
			txn.Put(key, value) // 1. 将所有 Put 操作缓冲到事务中
		}
		// 2. 尝试提交整个事务
		commitErr := txn.Commit() // txn.Commit() 内部应包含其自身的2PC和重试逻辑
		if commitErr == nil {
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn PutBatch (TxnStartTs %d) SUCCESS."+colorReset, ck.clientId, txn.startTs)
			}
			return OK // 整个批量事务成功
		}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn PutBatch (TxnStartTs %d) failed commit with error '%v'. Retrying entire PutBatch operation..."+colorReset, ck.clientId, txn.startTs, commitErr)
		}
		time.Sleep(200 * time.Millisecond) // 重试前等待
	}
}

func (ck *Clerk) AppendBatch(puts map[string]string) Err {
	if EnableClientLog {
		var putsDescriptions []string
		for k, v := range puts {
			putsDescriptions = append(putsDescriptions, fmt.Sprintf("'%s':'%s'", k, v))
		}
		// 使用 strings.Join 将所有描述连接成一个字符串，用逗号和空格分隔
		allPutsString := strings.Join(putsDescriptions, ", ")
		log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn AppendBatch with %d operations: {%s}"+colorReset, ck.clientId, len(puts), allPutsString)
	}
	for { // 无限重试整个事务
		txn := ck.BeginTxn()
		for key, value := range puts {
			txn.Append(key, value) // 1. 将所有 Append 操作缓冲到事务中
		}
		// 2. 尝试提交整个事务
		commitErr := txn.Commit() // txn.Commit() 内部应包含其自身的2PC和重试逻辑
		if commitErr == nil {
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn AppendBatch (TxnStartTs %d) SUCCESS."+colorReset, ck.clientId, txn.startTs)
			}
			return OK // 整个批量事务成功
		}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"Client %d: Auto-Txn AppendBatch (TxnStartTs %d) failed commit with error '%v'. Retrying entire PutBatch operation..."+colorReset, ck.clientId, txn.startTs, commitErr)
		}
		time.Sleep(200 * time.Millisecond) // 重试前等待
	}
}

//////////////////////////////////////////////////////////////////////////////////////////////////////
// 功能函数

// GetClientColor
// 通过clientId来获取不同的颜色
func (ck *Clerk) GetClientColor() string {
	switch ck.clientId % 11 {
	case 0:
		return colorClient1
	case 1:
		return colorClient0
	case 2:
		return colorClient2
	case 3:
		return colorClient3
	case 4:
		return colorClient4
	case 5:
		return colorClient5
	case 6:
		return colorClient6
	case 7:
		return colorClient7
	case 8:
		return colorClient8
	case 9:
		return colorClient9
	case 10:
		return colorClient10
	default:
		return colorNewLog
	}
}

/////////////////////////////////////////////////////////////////////////////////////////////////////
// Original Get Put Append
//func (ck *Clerk) Get(key string) string {
//	ck.requestCounter++
//	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
//	args := GetArgs{
//		Key:       key,
//		ClientId:  ck.clientId,
//		RequestId: ck.requestId,
//	}
//
//	for {
//		shard := key2shard(key)
//		gid := ck.config.Shards[shard]
//		if servers, ok := ck.config.Groups[gid]; ok {
//			// try each server for the shard.
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply GetReply
//				if EnableClientLog {
//					log.Printf(ck.GetClientColor()+"client %d send Get request %d to server %s, key: %s"+colorReset, ck.clientId, ck.requestId, servers[si], key)
//				}
//				ok := srv.Call("ShardKV.Get", &args, &reply)
//				// ... not ok, or ErrWrongLeader
//				if ok && (reply.Err == OK || reply.Err == ErrNoKey) {
//					if EnableClientLog {
//						log.Printf(ck.GetClientColor()+"client %d finish request %d"+colorReset, ck.clientId, ck.requestId)
//					}
//					return reply.Value
//				}
//				if ok && reply.Err == ErrWrongGroup {
//					if EnableClientLog {
//						log.Printf(ck.GetClientColor()+"client %d request %d failed, Error Group %d"+colorReset, ck.clientId, ck.requestId, gid)
//					}
//					break
//				}
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask controler for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//	}
//
//	return ""
//}
// PutAppend
// shared by Put and Append.
// You will have to modify this function.
//func (ck *Clerk) PutAppend(key string, value string, op string) {
//	ck.requestCounter++
//	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
//	args := PutAppendArgs{
//		Key:       key,
//		Value:     value,
//		Op:        op,
//		ClientId:  ck.clientId,
//		RequestId: ck.requestId,
//	}
//
//	for {
//		shard := key2shard(key)
//		gid := ck.config.Shards[shard]
//		if servers, ok := ck.config.Groups[gid]; ok {
//			for si := 0; si < len(servers); si++ {
//				srv := ck.make_end(servers[si])
//				var reply PutAppendReply
//				if EnableClientLog {
//					log.Printf(ck.GetClientColor()+"client %d send PutAppend request %d to server %s, key: %s, value: %s"+colorReset, ck.clientId, ck.requestId, servers[si], key, value)
//				}
//				ok := srv.Call("ShardKV.PutAppend", &args, &reply)
//				if ok && reply.Err == OK {
//					if EnableClientLog {
//						log.Printf(ck.GetClientColor()+"client %d finish request %d"+colorReset, ck.clientId, ck.requestId)
//					}
//					return
//				}
//				if ok && reply.Err == ErrWrongGroup {
//					if EnableClientLog {
//						log.Printf(ck.GetClientColor()+"client %d request %d failed, Error Group %d"+colorReset, ck.clientId, ck.requestId, gid)
//					}
//					break
//				}
//				// ... not ok, or ErrWrongLeader
//			}
//		}
//		time.Sleep(100 * time.Millisecond)
//		// ask controler for the latest configuration.
//		ck.config = ck.sm.Query(-1)
//	}
//}
