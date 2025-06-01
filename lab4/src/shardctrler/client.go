package shardctrler

//
// Shardctrler clerk.
//

import (
	"crypto/rand"
	"log"
	"math/big"
	"sync"
	"time"

	"mit6.5840/labrpc"
)

// 客户端颜色常量
const (
	// 客户端颜色 (基于 kvraft 的风格，但使用更柔和的色调)
	colorCtrlClient1  = "\033[38;2;240;128;128m" // Light Coral (浅珊瑚)
	colorCtrlClient2  = "\033[38;2;152;251;152m" // Pale Green (浅绿)
	colorCtrlClient3  = "\033[38;2;175;238;238m" // Pale Turquoise (浅青)
	colorCtrlClient4  = "\033[38;2;255;245;238m" // Seashell (贝壳白)
	colorCtrlClient5  = "\033[38;2;221;160;221m" // Plum (梅紫)
	colorCtrlClient6  = "\033[38;2;255;228;196m" // Bisque (米色)
	colorCtrlClient7  = "\033[38;2;245;245;220m" // Beige (米黄)
	colorCtrlClient8  = "\033[38;2;176;196;222m" // Light Steel Blue (浅钢蓝)
	colorCtrlClient9  = "\033[38;2;255;182;193m" // Light Pink (浅粉)
	colorCtrlClient10 = "\033[38;2;144;238;144m" // Light Green (浅绿)
	colorCtrlClient0  = "\033[38;2;173;216;230m" // Light Cyan (浅青)
)

// EnableClientLog
// 假设日志开关（与 kvraft 一致）
var EnableClientLog = false

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId       int64 // 客户端 ID
	requestCounter int64 // 请求计数器
	requestId      int64 // 请求 ID
	lastLeader     int   // 最后已知的领导者 ID
	mu             sync.Mutex
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

// GetClientColor 返回客户端的日志颜色
func (ck *Clerk) GetClientColor() string {
	switch ck.clientId % 11 {
	case 0:
		return colorCtrlClient0 // Light Coral (浅珊瑚)
	case 1:
		return colorCtrlClient1 // Pale Green (浅绿)
	case 2:
		return colorCtrlClient2 // Pale Turquoise (浅青)
	case 3:
		return colorCtrlClient3 // Seashell (贝壳白)
	case 4:
		return colorCtrlClient4 // Plum (梅紫)
	case 5:
		return colorCtrlClient5 // Bisque (米色)
	case 6:
		return colorCtrlClient6 // Beige (米黄)
	case 7:
		return colorCtrlClient7 // Light Steel Blue (浅钢蓝)
	case 8:
		return colorCtrlClient8 // Light Pink (浅粉)
	case 9:
		return colorCtrlClient9 // Light Green (浅绿)
	case 10:
		return colorCtrlClient10 // Light Cyan (浅青)
	default:
		return colorNewLog // Bright Cyan (亮青)
	}
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.clientId = nrand()
	ck.requestCounter = 0
	ck.requestId = 0
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := QueryArgs{
		Num:       num,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.lastLeader
	ck.mu.Unlock()

	for {
		reply := QueryReply{}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d send Query request %d to server %d, configNum: %d"+colorReset, ck.clientId, ck.requestId, server, num)
		}
		ok := ck.servers[server].Call("ShardCtrler.Query", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastLeader = server
			ck.mu.Unlock()
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"CtrlClient %d set lastLeader to %d, finish request %d"+colorReset, ck.clientId, ck.lastLeader, ck.requestId)
			}
			return reply.Config
		}
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := JoinArgs{
		Servers:   servers,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.lastLeader
	ck.mu.Unlock()

	for {
		reply := JoinReply{}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d send Join request %d to server %d, servers: %v"+colorReset, ck.clientId, ck.requestId, server, servers)
		}
		ok := ck.servers[server].Call("ShardCtrler.Join", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastLeader = server
			ck.mu.Unlock()
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"CtrlClient %d set lastLeader to %d, finish request %d"+colorReset, ck.clientId, ck.lastLeader, ck.requestId)
			}
			return
		}
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := LeaveArgs{
		GIDs:      gids,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.lastLeader
	ck.mu.Unlock()

	for {
		reply := LeaveReply{}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d send Leave request %d to server %d, gids: %v"+colorReset, ck.clientId, ck.requestId, server, gids)
		}
		ok := ck.servers[server].Call("ShardCtrler.Leave", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastLeader = server
			ck.mu.Unlock()
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"CtrlClient %d set lastLeader to %d, finish request %d"+colorReset, ck.clientId, ck.lastLeader, ck.requestId)
			}
			return
		}
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := MoveArgs{
		Shard:     shard,
		GID:       gid,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.lastLeader
	ck.mu.Unlock()

	for {
		reply := MoveReply{}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d send Move request %d to server %d, shard: %d, gid: %d"+colorReset, ck.clientId, ck.requestId, server, shard, gid)
		}
		ok := ck.servers[server].Call("ShardCtrler.Move", &args, &reply)
		if ok && reply.Err == OK {
			ck.mu.Lock()
			ck.lastLeader = server
			ck.mu.Unlock()
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"CtrlClient %d set lastLeader to %d, finish request %d"+colorReset, ck.clientId, ck.lastLeader, ck.requestId)
			}
			return
		}
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"CtrlClient %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		time.Sleep(100 * time.Millisecond)
	}
}
