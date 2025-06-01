package kvraft

import (
	"crypto/rand"
	"log"
	"math/big"
	"mit6.5840/labrpc"
	"time"
)

const (
	// colorReset = "\033[0m"
	colorClient0 = "\033[38;2;255;255;255m"

	// Client 10: 深橄榄绿 (Dark Olive Green)
	colorClient10 = "\033[38;2;85;107;47m"

	// Client 1: （中等紫，Medium Purple）
	colorClient1 = "\033[38;2;147;112;219m"

	// Client 2: 珊瑚红 (Coral)
	colorClient2 = "\033[38;2;255;127;127m"

	// Client 3: 深天蓝 (Deep Sky Blue)
	colorClient3 = "\033[38;2;0;191;255m"

	// Client 4: 巧克力色 (Chocolate)
	colorClient4 = "\033[38;2;210;105;30m"

	// Client 5: 亮草绿 (Lawn Green)
	colorClient5 = "\033[38;2;124;252;0m"

	// Client 6: 深海蓝 (Deep Sea Blue)
	colorClient6 = "\033[38;2;0;105;148m"

	// Client 7: 玫瑰红 (Rose Red)
	colorClient7 = "\033[38;2;188;0;77m"

	// Client 8: 亮橙色 (Bright Orange)
	colorClient8 = "\033[38;2;255;165;0m"

	// Client 9: 深灰蓝 (Slate Gray)
	colorClient9 = "\033[38;2;112;128;144m"
)

var EnableClientLog = false // 默认Client禁用日志打印

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId       int64 // client id
	requestCounter int64 // request counter
	requestId      int64 // request id
	LastLeader     int   // last leader Id
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientId = nrand() // random client id generate
	ck.requestCounter = 0 // request Counter generate
	ck.requestId = 0      // request id generate
	ck.LastLeader = 0     // last leader id
	return ck
}

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

// Get
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.

func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := GetArgs{
		Key:       key,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.LastLeader
	for {
		reply := GetReply{}
		// send the request to the server
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"client %d send Get request %d to server %d, key: %s"+colorReset, ck.clientId, ck.requestId, server, key)
		}
		ok := ck.servers[server].Call("KVServer.Get", &args, &reply)
		if ok && reply.Err == OK {
			// find the right leader
			ck.LastLeader = server
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"client %d set LastLeader to %d，finish request %d"+colorReset, ck.clientId, ck.LastLeader, ck.requestId)
			}
			return reply.Value
		}
		// if the server is not the leader, try the next server
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"client %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		// 加一点延迟避免过度加载系统
		time.Sleep(100 * time.Millisecond)
	}
}

// PutAppend
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.requestCounter++
	ck.requestId = (ck.clientId&0xFFFF)*1000000 + ck.requestCounter
	args := PutAppendArgs{
		Key:       key,
		Value:     value,
		Op:        op,
		ClientId:  ck.clientId,
		RequestId: ck.requestId,
	}
	server := ck.LastLeader
	for {
		reply := PutAppendReply{}
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"client %d send PutAppend request %d to server %d, key: %s, value: %s"+colorReset, ck.clientId, ck.requestId, server, key, value)
		}
		ok := ck.servers[server].Call("KVServer.PutAppend", &args, &reply)
		if ok && reply.Err == OK {
			// find the right leader
			ck.LastLeader = server
			if EnableClientLog {
				log.Printf(ck.GetClientColor()+"client %d set LastLeader to %d，finish request %d"+colorReset, ck.clientId, ck.LastLeader, ck.requestId)
			}
			return
		}
		// if the server is not the leader, try the next server
		oldserver := server
		server = (server + 1) % len(ck.servers)
		if EnableClientLog {
			log.Printf(ck.GetClientColor()+"client %d request %d failed, server %d is not the leader, try next server %d"+colorReset, ck.clientId, ck.requestId, oldserver, server)
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
