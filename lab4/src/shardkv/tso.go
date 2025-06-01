package shardkv

import (
	"log"
	"mit6.5840/labrpc"
	"sync"
)

// 全局 TSO 单例
var (
	tsoServer *TSOServer
	tsoEnd    *labrpc.ClientEnd
	tsoOnce   sync.Once
)

// TSOServer 提供全局时间戳服务
type TSOServer struct {
	mu        sync.Mutex
	timestamp uint64
}

type GetTimestampArgs struct {
	ClientId int64
}

type GetTimestampReply struct {
	Timestamp uint64
}

func (tso *TSOServer) GetTimestamp(args *GetTimestampArgs, reply *GetTimestampReply) {
	tso.mu.Lock()
	defer tso.mu.Unlock()
	tso.timestamp++
	reply.Timestamp = tso.timestamp
	log.Printf("\033[38;2;255;255;0mTSOServer: Issued Timestamp %d to Client %d\033[0m", reply.Timestamp, args.ClientId)
}

// MakeTSOClientEnd
// 这个函数现在可以放在 shardkv 包内，或者 client 包内，或者一个公共包
// 它不再 *启动* TSO 服务，只是创建连接
func MakeTSOClientEnd(network *labrpc.Network, tsoServerName string) *labrpc.ClientEnd {
	// 这里的 tsoServerName 就是 "TSOServerGlobal"
	name := randstring(20) // 客户端末端的唯一名称
	end := network.MakeEnd(name)
	network.Connect(name, tsoServerName)
	network.Enable(name, true)
	return end
}

// MakeTSO 启动并返回一个新的TSOServer实例 (这个函数不直接注册到网络)
func MakeTSO() *TSOServer {
	ts := &TSOServer{}
	// 初始化时间戳，可以基于当前时间，或者从0开始然后快速增加
	ts.timestamp = 0
	// 或者 ts.timestamp = 0
	// 然后在GetTimestamp中确保 tso.timestamp++ 总是发生
	return ts
}
