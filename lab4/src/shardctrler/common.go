package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// NShards
// The number of shards.
const NShards = 10

// Config
// A configuration -- an assignment of shards to groups.
// Please don't change this.
// 分片（Shards）
// 分布式键/值存储系统中数据（键/值对）的逻辑分区。整个数据集被分成若干子集，每个子集称为一个分片。
// 例如，Shards[0] = 1 表示分片 0 由 GID 为 1 的副本组负责。
// GID（副本组标识符）
// 副本组（replica group）的唯一标识符，用一个整数表示（例如，1、2、3）。
// 副本组是一组服务器（通常 3-5 台），共同管理一个或多个分片，使用 Raft 协议确保数据一致性和容错。
// 每个副本组负责系统中部分分片的数据操作（Put、Append、Get）。
// 例如，Groups[1] = []string{"server1", "server2", "server3"} 表示 GID 1 的副本组包含三台服务器。
// 服务器（Servers）
// 副本组中运行的实际节点（物理或虚拟机），每个服务器是一个 Raft 节点，参与 Raft 协议以确保一致性。
// 在 Groups map 中，服务器以字符串形式表示（例如，"server1"），通常是服务器的地址或标识符。
// 每个副本组包含多个服务器（例如，3 台），共同管理该组负责的分片。
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

// 错误常量
const (
	OK                 = "OK"
	ErrWrongLeader     = "ErrWrongLeader"
	ErrClientOrRequest = "ErrClientOrRequest"
)

type Err string

type JoinArgs struct {
	Servers   map[int][]string // new GID -> servers mappings
	ClientId  int64            // 客户端 ID
	RequestId int64            // 请求序列号
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs      []int // 要移除的 GID 列表
	ClientId  int64 // 客户端 ID
	RequestId int64 // 请求序列号
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard     int   // 分片编号
	GID       int   // 目标 GID
	ClientId  int64 // 客户端 ID
	RequestId int64 // 请求序列号
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num       int   // desired config number
	ClientId  int64 // 客户端 ID
	RequestId int64 // 请求序列号
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
