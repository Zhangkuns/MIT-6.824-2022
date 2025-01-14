package raft

//
// support for Raft and kvraft to save persistent
// Raft state (log &c) and k/v server snapshots.
//
// we will use the original persister.go to test your code for grading.
// so, while you can modify this code to help you debug, please
// test with the original before submitting.
//

import "sync"

type Persister struct {
	mu        sync.Mutex
	raftstate []byte // 存储 Raft 的持久化状态
	snapshot  []byte // 存储 K/V 服务器的快照
}

func MakePersister() *Persister {
	return &Persister{}
}

func clone(orig []byte) []byte { // 字节数组克隆
	x := make([]byte, len(orig))
	copy(x, orig)
	return x
}

func (ps *Persister) Copy() *Persister { // 创建 Persister 副本
	ps.mu.Lock()
	defer ps.mu.Unlock()
	np := MakePersister()
	np.raftstate = ps.raftstate
	np.snapshot = ps.snapshot
	return np
}

func (ps *Persister) ReadRaftState() []byte { // 读取 Raft 状态
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.raftstate)
}

func (ps *Persister) RaftStateSize() int { // 获取 Raft 状态大小：
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.raftstate)
}

// Save both Raft state and K/V snapshot as a single atomic action,
// to help avoid them getting out of sync.
func (ps *Persister) Save(raftstate []byte, snapshot []byte) { // 保存 Raft 状态和 K/V 快照
	ps.mu.Lock()
	defer ps.mu.Unlock()
	ps.raftstate = clone(raftstate)
	ps.snapshot = clone(snapshot)
}

func (ps *Persister) ReadSnapshot() []byte { // 读取快照
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return clone(ps.snapshot)
}

func (ps *Persister) SnapshotSize() int { // 获取快照大小
	ps.mu.Lock()
	defer ps.mu.Unlock()
	return len(ps.snapshot)
}
