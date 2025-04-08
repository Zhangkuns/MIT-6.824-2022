package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"fmt"
	"log"
	"math"
	"math/rand"
	"mit6.5840/labgob"
	"sync"
	"sync/atomic"
	"time"
	//	"6.5840/labgob"
	"mit6.5840/labrpc"
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
	CommandTerm  int
	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type CommitInfo struct {
	oldIndex int
	newIndex int
}

// 定义节点状态常量
const (
	Follower = iota
	Candidate
	Leader
)

// 定义超时常量
const (
	HeartbeatTimeout   = 100 * time.Millisecond  // 心跳间隔100ms，确保每秒不超过10次
	ElectionTimeoutMin = 300 * time.Millisecond  // 最小选举超时
	ElectionTimeoutMax = 1000 * time.Millisecond // 最大选举超时
)

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

	// Server 5 的颜色 (橙色系)
	colorS5Follower = "\033[38;2;255;204;153m" // Light Orange (浅橙)
	colorS5Leader   = "\033[38;2;255;140;0m"   // Dark Orange (深橙)

	// Server 6 的颜色 (青绿色系)
	colorS6Follower = "\033[38;2;153;255;204m" // Light Teal (浅青绿)
	colorS6Leader   = "\033[38;2;0;128;128m"   // Teal (深青绿)

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

// EnableRaftLogging
// 添加日志开关字段
var EnableRaftLogging = false // 默认启用日志打印

// GetServerColor
// 根据服务器ID和状态获取颜色
func (rf *Raft) GetServerColor() string {
	switch rf.me {
	case 0:
		if rf.state == Leader {
			return colorS0Leader
		}
		return colorS0Follower
	case 1:
		if rf.state == Leader {
			return colorS1Leader
		}
		return colorS1Follower
	case 2:
		if rf.state == Leader {
			return colorS2Leader
		}
		return colorS2Follower
	case 3:
		if rf.state == Leader {
			return colorS3Leader
		}
		return colorS3Follower
	case 4:
		if rf.state == Leader {
			return colorS4Leader
		}
		return colorS4Follower
	case 5:
		if rf.state == Leader {
			return colorS5Leader
		}
		return colorS5Follower
	case 6:
		if rf.state == Leader {
			return colorS6Leader
		}
		return colorS6Follower
	default:
		return colorReset
	}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers: (Updated on stable storage before responding to RPCs)
	currentTerm int // 当前任期
	votedFor    int // 在当前任期投票给谁，-1表示还没投票
	state       int // 节点状态：Follower, Candidate, or Leader

	// 选举相关的时间
	electionTimer  *time.Timer // 选举超时计时器
	heartbeatTimer *time.Timer // 上次收到心跳的时间

	// 日志相关（虽然2A不需要处理日志，但是需要基本结构）
	log []LogEntry

	//interface{} from Raft to k/v server
	applyCh  chan ApplyMsg
	commitCh chan CommitInfo // 用于通知有新的日志需要提交
	// 所有服务器上的易失性状态
	// Volatile state on all servers:
	commitIndex int // 已知的最大已提交索引
	lastApplied int // 最后被应用到状态机的日志索引

	// leader上的易失性状态(选举后重新初始化)
	nextIndex  []int // 对于每一个服务器，需要发送给他的下一个日志条目的索引值
	matchIndex []int // 对于每一个服务器，已经复制给他的日志的最高索引值

	// 2D 快照相关字段
	lastIncludedIndex int    // 快照包含的最后日志索引
	lastIncludedTerm  int    // 该日志的任期号
	snapshot          []byte // 快照数据
}

// LogEntry 日志条目结构
type LogEntry struct {
	Term    int         // 条目被写入时的任期号
	Index   int         // 条目在日志中的索引
	Command interface{} // 实际命令
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == Leader
}

// 将绝对索引转换为相对索引
func (rf *Raft) getRelativeIndex(absoluteIndex int) int {
	relativeIndex := absoluteIndex - rf.lastIncludedIndex
	if relativeIndex < 0 {
		return -1
	}
	return relativeIndex
}

// 将相对索引转换为绝对索引
func (rf *Raft) getAbsoluteIndex(relativeIndex int) int {
	return relativeIndex + rf.lastIncludedIndex
}

// 日志访问相关函数
func (rf *Raft) getLastLogIndex() int {
	// 如果日志为空，返回快照的最后索引
	if len(rf.log) == 0 {
		return rf.lastIncludedIndex
	}
	// 否则返回最后一条日志的索引
	return len(rf.log) - 1 + rf.lastIncludedIndex
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getLog(index int) LogEntry {
	// 转换为相对索引
	return rf.log[index-rf.lastIncludedIndex]
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
	// 将 Raft 的关键状态保存到稳定存储中
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	// 需要持久化的状态（根据论文图2）:
	e.Encode(rf.currentTerm)       // 当前任期
	e.Encode(rf.votedFor)          // 投票给谁
	e.Encode(rf.log)               // 日志条目
	e.Encode(rf.lastIncludedIndex) // 添加快照的元数据
	e.Encode(rf.lastIncludedTerm)  // 添加快照的元数据
	raftstate := w.Bytes()
	rf.persister.Save(raftstate, rf.snapshot) // 保存到 Persister
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"S%d persisted state, RaftStateSize=%d"+colorReset, rf.me, rf.persister.RaftStateSize())
	}
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// 从持久化存储中恢复之前保存的状态
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var logentry []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"log before S%d restore persisted state"+colorReset, rf.me)
		rf.printLogState()
	}
	// 反序列化数据
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&logentry) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		// 处理错误
		DPrintf("{Node %v} restores persisted state failed", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = logentry
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
		// 恢复快照
		rf.snapshot = rf.persister.ReadSnapshot()

		// 设置初始的 commitIndex 和 lastApplied
		rf.commitIndex = rf.lastIncludedIndex
		rf.lastApplied = rf.lastIncludedIndex
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"log after S%d restore persisted state"+colorReset, rf.me)
			rf.printLogState()
		}

	}
}

// Your code here (2C).
// Example:
// r := bytes.NewBuffer(data)
// d := labgob.NewDecoder(r)
// var xxx
// var yyy
// if d.Decode(&xxx) != nil ||
//    d.Decode(&yyy) != nil {
//   error...
// } else {
//   rf.xxx = xxx
//   rf.yyy = yyy
// }

// RequestVoteArgs
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// 候选人的任期号
	Term int

	// 请求投票的候选人ID
	CandidateId int

	// 候选人的最后一条日志条目的索引值
	LastLogIndex int

	// 候选人最后一条日志条目的任期号
	LastLogTerm int
	// Your data here (2A, 2B).
}

// RequestVoteReply
// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// 当前任期号，用于候选人更新自己的任期号
	Term int

	// 当前服务器是否同意投票
	VoteGranted bool
	// Your data here (2A).
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(request *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 记录收到投票请求
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"S%d T%d asking for vote from %s"+colorReset, request.CandidateId, request.Term, rf.prefix())
	}
	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	// 1. 如果请求者任期小于当前任期，拒绝投票
	if request.Term < rf.currentTerm {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting vote for S%d, term too old (%d < %d)"+colorReset, rf.prefix(), request.CandidateId, request.Term, rf.currentTerm)
		}
		return
	}

	// 2. 如果请求者任期大于当前任期，转为follower
	if request.Term > rf.currentTerm {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"S%d Term is higher, %s become Follower, updating (%d > %d)"+colorReset, request.CandidateId, rf.prefix(), request.Term, rf.currentTerm)
		}
		rf.currentTerm = request.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist()
	}

	// 3. 如果已经投票给了别人，拒绝投票
	if request.Term == rf.currentTerm && rf.votedFor != -1 && rf.votedFor != request.CandidateId {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Already voted for S%d in T%d, reject S%d"+colorReset, rf.prefix(), rf.votedFor, rf.currentTerm, request.CandidateId)
		}
		return
	}

	// 4. 检查日志
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.IndextoTerm(lastLogIndex)

	// 如果自己的最后一条日志任期号更大，说明自己的日志更新，拒绝投票
	if lastLogTerm > request.LastLogTerm {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting vote for S%d, log term newer (%d > %d)"+colorReset, rf.prefix(), request.CandidateId, lastLogTerm, request.LastLogTerm)
		}
		return
	}
	// 如果任期号相同但自己的日志更长，说明自己的日志更新，拒绝投票
	if lastLogTerm == request.LastLogTerm && lastLogIndex > request.LastLogIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting vote for S%d, log longer at term %d"+colorReset, rf.prefix(), request.CandidateId, lastLogTerm)
		}
		return
	}

	// 满足所有条件，投出赞成票
	reply.VoteGranted = true
	rf.votedFor = request.CandidateId
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Granting Vote to S%d at T%d"+colorReset, rf.prefix(), request.CandidateId, request.Term)
	}
	// 重置选举定时器（因为发现了合法的候选人）
	rf.electionTimer.Reset(randomElectionTimeout())
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log.
// if this server isn't the leader, returns false.
// otherwise start the agreement and return immediately.
// there is no guarantee that this command will ever be committed to the Raft log,
// since the leader may fail or lose an election.
// even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	log.Printf(rf.GetServerColor()+"%s Start command %v"+colorReset, rf.prefix(), command)
	defer rf.mu.Unlock()

	// 1.if this server isn't the leader, returns false.
	if rf.state != Leader {
		return -1, -1, false
	}

	isLeader = true
	// 2. 创建并追加日志条目
	newIndex := rf.getLastLogIndex() + 1 // 使用绝对索引
	newEntry := LogEntry{
		Term:    rf.currentTerm,
		Index:   newIndex,
		Command: command,
	}
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"log before %s get new log entry %v at index %d, T:%d"+colorReset, rf.prefix(), newEntry.Command, newEntry.Index, newEntry.Term)
		rf.printLogState()
		log.Printf(colorNewLog+"%s get new log entry %v at index %d, T:%d"+colorReset, rf.prefix(), newEntry.Command, newEntry.Index, newEntry.Term)
	}
	rf.log = append(rf.log, newEntry)
	rf.persist()
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"log after %s get new log entry %v at index %d, T:%d"+colorReset, rf.prefix(), newEntry.Command, newEntry.Index, newEntry.Term)
		rf.printLogState()
	}
	rf.heartbeatTimer.Reset(1 * time.Millisecond)
	return newEntry.Index, newEntry.Term, isLeader
}

// Kill
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// 添加前缀格式化函数
func (rf *Raft) prefix() string {
	return fmt.Sprintf("S%d", rf.me)
}

func (rf *Raft) ticker() {
	for rf.killed() == false {
		select {
		case <-rf.electionTimer.C:
			rf.mu.Lock()
			if rf.state != Leader {
				// 只有非Leader才会进行选举
				if EnableRaftLogging {
					log.Printf(rf.GetServerColor()+"%s Converting to Candidate, calling election T:%d"+colorReset, rf.prefix(), rf.currentTerm+1)
				}
				rf.startElection()
			}
			// 重置选举定时器
			rf.electionTimer.Reset(randomElectionTimeout())
			rf.mu.Unlock()

		case <-rf.heartbeatTimer.C:
			rf.mu.Lock()
			if rf.state == Leader {
				if EnableRaftLogging {
					log.Printf(rf.GetServerColor()+"%s heartbeatTimer alarm T:%d"+colorReset, rf.prefix(), rf.currentTerm)
				}
				rf.broadcastHeartbeat()
				// 重置心跳定时器
				rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
			}
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) IndextoTerm(LogIndex int) int {
	if LogIndex < rf.lastIncludedIndex {
		return 0
	}
	if LogIndex == rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}
	relativeIndex := LogIndex - rf.lastIncludedIndex
	if relativeIndex >= len(rf.log) {

		return -1
	}
	return rf.log[relativeIndex].Term
}

func (rf *Raft) startElection() {
	// 创建一个结构体来存储选举信息
	type ElectionInfo struct {
		peer         int
		term         int
		candidateId  int
		lastLogIndex int
		lastLogTerm  int
		currentColor string
	}

	// 更新状态
	rf.state = Candidate
	rf.currentTerm++
	rf.votedFor = rf.me

	// 收集选举信息
	currentColor := rf.GetServerColor()
	lastLogIndex := rf.getLastLogIndex()
	lastLogTerm := rf.IndextoTerm(lastLogIndex)
	currentTerm := rf.currentTerm

	// 准备发送给所有其他节点的选举信息
	electionRequests := make([]ElectionInfo, 0)
	for i := range rf.peers {
		if i != rf.me {
			info := ElectionInfo{
				peer:         i,
				term:         currentTerm,
				candidateId:  rf.me,
				lastLogIndex: lastLogIndex,
				lastLogTerm:  lastLogTerm,
				currentColor: currentColor,
			}
			electionRequests = append(electionRequests, info)
		}
	}

	// 记录收到的投票数（包括自己的一票）
	votesReceived := 1
	rf.persist()
	if EnableRaftLogging {
		log.Printf(currentColor+"%s Starting election T:%d"+colorReset, rf.prefix(), currentTerm)
	}

	// 并行发送请求投票
	for _, info := range electionRequests {
		go func(info ElectionInfo) {
			args := &RequestVoteArgs{
				Term:         info.term,
				CandidateId:  info.candidateId,
				LastLogIndex: info.lastLogIndex,
				LastLogTerm:  info.lastLogTerm,
			}
			reply := &RequestVoteReply{}

			if EnableRaftLogging {
				log.Printf(info.currentColor+"%s -> S%d, %s Requesting vote for T:%d from S%d"+colorReset, rf.prefix(), info.peer, rf.prefix(), info.term, info.peer)
			}
			if rf.sendRequestVote(info.peer, args, reply) {
				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 检查term和状态
				if rf.state != Candidate {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s <- S%d, %s is no Candidate "+colorReset, rf.prefix(), info.peer, rf.prefix())
					}
					return
				}

				if rf.currentTerm != info.term {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s <- S%d, %s term %d changes to %d "+colorReset, rf.prefix(), info.peer, rf.prefix(), info.term, rf.currentTerm)
					}
					return
				}

				if reply.Term > rf.currentTerm {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s <- S%d , S%d Term %d is higher %d, %s becoming follower"+colorReset, rf.prefix(), info.peer, info.peer, reply.Term, rf.currentTerm, rf.prefix())
					}
					rf.becomeFollower(reply.Term)
					return
				}

				if reply.VoteGranted {
					votesReceived++
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s <- S%d, %s Got vote, total votes:%d"+colorReset,
							rf.prefix(), info.peer, rf.prefix(), votesReceived)
					}
					if votesReceived > len(rf.peers)/2 && rf.state == Candidate {
						if EnableRaftLogging {
							log.Printf(info.currentColor+"%s Achieved Majority for T%d (%d), converting to Leader"+colorReset,
								rf.prefix(), rf.currentTerm, votesReceived)
						}
						rf.becomeLeader()
					}
				}
			}
		}(info)
	}
}

func (rf *Raft) becomeFollower(term int) {
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Converting to Follower, T:%d"+colorReset, rf.prefix(), term)
	}
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.persist()
	// 转为follower时重置选举定时器
	rf.electionTimer.Reset(randomElectionTimeout())
}

func (rf *Raft) becomeLeader() {
	rf.state = Leader
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Converting to Leader, T:%d"+colorReset, rf.prefix(), rf.currentTerm)
	}
	// 初始化leader专用的状态
	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
	// 立即发送一次心跳
	rf.broadcastHeartbeat()
	// 重置心跳定时器
	rf.heartbeatTimer.Reset(StableHeartbeatTimeout())
}

func (rf *Raft) broadcastHeartbeat() {
	// 创建一个结构体来存储每个 follower 的心跳信息
	type HeartbeatInfo struct {
		peer              int
		term              int
		leaderId          int
		nextIndex         int
		prevLogIndex      int
		prevLogTerm       int
		entries           []LogEntry
		leaderCommit      int
		currentColor      string
		snapshot          bool
		LastIncludedIndex int
		LastIncludedTerm  int
		SnapData          []byte
	}
	// 收集所有 follower 的心跳信息
	heartbeats := make([]HeartbeatInfo, 0)
	currentColor := rf.GetServerColor() // 获取当前颜色
	if EnableRaftLogging {
		log.Printf(currentColor+"%s -> ALL Sending heartbeat T:%d"+colorReset, rf.prefix(), rf.currentTerm)
	}
	// 向所有follower发送心跳
	for i := range rf.peers {
		if i != rf.me {
			nextIndex := rf.nextIndex[i]
			entries := rf.getLogEntriesFrom(nextIndex)
			// 深拷贝日志条目
			entriesCopy := make([]LogEntry, len(entries))
			copy(entriesCopy, entries)
			info := HeartbeatInfo{
				peer:              i,
				term:              rf.currentTerm,
				leaderId:          rf.me,
				nextIndex:         nextIndex,
				prevLogIndex:      nextIndex - 1,
				prevLogTerm:       rf.IndextoTerm(nextIndex - 1),
				entries:           entriesCopy,
				leaderCommit:      rf.commitIndex,
				currentColor:      currentColor,
				LastIncludedIndex: rf.lastIncludedIndex,
				LastIncludedTerm:  rf.lastIncludedTerm,
				SnapData:          rf.snapshot,
			}
			if nextIndex <= rf.lastIncludedIndex {
				info.snapshot = true
				if EnableRaftLogging {
					log.Printf(currentColor+"%s Sending snapshot heartbeat to S%d, nextIndex:%d, LastIncludedIndex:%d"+colorReset, rf.prefix(), i, nextIndex, rf.lastIncludedIndex)
				}
			} else {
				info.snapshot = false
				if EnableRaftLogging {
					log.Printf(currentColor+"%s Sending noraml heartbeat to S%d, nextIndex:%d, LastIncludedIndex:%d"+colorReset, rf.prefix(), i, nextIndex, rf.lastIncludedIndex)
				}
			}
			heartbeats = append(heartbeats, info)
		}
	}
	// 对每个 follower 并行发送心跳
	for _, info := range heartbeats {
		go func(info HeartbeatInfo) {
			// 如果是发送快照
			if info.snapshot {
				args := &InstallSnapshotArgs{
					Term:              info.term,              // 领导者的任期号
					LeaderId:          info.leaderId,          // 领导者的ID
					LastIncludedIndex: info.LastIncludedIndex, // 最后日志条目的索引
					LastIncludedTerm:  info.LastIncludedTerm,  // 最后日志条目的任期
					Data:              info.SnapData,          // 快照数据
				}
				reply := &InstallSnapshotReply{}

				ok := rf.sendInstallSnapshot(info.peer, args, reply, currentColor)
				if !ok {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s -> S%d InstallSnapshot RPC failed, T%d, LastIncludedIndex %d"+colorReset, rf.prefix(), info.peer, info.term, info.LastIncludedIndex)
					}
					return // RPC 失败直接返回，不要继续处理
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 检查 term 是否已经改变
				if rf.currentTerm != info.term {
					return // term 已改变，放弃这次更新
				}

				if reply.Success {
					rf.nextIndex[info.peer] = info.LastIncludedIndex + 1
					rf.matchIndex[info.peer] = info.LastIncludedIndex
					rf.updateCommitIndex()
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s InstallSnapshot success, S%d, T%d, nextIndex:%d, matchIndex:%d"+colorReset, rf.prefix(), info.peer, info.term, rf.nextIndex[info.peer], rf.matchIndex[info.peer])
					}
				} else {
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s InstallSnapshot failed, S%d, T%d, LastIncludedIndex%d"+colorReset, rf.prefix(), info.peer, info.term, info.LastIncludedIndex)
					}
				}
			} else {
				args := &AppendEntriesArgs{
					Term:         info.term,
					LeaderId:     info.leaderId,
					PrevLogIndex: info.prevLogIndex,
					PrevLogTerm:  info.prevLogTerm,
					Entries:      info.entries,
					LeaderCommit: info.leaderCommit,
				}
				reply := &AppendEntriesReply{}

				ok := rf.sendAppendEntries(info.peer, args, reply, currentColor)
				if !ok {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s -> S%d AppendEntries RPC failed, T%d, PrevLogIndex %d"+colorReset, rf.prefix(), info.peer, info.term, info.prevLogIndex)
					}
					return // RPC 失败直接返回，不要继续处理
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()

				// 检查 term 是否已经改变
				if rf.currentTerm != info.term {
					return // term 已改变，放弃这次更新
				}

				if reply.Success == true {
					rf.nextIndex[info.peer] = args.PrevLogIndex + len(args.Entries) + 1
					rf.matchIndex[info.peer] = rf.nextIndex[info.peer] - 1
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s AppendEntries success, S%d, T%d, nextIndex:%d, RejectHint:%d"+colorReset, rf.prefix(), info.peer, info.term, rf.nextIndex[info.peer], reply.RejectHint)
					}
					rf.updateCommitIndex()
					return
				} else if reply.Success == false {
					if EnableRaftLogging {
						log.Printf(info.currentColor+"%s AppendEntries failed, S%d, T%d, nextIndex:%d, RejectHint:%d"+colorReset, rf.prefix(), info.peer, info.term, rf.nextIndex[info.peer], reply.RejectHint)
					}
					// 如果 term 更大，转为 follower
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.state = Follower
						rf.votedFor = -1
						rf.persist()
						return
					}
					// 尝试回退
					// etcd 风格的快速回退
					// rf.nextIndex[info.peer] = int(math.Max(1, float64(rf.nextIndex[info.peer]-1)))
					if reply.RejectHint < rf.nextIndex[info.peer]-1 {
						// 如果 RejectHint 建议的位置比当前 nextIndex 小，说明需要快速回退
						if reply.RejectHint < rf.lastIncludedIndex {
							// 说明需要进行snapshot安装
							if EnableRaftLogging {
								log.Printf(info.currentColor+"%s backup for S%d from index %d to %d, need InstallSnapshot"+colorReset, rf.prefix(), info.peer, rf.nextIndex[info.peer], reply.RejectHint+1)
							}
							rf.nextIndex[info.peer] = reply.RejectHint + 1
							return
						} else {
							choosen := int(math.Max(float64(rf.lastIncludedIndex+1), float64(reply.RejectHint+1)))
							if EnableRaftLogging {
								log.Printf(info.currentColor+"%s Fast backup for S%d from index %d to %d"+colorReset, rf.prefix(), info.peer, rf.nextIndex[info.peer], choosen)
							}
							rf.nextIndex[info.peer] = choosen
							return
						}
					} else {
						// 否则只回退一个位置
						rf.nextIndex[info.peer] = int(math.Max(float64(rf.lastIncludedIndex+1), float64(rf.nextIndex[info.peer]-1)))
						if EnableRaftLogging {
							log.Printf(info.currentColor+"%s Slow backup for S%d to index %d"+colorReset, rf.prefix(), info.peer, rf.nextIndex[info.peer])
						}
						return
					}
				}
			}
		}(info)
	}
}

// 获取从指定索引开始的日志条目副本
func (rf *Raft) getLogEntriesFrom(nextIndex int) []LogEntry {
	// 如果请求的索引在快照内或无效，返回空切片
	if nextIndex <= rf.lastIncludedIndex {
		return []LogEntry{}
	}

	// 将绝对索引转换为相对索引
	relativeIndex := nextIndex - rf.lastIncludedIndex

	// 检查相对索引是否有效
	if relativeIndex < 0 || relativeIndex >= len(rf.log) {
		return []LogEntry{}
	}

	// 创建新切片并复制日志条目
	entries := make([]LogEntry, len(rf.log[relativeIndex:]))
	copy(entries, rf.log[relativeIndex:])
	return entries

}

// 更新commitIndex的辅助函数
func (rf *Raft) updateCommitIndex() {
	// 从后往前查找最大的可提交索引
	oldCommitIndex := rf.commitIndex
	lastLogIndex := rf.getLastLogIndex()
	for N := int(math.Max(float64(rf.commitIndex+1), float64(rf.lastIncludedIndex+1))); N <= lastLogIndex+1; N++ {
		// 只能提交当前任期的日志
		if rf.IndextoTerm(N) != rf.currentTerm {
			if EnableRaftLogging {
				log.Printf(rf.GetServerColor()+"%s Skipping updateCommitIndex for T%d, found T%d at index %d"+colorReset, rf.prefix(), rf.currentTerm, rf.IndextoTerm(N), N)
			}
			continue
		}

		count := 1 // 计数器从1开始（包括自己）
		// 该节点的 matchIndex 大于等于 N
		// 说明该节点已经成功复制了索引 N 的日志
		for peer := range rf.peers {
			if peer != rf.me && rf.matchIndex[peer] >= N {
				count++
			}
		}

		// 如果多数派都复制了该日志，则可以提交
		if count > len(rf.peers)/2 {
			rf.commitIndex = N
		}
	}
	// 如果 commitIndex 有更新，通知应用层
	if rf.commitIndex > oldCommitIndex {
		commitInfo := CommitInfo{
			oldIndex: oldCommitIndex,
			newIndex: rf.commitIndex,
		}
		// 发送 commitInfo 通知应用层
		select {
		case <-rf.commitCh:
			// 清空旧的通知
			log.Printf(rf.GetServerColor()+"%s updateCommitIndex, commitCh is full, dropping old commit info"+colorReset, rf.prefix())
		default:
		}
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Updated commitIndex from %d to %d"+colorReset, rf.prefix(), oldCommitIndex, rf.commitIndex)
		}
		rf.commitCh <- commitInfo
	}
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int        // 新日志条目紧邻的前一个日志条目的索引值
	PrevLogTerm  int        // prevLogIndex条目的任期号
	Entries      []LogEntry // 准备存储的日志条目（心跳时为空）
	LeaderCommit int        // leader的commitIndex
}

type AppendEntriesReply struct {
	Term    int  // 当前任期号，用于leader更新自己
	Success bool // 如果follower包含匹配prevLogIndex和prevLogTerm的日志则为真
	// 优化项：用于快速回退
	RejectHint int // follower 的最后一条日志的索引
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Success = false
	reply.Term = rf.currentTerm
	reply.RejectHint = args.PrevLogIndex
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s <- S%d Received AppendEntries T:%d, PLI:%d"+colorReset, rf.prefix(), args.LeaderId, args.Term, args.PrevLogIndex)
	}
	// 1. Reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Success = false
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting AppendEntries from S%d, term too old (%d < %d)"+colorReset, rf.prefix(), args.LeaderId, args.Term, rf.currentTerm)
		}
		return
	}

	// 任期合法，重置选举超时
	rf.electionTimer.Reset(randomElectionTimeout())

	// 先检查是否需要快照
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = false
		reply.RejectHint = args.PrevLogIndex
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting AppendEntries from S%d, need install snapshot: PrevLogIndex:%d <= lastIncludedIndex:%d"+colorReset, rf.prefix(), args.LeaderId, args.PrevLogIndex, rf.lastIncludedIndex)
		}
		return
	}

	//2. Reply false if log doesn’t contain an entry at prevLogIndex whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex >= rf.getLastLogIndex()+1 {
		reply.Success = false
		reply.RejectHint = rf.getLastLogIndex() // 返回最后一条日志的索引
		//reply.ConflictIndex = len(rf.log)
		//reply.ConflictTerm = -1
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting AppendEntries from S%d, Log too short: lastIndex:%d <= PLI:%d"+colorReset, rf.prefix(), args.LeaderId, rf.getLastLogIndex(), args.PrevLogIndex)
		}
		return
	}

	prevLogTerm := rf.IndextoTerm(args.PrevLogIndex)
	if args.PrevLogIndex > rf.lastIncludedIndex && prevLogTerm != args.PrevLogTerm {
		reply.Success = false
		ConflictTerm := prevLogTerm

		// 使用二分查找找到冲突任期的第一个索引
		left := rf.lastIncludedIndex + 1
		right := args.PrevLogIndex
		ConflictIndex := args.PrevLogIndex

		for left <= right {
			mid := left + (right-left)/2
			midTerm := rf.IndextoTerm(mid)
			if midTerm == ConflictTerm {
				// 如果前一个任期不同，说明找到了第一个位置
				if mid == rf.lastIncludedIndex+1 || rf.IndextoTerm(mid-1) != ConflictTerm {
					ConflictIndex = mid
					break
				}
				// 否则继续在左半部分查找
				right = mid - 1
			} else {
				// 在右半部分查找
				left = mid + 1
			}
		}

		reply.RejectHint = ConflictIndex - 1
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Log mismatch: T%d != PLT:%d at index %d"+colorReset, rf.prefix(), rf.IndextoTerm(args.PrevLogIndex), args.PrevLogTerm, args.PrevLogIndex)
		}
		return
	}

	// 3. If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)
	ifappend := false
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"log before S%d make %s appending new entries"+colorReset, args.LeaderId, rf.prefix())
		rf.printLogState()
	}
	if len(args.Entries) > 0 {
		newEntries := make([]LogEntry, len(args.Entries))
		copy(newEntries, args.Entries)

		// 从 PrevLogIndex + 1 开始比对
		for i, entry := range newEntries {
			index := args.PrevLogIndex + 1 + i

			if index < rf.getLastLogIndex()+1 {
				// 如果在相同位置上任期不同，说明有冲突
				if rf.IndextoTerm(index) != entry.Term {
					// 删除这一位置及之后的所有日志
					rf.log = rf.log[:rf.getRelativeIndex(index)]
					// 追加新的日志
					rf.log = append(rf.log, newEntries[i:]...)
					rf.persist() // 在这里添加持久化，因为修改了日志
					if EnableRaftLogging {
						log.Printf(rf.GetServerColor()+"S%d Found conflict with %s at index %d , truncating log and appending new entries %v from index %d"+colorReset, args.LeaderId, rf.prefix(), index, newEntries[i:], index)
						log.Printf(rf.GetServerColor()+"log after S%d Found conflict with %s at index %d , truncating log and appending new entries"+colorReset, args.LeaderId, rf.prefix(), index)
						rf.printLogState()
					}
					break
				}
			} else {
				// 如果已经超出现有日志长度，直接追加剩余的新日志
				// 4. Append any new entries not already in the log
				rf.log = append(rf.log, newEntries[i:]...)
				rf.persist() // 在这里添加持久化，因为修改了日志
				reply.RejectHint = rf.getLastLogIndex()
				reply.Success = true
				ifappend = true
				if EnableRaftLogging {
					log.Printf(rf.GetServerColor()+"S%d make %s appending new entries %v from index %d"+colorReset, args.LeaderId, rf.prefix(), newEntries[i:], index)
					log.Printf(rf.GetServerColor()+"log after S%d make %s appending new entries %v"+colorReset, args.LeaderId, rf.prefix(), newEntries[i:])
					rf.printLogState()
				}
				break
			}
		}
	}

	// 5. 更新 commitIndex
	// 5. If leaderCommit > commitIndex, set commitIndex = min(leaderCommit, index of last new entry)
	ifcommit := false
	if args.LeaderCommit > rf.commitIndex {
		ifcommit = true
		rf.commitIndex = int(math.Min(float64(args.LeaderCommit), float64(rf.getLastLogIndex())))
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"S%d updated %s commitIndex to %d"+colorReset, args.LeaderId, rf.prefix(), rf.commitIndex)
		}
		commitInfo := CommitInfo{
			oldIndex: rf.commitIndex,
			newIndex: args.LeaderCommit,
		}
		// 发送 commitInfo 通知应用层
		// 修改了的部分
		select {
		case <-rf.commitCh:
			// 清空旧的通知
			log.Printf(rf.GetServerColor()+"%s AppendEntries, commitCh is full, dropping old commit info"+colorReset, rf.prefix())
		default:
		}
		rf.commitCh <- commitInfo
	}

	// 如果收到的任期更大，转为follower
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.state = Follower
		rf.votedFor = -1
		rf.persist() // 在这里添加持久化，因为修改了状态
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"S%d Term is higher than %s, updating (%d > %d)"+colorReset, args.LeaderId, rf.prefix(), args.Term, rf.currentTerm)
		}
	}

	reply.Success = true
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"S%d make %s appending entries: %t, commit entries: %t"+colorReset, args.LeaderId, rf.prefix(), ifappend, ifcommit)
	}
	return
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply, currentColor string) bool {
	if EnableRaftLogging {
		log.Printf(currentColor+"%s -> S%d Sending AppendEntries T:%d PLI:%d"+colorReset, rf.prefix(), server, args.Term, args.PrevLogIndex)
	}
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	if !ok {
		if EnableRaftLogging {
			log.Printf(currentColor+"%s -> S%d sendAppendEntries failed: Success=%v Term=%d RejectHint=%d"+colorReset, rf.prefix(), server, reply.Success, reply.Term, reply.RejectHint)
		}
		return false
	} else {
		if EnableRaftLogging {
			log.Printf(currentColor+"%s sendAppendEntries response from S%d: Success=%v Term=%d RejectHint=%d"+colorReset, rf.prefix(), server, reply.Success, reply.Term, reply.RejectHint)
		}
		return true
	}
}

// Snapshot
// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
// The index argument indicates the highest log entry that's reflected in the snapshot.
// Raft should discard its log entries before that point.
// You'll need to revise your Raft code to operate while storing only the tail of the log.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 如果已经有更新的快照，直接返回
	if index <= rf.lastIncludedIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"S%d skip snapshot, current lastIncludedIndex %d >= index %d"+colorReset, rf.me, rf.lastIncludedIndex, index)
		}
		return
	}
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"log before %s Snapshot"+colorReset, rf.prefix())
		rf.printLogState()
	}
	oldLastIncludedIndex := rf.lastIncludedIndex
	oldLen := len(rf.log)
	// log.Printf(rf.getServerColor()+"S%d T%d creating snapshot at absolute index %d (relative index %d)"+colorReset, rf.me, rf.currentTerm, index, rf.getRelativeIndex(index))
	// 保存快照和元数据
	startIndex := index - oldLastIncludedIndex
	rf.snapshot = snapshot
	rf.lastIncludedIndex = index
	rf.lastIncludedTerm = rf.log[startIndex].Term

	// 压缩日志：删除快照覆盖的部分
	// 压缩日志：保留index 0并且只保留快照之后的日志
	newlog := rf.log[(startIndex + 1):]
	rf.log = make([]LogEntry, 0)
	rf.log = []LogEntry{{Term: 0}}
	rf.log = append(rf.log, newlog...)
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"S%d T%d created snapshot, log size %d->%d, LastIncludeIndex %d->%d"+colorReset, rf.me, rf.currentTerm, oldLen, len(rf.log), oldLastIncludedIndex, rf.lastIncludedIndex)
		log.Printf(rf.GetServerColor()+"log after %s Snapshot, Truncating log from size %d to %d"+colorReset, rf.prefix(), oldLen, len(rf.log))
		rf.printLogState()
	}
}

type InstallSnapshotArgs struct {
	Term              int    // 领导者的任期号
	LeaderId          int    // 领导者的ID
	LastIncludedIndex int    // 最后日志条目的索引
	LastIncludedTerm  int    // 最后日志条目的任期
	Data              []byte // 快照数据
}

type InstallSnapshotReply struct {
	Term    int // 当前任期号，用于leader更新自己
	Success bool
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply, currentColor string) bool {
	if EnableRaftLogging {
		log.Printf(currentColor+"%s -> S%d Sending InstallSnapshot LastIncludeIndex %d, LastIncludeTerm %d"+colorReset, rf.prefix(), server, args.LastIncludedIndex, args.LastIncludedTerm)
	}
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	if !ok {
		if EnableRaftLogging {
			log.Printf(currentColor+"%s -> S%d sendInstallSnapshot failed: Success=%v Term=%d "+colorReset, rf.prefix(), server, reply.Success, reply.Term)
		}
		return false
	} else {
		if EnableRaftLogging {
			log.Printf(currentColor+"%s sendInstallSnapshot response from S%d: Success=%v Term=%d "+colorReset, rf.prefix(), server, reply.Success, reply.Term)
		}
		return true
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm

	// 1. 检查任期
	if args.Term < rf.currentTerm {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Rejecting InstallSnapshot from S%d, term too old (%d < %d)"+colorReset, rf.prefix(), args.LeaderId, args.Term, rf.currentTerm)
		}
		reply.Success = false
		return
	}

	// 重置选举定时器
	rf.electionTimer.Reset(randomElectionTimeout())

	// 如果收到更高任期
	// 1. Reply immediately if term < currentTerm
	if args.Term > rf.currentTerm {
		oldTerm := rf.currentTerm
		rf.becomeFollower(args.Term)
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s InstallSnapshot from S%d converting to follower, term %d -> %d"+colorReset, rf.prefix(), args.LeaderId, oldTerm, args.Term)
		}
	}

	// 4. Reply and wait for more data chunks if done is false
	//if !args.Done {
	//	log.Printf(rf.getServerColor()+"%s Received snapshot chunk from S%d, offset %d, size %d, Waiting... "+colorReset, rf.prefix(), args.LeaderId, args.Offset, len(args.Data))
	//	reply.Success = false
	//	return
	//}

	// 5. Save snapshot file, discard any existing or partial snapshot with smaller index
	// 如果快照索引小于等于当前快照索引,忽略
	if args.LastIncludedIndex < rf.lastIncludedIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Ignoring InstallSnapshot from S%d, lastIncludeIndex %d >= %d"+colorReset, rf.prefix(), args.LeaderId, rf.lastIncludedIndex, args.LastIncludedIndex)
		}
		reply.Success = false
		return
	}

	if args.LastIncludedIndex == rf.lastIncludedIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Ignoring InstallSnapshot from S%d, lastIncludeIndex %d == %d"+colorReset, rf.prefix(), args.LeaderId, rf.lastIncludedIndex, args.LastIncludedIndex)
		}
		reply.Success = true
		return
	}

	// 应用快照到状态机
	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Sending ApplyMsg snapshot at index %d, term %d"+colorReset, rf.prefix(), args.LastIncludedIndex, args.LastIncludedTerm)
	}
	select {
	case rf.applyCh <- msg:
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s Successfully sent snapshot to applyCh at index %d, term %d"+colorReset, rf.prefix(), args.LastIncludedIndex, args.LastIncludedTerm)
		}
	default:
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"%s applyCh is full, rejecting snapshot at index %d, term %d"+colorReset, rf.prefix(), args.LastIncludedIndex, args.LastIncludedTerm)
		}
		reply.Success = false
		return
	}

	// 只有在 applyCh 发送成功后，才更新 Raft 状态
	// 2. Create new snapshot file if first chunk (offset is 0)
	rf.snapshot = make([]byte, 0)
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Creating new snapshot file from S%d"+colorReset, rf.prefix(), args.LeaderId)
	}

	// 3. Write data into snapshot file at given offset
	rf.snapshot = append(rf.snapshot, args.Data...)

	// 更新快照
	relativeindex := rf.getRelativeIndex(args.LastIncludedIndex)
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	// 重置状态
	if rf.lastApplied < args.LastIncludedIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"InstallSnapshot, %s Resetting lastApplied from %d to %d"+colorReset, rf.prefix(), rf.lastApplied, args.LastIncludedIndex)
		}
		rf.lastApplied = args.LastIncludedIndex
	}
	if rf.commitIndex < args.LastIncludedIndex {
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"InstallSnapshot, %s Resetting commitIndex from %d to %d"+colorReset, rf.prefix(), rf.commitIndex, args.LastIncludedIndex)
		}
		rf.commitIndex = args.LastIncludedIndex
	}

	// 检查日志是否有相同的条目
	// 需要修改逻辑
	oldLen := len(rf.log)
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"InstallSnapshot, %s log oldLen %d, lastIncludedIndex %d is relativeindex %d, "+colorReset, rf.prefix(), oldLen, args.LastIncludedIndex, relativeindex)
		log.Printf(rf.GetServerColor()+"log before %s InstallSnapshot, LastApplied:%d, CommitIndex:%d"+colorReset, rf.prefix(), rf.lastApplied, rf.commitIndex)
		rf.printLogState()
	}
	if len(rf.log) > 0 && relativeindex > 0 && relativeindex < len(rf.log) {
		// 6. If existing log entry has same index and term as snapshot’s last included entry,
		// retain log entries following it and reply
		if rf.IndextoTerm(relativeindex+rf.lastIncludedIndex) == args.LastIncludedTerm {
			// 保留后续日志
			zeroLog := []LogEntry{{Term: 0}}
			newLog := append(zeroLog, rf.log[(relativeindex+1):]...)
			rf.log = newLog
			if EnableRaftLogging {
				log.Printf(rf.GetServerColor()+"InstallSnapshot, %s Truncating log from size %d to %d, keeping entries after index %d"+colorReset, rf.prefix(), oldLen, len(rf.log), args.LastIncludedIndex)
				log.Printf(rf.GetServerColor()+"log after %s InstallSnapshot, Truncating log from size %d to %d"+colorReset, rf.prefix(), oldLen, len(rf.log))
				rf.printLogState()
			}
		} else {
			// 7. Discard the entire log
			// 丢弃全部日志
			rf.log = nil
			rf.log = make([]LogEntry, 0)
			rf.log = []LogEntry{{Term: 0}}
			if EnableRaftLogging {
				log.Printf(rf.GetServerColor()+"InstallSnapshot, %s Found mismatched term at index %d (log term %d != snapshot term %d), discarding all logs (size %d -> 0)"+colorReset, rf.prefix(), args.LastIncludedIndex, rf.IndextoTerm(relativeindex), args.LastIncludedTerm, oldLen)
				log.Printf(rf.GetServerColor()+"log after %s InstallSnapshot, Found mismatched term at index %d (log term %d != snapshot term %d)"+colorReset, rf.prefix(), args.LastIncludedIndex, rf.IndextoTerm(relativeindex), args.LastIncludedTerm)
				rf.printLogState()
			}
		}
	} else {
		// 丢弃全部日志
		rf.log = nil
		rf.log = make([]LogEntry, 0)
		rf.log = []LogEntry{{Term: 0}}
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"InstallSnapshot, %s Cannot find matching index %d in log (size %d), discarding all logs"+colorReset, rf.prefix(), args.LastIncludedIndex, oldLen)
			log.Printf(rf.GetServerColor()+"log after %s InstallSnapshot, Cannot find matching index %d in log"+colorReset, rf.prefix(), args.LastIncludedIndex)
			rf.printLogState()
		}
	}

	rf.persist()
	reply.Success = true
	return
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0

	// Your initialization code here (2A, 2B, 2C).
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.state = Follower

	// 初始化快照相关字段
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = 0
	rf.snapshot = nil

	// 初始化选举计时器
	rf.electionTimer = time.NewTimer(randomElectionTimeout())
	rf.heartbeatTimer = time.NewTimer(StableHeartbeatTimeout())

	rf.log = make([]LogEntry, 0)
	rf.log = []LogEntry{{Term: 0}}
	rf.applyCh = applyCh
	rf.commitCh = make(chan CommitInfo, 1) // 使用带缓冲的通道
	rf.lastApplied = 0
	rf.commitIndex = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	// 启动应用例程
	go rf.applyLogs()

	return rf
}

// 应用日志到状态机的例程
func (rf *Raft) applyLogs() {
	for !rf.killed() {
		// 检查是否有新的已提交日志需要应用
		<-rf.commitCh
		rf.mu.Lock()
		if EnableRaftLogging {
			log.Printf(rf.GetServerColor()+"log before %s Applying logs, LastApplied:%d, CommitIndex:%d"+colorReset, rf.prefix(), rf.lastApplied, rf.commitIndex)
			rf.printLogState()
		}
		if rf.lastApplied < rf.commitIndex {
			if EnableRaftLogging {
				log.Printf(rf.GetServerColor()+"%s LastApplied:%d, CommitIndex:%d"+colorReset, rf.prefix(), rf.lastApplied, rf.commitIndex)
			}
			entries := make([]LogEntry, 0)
			for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
				if i > rf.lastIncludedIndex {
					if EnableRaftLogging {
						log.Printf(rf.GetServerColor()+"%s Applying log at relative index %d"+colorReset, rf.prefix(), rf.getRelativeIndex(i))
					}
					entries = append(entries, rf.log[rf.getRelativeIndex(i)])
				}
			}
			startIndex := rf.lastApplied + 1
			rf.lastApplied = rf.commitIndex // 更新 lastApplied
			colorApplylog := rf.GetServerColor()
			rf.mu.Unlock() // 尽早释放锁

			// 在释放锁后应用日志到状态机
			for i, entry := range entries {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      entry.Command,
					CommandIndex: startIndex + i,
					CommandTerm:  entry.Term,
				}
				if EnableRaftLogging {
					log.Printf(colorApplylog+"%s Applied log %d at index %d"+colorReset, rf.prefix(), entry, startIndex+i)
				}
				rf.applyCh <- applyMsg
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

// 生成随机选举超时时间
func randomElectionTimeout() time.Duration {
	// 生成随机超时时间
	return ElectionTimeoutMin + time.Duration(rand.Int63())%(ElectionTimeoutMax-ElectionTimeoutMin)
}

func StableHeartbeatTimeout() time.Duration {
	return HeartbeatTimeout
}

// printLogState 打印 Raft 节点的日志状态
func (rf *Raft) printLogState() {
	if EnableRaftLogging {
		log.Printf(rf.GetServerColor()+"%s Log State: LastApplied:%d, CommitIndex:%d, LastIncludedIndex:%d, LogLength:%d, Log:%v"+colorReset,
			rf.prefix(), rf.lastApplied, rf.commitIndex, rf.lastIncludedIndex, len(rf.log), rf.log)
	}
}
