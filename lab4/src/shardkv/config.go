package shardkv

import (
	"log"
	"os"
	"testing"

	"mit6.5840/labrpc"
	"mit6.5840/shardctrler"
	// import "log"
	crand "crypto/rand"
	"encoding/base64"
	"fmt"
	"math/big"
	"math/rand"
	"runtime"
	"strconv"
	"sync"
	"time"

	"mit6.5840/raft"
)

func randstring(n int) string {
	b := make([]byte, 2*n)
	crand.Read(b)
	s := base64.URLEncoding.EncodeToString(b)
	return s[0:n]
}

func makeSeed() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

// Randomize server handles
func random_handles(kvh []*labrpc.ClientEnd) []*labrpc.ClientEnd {
	sa := make([]*labrpc.ClientEnd, len(kvh))
	copy(sa, kvh)
	for i := range sa {
		j := rand.Intn(i + 1)
		sa[i], sa[j] = sa[j], sa[i]
	}
	return sa
}

// group
// gid: Group ID
// servers: The actual server instances
// saved: Persisters for each server's Raft state
// endnames: Communication endpoints between servers
// mendnames: Communication endpoints to the shard controller
type group struct {
	gid       int
	servers   []*ShardKV
	saved     []*raft.Persister
	endnames  [][]string
	mendnames [][]string
}

type config struct {
	mu    sync.Mutex
	t     *testing.T
	net   *labrpc.Network
	start time.Time // time at which make_config() was called

	nctrlers      int                        // number of shard controller servers
	ctrlerservers []*shardctrler.ShardCtrler // shard controller servers
	mck           *shardctrler.Clerk         // shard controller client

	ngroups   int      // number of groups
	npergroup int      // servers per k/v group
	groups    []*group // groups of servers

	clerks       map[*Clerk][]string
	nextClientId int
	maxraftstate int

	tsoserver *TSOServer
}

const GlobalTSOServerName = "TSOServerGlobal" // TSO服务的全局已知名称

func (cfg *config) checkTimeout() {
	// enforce a two minute real-time limit on each test
	if !cfg.t.Failed() && time.Since(cfg.start) > 120*time.Second {
		cfg.t.Fatal("test took longer than 120 seconds")
	}
}

func (cfg *config) cleanup() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		cfg.ShutdownGroup(gi)
	}
	for i := 0; i < cfg.nctrlers; i++ {
		cfg.ctrlerservers[i].Kill()
	}
	cfg.net.Cleanup()
	cfg.checkTimeout()
}

// check that no server's log is too big.
func (cfg *config) checklogs() {
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.npergroup; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			if cfg.maxraftstate >= 0 && raft > 8*cfg.maxraftstate {
				cfg.t.Fatalf("persister.RaftStateSize() %v, but maxraftstate %v",
					raft, cfg.maxraftstate)
			}
			if cfg.maxraftstate < 0 && snap > 0 {
				cfg.t.Fatalf("maxraftstate is -1, but snapshot is non-empty!")
			}
		}
	}
}

// controler server name for labrpc.
func (cfg *config) ctrlername(i int) string {
	return "ctrler" + strconv.Itoa(i)
}

// shard server name for labrpc.
// i'th server of group gid.
func (cfg *config) servername(gid int, i int) string {
	return "server-" + strconv.Itoa(gid) + "-" + strconv.Itoa(i)
}

func (cfg *config) makeClient() *Clerk {
	// 创建一个新客户端（Clerk）
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// ClientEnds to talk to controler service.
	// 设置与所有控制器服务器的连接
	ends := make([]*labrpc.ClientEnd, cfg.nctrlers)
	endnames := make([]string, cfg.npergroup)
	for j := 0; j < cfg.nctrlers; j++ {
		endnames[j] = randstring(20)
		ends[j] = cfg.net.MakeEnd(endnames[j])
		cfg.net.Connect(endnames[j], cfg.ctrlername(j))
		cfg.net.Enable(endnames[j], true)
	}

	//ck := MakeClerk(ends, func(servername string) *labrpc.ClientEnd {
	//	name := randstring(20)
	//	end := cfg.net.MakeEnd(name)
	//	cfg.net.Connect(name, servername)
	//	cfg.net.Enable(name, true)
	//	return end
	//})
	// 使用这些连接和一个用于创建到任意服务器的新连接的函数创建客户端
	ck := MakeClerk(ends, cfg.net, func(servername string) *labrpc.ClientEnd {
		name := randstring(20)
		end := cfg.net.MakeEnd(name)
		cfg.net.Connect(name, servername)
		cfg.net.Enable(name, true)
		return end
	})
	// 在配置中跟踪客户端并增加客户端ID计数器
	cfg.clerks[ck] = endnames
	cfg.nextClientId++
	return ck
}

func (cfg *config) deleteClient(ck *Clerk) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()
	// 通过删除与其端点关联的任何文件并从跟踪映射中移除它来清理客户端
	v := cfg.clerks[ck]
	for i := 0; i < len(v); i++ {
		os.Remove(v[i])
	}
	delete(cfg.clerks, ck)
}

// ShutdownServer
// Shutdown i'th server of gi'th group, by isolating it
func (cfg *config) ShutdownServer(gi int, i int) {
	cfg.mu.Lock()
	defer cfg.mu.Unlock()

	gg := cfg.groups[gi]

	// prevent this server from sending
	for j := 0; j < len(gg.servers); j++ {
		name := gg.endnames[i][j]
		cfg.net.Enable(name, false)
	}
	for j := 0; j < len(gg.mendnames[i]); j++ {
		name := gg.mendnames[i][j]
		cfg.net.Enable(name, false)
	}

	// disable client connections to the server.
	// it's important to do this before creating
	// the new Persister in saved[i], to avoid
	// the possibility of the server returning a
	// positive reply to an Append but persisting
	// the result in the superseded Persister.
	cfg.net.DeleteServer(cfg.servername(gg.gid, i))

	// a fresh persister, in case old instance
	// continues to update the Persister.
	// but copy old persister's content so that we always
	// pass Make() the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	}

	kv := gg.servers[i]
	if kv != nil {
		cfg.mu.Unlock()
		kv.Kill()
		cfg.mu.Lock()
		gg.servers[i] = nil
	}
}

func (cfg *config) ShutdownGroup(gi int) {
	for i := 0; i < cfg.npergroup; i++ {
		cfg.ShutdownServer(gi, i)
	}
}

// StartServer
// start i'th server in gi'th group
func (cfg *config) StartServer(gi int, i int) {
	cfg.mu.Lock()

	gg := cfg.groups[gi]

	// a fresh set of outgoing ClientEnd names
	// to talk to other servers in this group.
	gg.endnames[i] = make([]string, cfg.npergroup)
	for j := 0; j < cfg.npergroup; j++ {
		gg.endnames[i][j] = randstring(20)
	}

	// and the connections to other servers in this group.
	ends := make([]*labrpc.ClientEnd, cfg.npergroup)
	for j := 0; j < cfg.npergroup; j++ {
		ends[j] = cfg.net.MakeEnd(gg.endnames[i][j])
		cfg.net.Connect(gg.endnames[i][j], cfg.servername(gg.gid, j))
		cfg.net.Enable(gg.endnames[i][j], true)
	}

	// ends to talk to shardctrler service
	mends := make([]*labrpc.ClientEnd, cfg.nctrlers)
	gg.mendnames[i] = make([]string, cfg.nctrlers)
	for j := 0; j < cfg.nctrlers; j++ {
		gg.mendnames[i][j] = randstring(20)
		mends[j] = cfg.net.MakeEnd(gg.mendnames[i][j])
		cfg.net.Connect(gg.mendnames[i][j], cfg.ctrlername(j))
		cfg.net.Enable(gg.mendnames[i][j], true)
	}

	// a fresh persister, so old instance doesn't overwrite
	// new instance's persisted state.
	// give the fresh persister a copy of the old persister's
	// state, so that the spec is that we pass StartKVServer()
	// the last persisted state.
	if gg.saved[i] != nil {
		gg.saved[i] = gg.saved[i].Copy()
	} else {
		gg.saved[i] = raft.MakePersister()
	}
	cfg.mu.Unlock()

	gg.servers[i] = StartServer(ends, cfg.net, i, gg.saved[i], cfg.maxraftstate,
		gg.gid, mends,
		func(servername string) *labrpc.ClientEnd {
			name := randstring(20)
			end := cfg.net.MakeEnd(name)
			cfg.net.Connect(name, servername)
			cfg.net.Enable(name, true)
			return end
		})

	kvsvc := labrpc.MakeService(gg.servers[i])
	rfsvc := labrpc.MakeService(gg.servers[i].rf)
	srv := labrpc.MakeServer()
	srv.AddService(kvsvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.servername(gg.gid, i), srv)
}

func (cfg *config) StartGroup(gi int) {
	for i := 0; i < cfg.npergroup; i++ {
		cfg.StartServer(gi, i)
	}
}

func (cfg *config) StartCtrlerserver(i int) {
	// ClientEnds to talk to other controler replicas.
	ends := make([]*labrpc.ClientEnd, cfg.nctrlers)
	for j := 0; j < cfg.nctrlers; j++ {
		endname := randstring(20)
		ends[j] = cfg.net.MakeEnd(endname)
		cfg.net.Connect(endname, cfg.ctrlername(j))
		cfg.net.Enable(endname, true)
	}

	p := raft.MakePersister()

	cfg.ctrlerservers[i] = shardctrler.StartServer(ends, i, p)

	msvc := labrpc.MakeService(cfg.ctrlerservers[i])
	rfsvc := labrpc.MakeService(cfg.ctrlerservers[i].Raft())
	srv := labrpc.MakeServer()
	srv.AddService(msvc)
	srv.AddService(rfsvc)
	cfg.net.AddServer(cfg.ctrlername(i), srv)
}

// Creates a client for the shard controller by establishing connections to all controller servers.
func (cfg *config) shardclerk() *shardctrler.Clerk {
	// ClientEnds to talk to ctrler service.
	ends := make([]*labrpc.ClientEnd, cfg.nctrlers)
	for j := 0; j < cfg.nctrlers; j++ {
		name := randstring(20)
		ends[j] = cfg.net.MakeEnd(name)
		cfg.net.Connect(name, cfg.ctrlername(j))
		cfg.net.Enable(name, true)
	}

	return shardctrler.MakeClerk(ends)
}

// tell the shardctrler that a group is joining.
func (cfg *config) join(gi int) {
	cfg.joinm([]int{gi})
}

func (cfg *config) joinm(gis []int) {
	m := make(map[int][]string, len(gis))
	for _, g := range gis {
		gid := cfg.groups[g].gid
		servernames := make([]string, cfg.npergroup)
		for i := 0; i < cfg.npergroup; i++ {
			servernames[i] = cfg.servername(gid, i)
		}
		m[gid] = servernames
	}
	cfg.mck.Join(m)
}

// tell the shardctrler that a group is leaving.
func (cfg *config) leave(gi int) {
	cfg.leavem([]int{gi})
}

func (cfg *config) leavem(gis []int) {
	gids := make([]int, 0, len(gis))
	for _, g := range gis {
		gids = append(gids, cfg.groups[g].gid)
	}
	cfg.mck.Leave(gids)
}

var ncpu_once sync.Once

func make_config(t *testing.T, n int, unreliable bool, maxraftstate int) *config {
	log.Printf("TEST CLIENT initialize environment for make_config\n")
	ncpu_once.Do(func() {
		log.Printf("TEST CLIENT Checking CPU count\n")
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	log.Printf("TEST CLIENT Setting GOMAXPROCS to 4\n")
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// --- 启动并注册唯一的 TSO 服务 ---
	cfg.tsoserver = MakeTSO()                            // 创建TSO实例
	tsosvc := labrpc.MakeService(cfg.tsoserver)          // 将TSO实例包装成RPC服务
	tsorpcserver := labrpc.MakeServer()                  // 创建一个RPC服务器来承载TSO服务
	tsorpcserver.AddService(tsosvc)                      // 将TSO服务添加到RPC服务器
	cfg.net.AddServer(GlobalTSOServerName, tsorpcserver) // 将RPC服务器注册到网络，使用固定名称
	log.Printf("TEST FRAMEWORK: Global TSO Server '%s' started and registered.", GlobalTSOServerName)
	// --- TSO 服务启动完毕 ---

	// controler
	log.Printf("TEST CLIENT initialize %d shardctrler servers\n", 3)
	cfg.nctrlers = 3
	cfg.ctrlerservers = make([]*shardctrler.ShardCtrler, cfg.nctrlers)
	for i := 0; i < cfg.nctrlers; i++ {
		log.Printf("TEST CLIENT start shardctrler server %d\n", i)
		cfg.StartCtrlerserver(i)
	}
	log.Printf("TEST CLIENT create shardctrler client\n")
	cfg.mck = cfg.shardclerk()

	// 初始化复制组
	cfg.ngroups = 3
	log.Printf("TEST CLIENT initialize %d groups\n", cfg.ngroups)
	cfg.groups = make([]*group, cfg.ngroups)
	cfg.npergroup = n
	for gi := 0; gi < cfg.ngroups; gi++ {
		log.Printf("TEST CLIENT initialize group %d with gid=%d\n", gi, 100+gi)
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*ShardKV, cfg.npergroup)
		gg.saved = make([]*raft.Persister, cfg.npergroup)
		gg.endnames = make([][]string, cfg.npergroup)
		gg.mendnames = make([][]string, cfg.nctrlers)
		for i := 0; i < cfg.npergroup; i++ {
			log.Printf("TEST CLIENT start shardkv server %d in group %d (gid=%d)\n", i, gi, gg.gid)
			cfg.StartServer(gi, i)
		}
	}

	// 初始化客户端
	log.Printf("TEST CLIENT initialize client mapping\n")
	cfg.clerks = make(map[*Clerk][]string)
	log.Printf("TEST CLIENT set nextClientId to %d\n", cfg.npergroup+1000)
	cfg.nextClientId = cfg.npergroup + 1000 // client ids start 1000 above the highest serverid

	// 设置网络
	log.Printf("TEST CLIENT set network reliability to reliable=%v\n", !unreliable)
	cfg.net.Reliable(!unreliable)

	return cfg
}

func make_configmore(t *testing.T, ngroups int, npergroup int, unreliable bool, maxraftstate int) *config {
	log.Printf("TEST CLIENT initialize environment for make_config\n")
	ncpu_once.Do(func() {
		log.Printf("TEST CLIENT Checking CPU count\n")
		if runtime.NumCPU() < 2 {
			fmt.Printf("warning: only one CPU, which may conceal locking bugs\n")
		}
		rand.Seed(makeSeed())
	})
	log.Printf("TEST CLIENT Setting GOMAXPROCS to 4\n")
	runtime.GOMAXPROCS(4)
	cfg := &config{}
	cfg.t = t
	cfg.maxraftstate = maxraftstate
	cfg.net = labrpc.MakeNetwork()
	cfg.start = time.Now()

	// --- 启动并注册唯一的 TSO 服务 ---
	cfg.tsoserver = MakeTSO()                            // 创建TSO实例
	tsosvc := labrpc.MakeService(cfg.tsoserver)          // 将TSO实例包装成RPC服务
	tsorpcserver := labrpc.MakeServer()                  // 创建一个RPC服务器来承载TSO服务
	tsorpcserver.AddService(tsosvc)                      // 将TSO服务添加到RPC服务器
	cfg.net.AddServer(GlobalTSOServerName, tsorpcserver) // 将RPC服务器注册到网络，使用固定名称
	log.Printf("TEST FRAMEWORK: Global TSO Server '%s' started and registered.", GlobalTSOServerName)
	// --- TSO 服务启动完毕 ---

	// controler
	log.Printf("TEST CLIENT initialize %d shardctrler servers\n", 3)
	cfg.nctrlers = 3
	cfg.ctrlerservers = make([]*shardctrler.ShardCtrler, cfg.nctrlers)
	for i := 0; i < cfg.nctrlers; i++ {
		log.Printf("TEST CLIENT start shardctrler server %d\n", i)
		cfg.StartCtrlerserver(i)
	}
	log.Printf("TEST CLIENT create shardctrler client\n")
	cfg.mck = cfg.shardclerk()

	// 初始化复制组
	cfg.ngroups = ngroups
	log.Printf("TEST CLIENT initialize %d groups\n", cfg.ngroups)
	cfg.groups = make([]*group, cfg.ngroups)
	cfg.npergroup = npergroup
	for gi := 0; gi < cfg.ngroups; gi++ {
		log.Printf("TEST CLIENT initialize group %d with gid=%d\n", gi, 100+gi)
		gg := &group{}
		cfg.groups[gi] = gg
		gg.gid = 100 + gi
		gg.servers = make([]*ShardKV, cfg.npergroup)
		gg.saved = make([]*raft.Persister, cfg.npergroup)
		gg.endnames = make([][]string, cfg.npergroup)
		gg.mendnames = make([][]string, cfg.nctrlers)
		for i := 0; i < cfg.npergroup; i++ {
			log.Printf("TEST CLIENT start shardkv server %d in group %d (gid=%d)\n", i, gi, gg.gid)
			cfg.StartServer(gi, i)
		}
	}

	// 初始化客户端
	log.Printf("TEST CLIENT initialize client mapping\n")
	cfg.clerks = make(map[*Clerk][]string)
	log.Printf("TEST CLIENT set nextClientId to %d\n", cfg.npergroup+1000)
	cfg.nextClientId = cfg.npergroup + 1000 // client ids start 1000 above the highest serverid

	// 设置网络
	log.Printf("TEST CLIENT set network reliability to reliable=%v\n", !unreliable)
	cfg.net.Reliable(!unreliable)

	return cfg
}
