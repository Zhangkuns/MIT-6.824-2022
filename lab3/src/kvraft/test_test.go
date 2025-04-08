package kvraft

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mit6.5840/models"
	"mit6.5840/porcupine"
)

// The tester generously allows solutions to complete elections in one second
// (much more than the paper's range of timeouts).
const electionTimeout = 1 * time.Second

const linearizabilityCheckTimeout = 1 * time.Second

// 定义了一个线程安全的结构体 OpLog，用于存储操作序列（例如 Get、Put、Append），
// 以便后续使用 Porcupine 线性一致性检查工具进行验证。
type OpLog struct {
	operations []porcupine.Operation
	sync.Mutex
}

// 为 OpLog 添加一个操作记录。
func (log *OpLog) Append(op porcupine.Operation) {
	log.Lock()
	defer log.Unlock()
	log.operations = append(log.operations, op)
}

// 读取 OpLog 中的所有操作并返回副本
func (log *OpLog) Read() []porcupine.Operation {
	log.Lock()
	defer log.Unlock()
	ops := make([]porcupine.Operation, len(log.operations))
	copy(ops, log.operations)
	return ops
}

// to make sure timestamps use the monotonic clock, instead of computing
// absolute timestamps with `time.Now().UnixNano()` (which uses the wall
// clock), we measure time relative to `t0` using `time.Since(t0)`, which uses
// the monotonic clock
var t0 = time.Now()

// get/put/putappend that keep counts
// 执行 Get 操作，从键/值服务中获取指定键的值，并记录操作日志
func Get(cfg *config, ck *Clerk, key string, opLog *OpLog, cli int) string {
	start := int64(time.Since(t0))
	v := ck.Get(key)
	end := int64(time.Since(t0))
	// 增加操作计数器（用于统计）。
	cfg.op()
	// log.Printf("TEST CLIENT=%d: Get operation, key=%s, value=%s\n", cli, key, v)
	// log.Printf("client=%d: Get operation, key=%s", cli, key)
	// 记录操作到日志
	if opLog != nil {
		opLog.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 0, Key: key},
			Output:   models.KvOutput{Value: v},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}

	return v
}

// 执行 Put 操作，将键/值对写入服务，并记录日志
func Put(cfg *config, ck *Clerk, key string, value string, opLog *OpLog, cli int) {
	start := int64(time.Since(t0))
	ck.Put(key, value)
	end := int64(time.Since(t0))
	cfg.op()
	// log.Printf("TEST CLIENT=%d: Put operation, key=%s, value=%s\n", cli, key, value)
	if opLog != nil {
		opLog.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 1, Key: key, Value: value},
			Output:   models.KvOutput{},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
}

// 执行 Append 操作，将值追加到指定键的现有值后，并记录日志。
func Append(cfg *config, ck *Clerk, key string, value string, opLog *OpLog, cli int) {
	start := int64(time.Since(t0))
	ck.Append(key, value)
	end := int64(time.Since(t0))
	cfg.op()
	// log.Printf("TEST CLIENT=%d: Append operation, key=%s, value=%s\n", cli, key, value)
	if opLog != nil {
		opLog.Append(porcupine.Operation{
			Input:    models.KvInput{Op: 2, Key: key, Value: value},
			Output:   models.KvOutput{},
			Call:     start,
			Return:   end,
			ClientId: cli,
		})
	}
}

// 验证 Get 操作返回的值是否符合预期。
func check(cfg *config, t *testing.T, ck *Clerk, key string, value string) {
	v := Get(cfg, ck, key, nil, -1)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// a client runs the function f and then signals it is done
// 运行单个客户端任务并报告完成状态。
func run_client(t *testing.T, cfg *config, me int, ca chan bool, fn func(me int, ck *Clerk, t *testing.T)) {
	// me int：客户端 ID。
	// ca chan bool：通道，用于通知任务完成。
	// fn：客户端执行的函数。
	ok := false
	defer func() { ca <- ok }()
	ck := cfg.makeClient(cfg.All())
	fn(me, ck, t)
	ok = true
	cfg.deleteClient(ck)
}

// spawn ncli clients and wait until they are all done
// 启动多个客户端并等待所有客户端完成。
// 为每个客户端创建通道并启动协程。
// 等待所有通道返回结果，若有失败则终止测试。
func spawn_clients_and_wait(t *testing.T, cfg *config, ncli int, fn func(me int, ck *Clerk, t *testing.T)) {
	ca := make([]chan bool, ncli)
	for cli := 0; cli < ncli; cli++ {
		ca[cli] = make(chan bool)
		go run_client(t, cfg, cli, ca[cli], fn)
	}
	log.Printf("TEST CLIENT spawn_clients_and_wait: waiting for clients")
	for cli := 0; cli < ncli; cli++ {
		ok := <-ca[cli]
		log.Printf("TEST CLIENT %d spawn_clients_and_wait: is done\n", cli)
		if ok == false {
			t.Fatalf("TEST CLIENT %d failure", cli)
		}
	}
}

// predict effect of Append(k, val) if old value is prev.
func NextValue(prev string, val string) string {
	return prev + val
}

// check that for a specific client all known appends are present in a value,
// and in order
// 验证特定客户端的所有 Append 操作是否按顺序正确反映在键值对的值 v 中。
func checkClntAppends(t *testing.T, clnt int, v string, count int) {
	lastoff := -1
	for j := 0; j < count; j++ {
		// 构造预期追加的字符串，格式为 "x <客户端ID> <序号> y"。
		wanted := "x " + strconv.Itoa(clnt) + " " + strconv.Itoa(j) + " y"
		// 查找 wanted 在 v 中的首次出现位置。
		off := strings.Index(v, wanted)
		if off < 0 {
			t.Fatalf("TEST CLIENT %v: missing element %v in Append result %v", clnt, wanted, v)
		}
		// 查找 wanted 的最后出现位置
		off1 := strings.LastIndex(v, wanted)
		if off1 != off {
			t.Fatalf("TEST CLIENT %v: duplicate element %v in Append result", clnt, wanted)
		}
		if off <= lastoff {
			t.Fatalf("TEST CLIENT %v: wrong order for element %v in Append result", clnt, wanted)
		}
		lastoff = off
	}
}

// check that all known appends are present in a value,
// and are in order for each concurrent client.
// 验证多个并发客户端的所有 Append 操作是否按顺序正确反映在键值对的值 v 中。
func checkConcurrentAppends(t *testing.T, v string, counts []int) {
	nclients := len(counts)
	for i := 0; i < nclients; i++ {
		lastoff := -1
		for j := 0; j < counts[i]; j++ {
			wanted := "x " + strconv.Itoa(i) + " " + strconv.Itoa(j) + " y"
			off := strings.Index(v, wanted)
			if off < 0 {
				t.Fatalf("Client %v: missing element %v in Append result %v", i, wanted, v)
			}
			off1 := strings.LastIndex(v, wanted)
			if off1 != off {
				t.Fatalf("Client %v: duplicate element %v in Append result", i, wanted)
			}
			if off <= lastoff {
				t.Fatalf("Client %v: wrong order for element %v in Append result", i, wanted)
			}
			lastoff = off
		}
	}
}

// repartition the servers periodically
// 周期性地重新分区服务器，模拟网络分区以测试 Raft 的容错能力。
func partitioner(t *testing.T, cfg *config, ch chan bool, done *int32) {
	defer func() { ch <- true }()
	for atomic.LoadInt32(done) == 0 {
		a := make([]int, cfg.n)
		for i := 0; i < cfg.n; i++ {
			a[i] = (rand.Int() % 2)
		}
		pa := make([][]int, 2)
		for i := 0; i < 2; i++ {
			pa[i] = make([]int, 0)
			for j := 0; j < cfg.n; j++ {
				if a[j] == i {
					pa[i] = append(pa[i], j)
				}
			}
		}
		cfg.partition(pa[0], pa[1])
		log.Printf("partitioner: partitioning to region%v and region%v\n", pa[0], pa[1])
		time.Sleep(electionTimeout + time.Duration(rand.Int63()%200)*time.Millisecond)
	}
}

// Basic test is as follows: one or more clients submitting Append/Get
// operations to set of servers for some period of time.  After the period is
// over, test checks that all appended values are present and in order for a
// particular key.  If unreliable is set, RPCs may fail.  If crash is set, the
// servers crash after the period is over and restart.  If partitions is set,
// the test repartitions the network concurrently with the clients and servers. If
// maxraftstate is a positive number, the size of the state for Raft (i.e., log
// size) shouldn't exceed 8*maxraftstate. If maxraftstate is negative,
// snapshots shouldn't be used.
func GenericTest(t *testing.T, part string, nclients int, nservers int, unreliable bool, crash bool, partitions bool, maxraftstate int, randomkeys bool) {

	title := "Test: "
	if unreliable {
		// the network drops RPC requests and replies.
		title = title + "unreliable net, "
	}
	if crash {
		// peers re-start, and thus persistence must work.
		title = title + "restarts, "
	}
	if partitions {
		// the network may partition
		title = title + "partitions, "
	}
	if maxraftstate != -1 {
		title = title + "snapshots, "
	}
	if randomkeys {
		title = title + "random keys, "
	}
	if nclients > 1 {
		title = title + "many clients"
	} else {
		title = title + "one client"
	}
	title = title + " (" + part + ")" // 3A or 3B

	// 设置测试环境并初始化必要资源。
	cfg := make_config(t, nservers, unreliable, maxraftstate)
	defer cfg.cleanup()
	cfg.begin(title)
	opLog := &OpLog{}
	// 创建客户端
	ck := cfg.makeClient(cfg.All())

	// 创建操作日志和全局客户端。
	done_partitioner := int32(0)
	done_clients := int32(0)
	// 初始化控制测试流程的变量和通信通道。
	ch_partitioner := make(chan bool)
	clnts := make([]chan int, nclients)
	for i := 0; i < nclients; i++ {
		clnts[i] = make(chan int)
	}
	// 运行三轮测试，每轮启动多个客户端执行随机操作。
	for i := 0; i < 3; i++ {
		log.Printf("TEST CLIENT Iteration %v\n", i)
		atomic.StoreInt32(&done_clients, 0)
		atomic.StoreInt32(&done_partitioner, 0)
		go spawn_clients_and_wait(t, cfg, nclients, func(cli int, myck *Clerk, t *testing.T) {
			j := 0
			defer func() {
				clnts[cli] <- j
				log.Printf("TEST CLIENT %d: COMPLETED ALL OPERATIONS, j=%d", cli, j)
			}()
			last := "" // only used when not randomkeys
			if !randomkeys {
				log.Printf("TEST CLIENT %d: put operation, key=%s, value=%s", cli, strconv.Itoa(cli), last)
				Put(cfg, myck, strconv.Itoa(cli), last, opLog, cli)
			}
			for atomic.LoadInt32(&done_clients) == 0 {
				var key string
				if randomkeys {
					key = strconv.Itoa(rand.Intn(nclients))
				} else {
					key = strconv.Itoa(cli)
				}
				nv := "x " + strconv.Itoa(cli) + " " + strconv.Itoa(j) + " y"
				// 50% 概率执行 Append，更新 last（非随机键时）。
				if (rand.Int() % 1000) < 500 {
					log.Printf("TEST CLIENT %d: new append key %v value %v\n", cli, key, nv)
					Append(cfg, myck, key, nv, opLog, cli)
					if !randomkeys {
						last = NextValue(last, nv)
					}
					j++
				} else if randomkeys && (rand.Int()%1000) < 100 {
					// 10% 概率执行 Put 操作
					// we only do this when using random keys, because it would break the
					// check done after Get() operations
					log.Printf("TEST CLIENT %d: put operation, key=%s， value=%s", cli, key, nv)
					Put(cfg, myck, key, nv, opLog, cli)
					j++
				} else {
					// 其余执行 Get，验证结果
					log.Printf("TEST CLIENT %d: get %v\n", cli, key)
					v := Get(cfg, myck, key, opLog, cli)
					// the following check only makes sense when we're not using random keys
					if !randomkeys && v != last {
						t.Fatalf("TEST CLIENT %d: get wrong value, key %v, wanted:\n%v\n, got\n%v\n", cli, key, last, v)
					}
				}
			}
		})

		if partitions {
			// Allow the clients to perform some operations without interruption
			time.Sleep(1 * time.Second)
			log.Printf("TEST CLIENT start partitioner\n")
			go partitioner(t, cfg, ch_partitioner, &done_partitioner)
		}
		time.Sleep(5 * time.Second)

		atomic.StoreInt32(&done_clients, 1)     // tell clients to quit
		atomic.StoreInt32(&done_partitioner, 1) // tell partitioner to quit

		if partitions {
			log.Printf("TEST CLIENT wait for partitioner\n")
			<-ch_partitioner
			// reconnect network and submit a request. A client may
			// have submitted a request in a minority.  That request
			// won't return until that server discovers a new term
			// has started.
			cfg.ConnectAll()
			// wait for a while so that we have a new term
			time.Sleep(electionTimeout)
		}

		// 如果启用崩溃，关闭所有服务器，等待选举超时后重启。
		if crash {
			log.Printf("TEST CLIENT shutdown servers\n")
			for i := 0; i < nservers; i++ {
				log.Printf("TEST CLIENT shutdown server %d\n", i)
				cfg.ShutdownServer(i)
			}
			// Wait for a while for servers to shut down, since
			// shutdown isn't a real crash and isn't instantaneous
			time.Sleep(electionTimeout)
			log.Printf("TEST CLIENT restart servers\n")
			// crash and re-start all
			for i := 0; i < nservers; i++ {
				log.Printf("TEST CLIENT restart server %d\n", i)
				cfg.StartServer(i)
			}
			log.Printf("TEST CLIENT Connect all servers\n")
			cfg.ConnectAll()
		}

		log.Printf("TEST CLIENT validate each client\n")
		// 验证每个客户端的操作结果，确保追加顺序正确（非随机键时）。
		for i := 0; i < nclients; i++ {
			log.Printf("TEST CLIENT read from clients %d\n", i)
			j := <-clnts[i]
			// if j < 10 {
			// 	log.Printf("Warning: client %d managed to perform only %d put operations in 1 sec?\n", i, j)
			// }
			key := strconv.Itoa(i)
			log.Printf("TEST CLIENT Check %v for client %d\n", j, i)
			v := Get(cfg, ck, key, opLog, 0)
			if !randomkeys {
				checkClntAppends(t, i, v, j)
			}
			log.Printf("TEST CLIENT %d: validate finished\n", i)
		}

		// 日志和快照检查
		// 若 maxraftstate > 0，检查日志大小是否被裁剪。
		if maxraftstate > 0 {
			// Check maximum after the servers have processed all client
			// requests and had time to checkpoint.
			log.Printf("TEST CLIENT Check maximum after the servers have processed all client\n")
			sz := cfg.LogSize()
			if sz > 8*maxraftstate {
				t.Fatalf("TEST CLIENT logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
			}
		}
		// 若 maxraftstate < 0，确保未使用快照。
		if maxraftstate < 0 {
			// Check that snapshots are not used
			log.Printf("TEST CLIENT Check that snapshots are not used\n")
			ssz := cfg.SnapshotSize()
			if ssz > 0 {
				t.Fatalf("TEST CLIENT snapshot too large (%v), should not be used when maxraftstate = %d", ssz, maxraftstate)
			}
		}
	}

	// 使用 Porcupine 检查操作日志的线性一致性，若不一致则失败。
	res, info := porcupine.CheckOperationsVerbose(models.KvModel, opLog.Read(), linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("TEST CLIENT info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("TEST CLIENT info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("TEST CLIENT info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("TEST CLIENT history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("TEST CLIENT info: linearizability check timed out, assuming history is ok")
	}

	cfg.end()
}

// Check that ops are committed fast enough, better than 1 per heartbeat interval
func GenericTestSpeed(t *testing.T, part string, maxraftstate int) {
	// 初始化常量和测试环境
	const nservers = 3
	const numOps = 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin(fmt.Sprintf("TEST CLIENT: ops complete fast enough (%s)", part))

	// wait until first op completes, so we know a leader is elected
	// and KV servers are ready to process client requests
	// 初始 Get 操作确保 Raft 集群已选举出领导者，键/值服务准备好处理请求，避免后续操作因初始化延迟而受影响。
	ck.Get("x")

	start := time.Now()
	log.Printf("TEST CLIENT %d start time: %v\n", 0, start)
	// 使用单一键 "x" 测试连续追加操作的性能。
	for i := 0; i < numOps; i++ {
		start_iter := time.Now()
		ck.Append("x", "x 0 "+strconv.Itoa(i)+" y")
		end_iter := time.Now()
		log.Printf("TEST CLIENT: %d, iter: %d, time: %v\n", 0, i, end_iter.Sub(start_iter))
	}
	dur := time.Since(start)
	log.Printf("TEST CLIENT %d end time: %v\n", 0, time.Now())
	log.Printf("TEST CLIENT %d duration: %v\n", 0, dur)
	v := ck.Get("x")
	checkClntAppends(t, 0, v, numOps)

	// heartbeat interval should be ~ 100 ms; require at least 3 ops per
	const heartbeatInterval = 100 * time.Millisecond
	const opsPerInterval = 3
	const timePerOp = heartbeatInterval / opsPerInterval
	if dur > numOps*timePerOp {
		t.Fatalf("TEST CLIENT %d Operations completed too slowly %v/op > %v/op\n", 0, dur/numOps, timePerOp)
	}

	cfg.end()
}

func TestBasic3A(t *testing.T) {
	// Test: one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, false, false, -1, false)
}

func TestSpeed3A(t *testing.T) {
	GenericTestSpeed(t, "3A", -1)
}

func TestConcurrent3A(t *testing.T) {
	// Test: many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, false, false, -1, false)
}

func TestUnreliable3A(t *testing.T) {
	// Test: unreliable net, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, false, false, -1, false)
}

// 测试在不可靠网络条件下，多个客户端对同一键进行并发 Append 操作时，键/值服务是否能正确处理并保持一致性
func TestUnreliableOneKey3A(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, true, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: concurrent append to same key, unreliable (3A)")

	Put(cfg, ck, "k", "", nil, -1)

	const nclient = 5 // 定义 5 个并发客户端。
	const upto = 10   // 每个客户端执行 10 次追加。
	spawn_clients_and_wait(t, cfg, nclient, func(me int, myck *Clerk, t *testing.T) {
		n := 0
		for n < upto {
			Append(cfg, myck, "k", "x "+strconv.Itoa(me)+" "+strconv.Itoa(n)+" y", nil, -1)
			n++
		}
	})

	var counts []int
	for i := 0; i < nclient; i++ {
		counts = append(counts, upto)
	}

	vx := Get(cfg, ck, "k", nil, -1)
	checkConcurrentAppends(t, vx, counts)

	cfg.end()
}

// Submit a request in the minority partition and check that the requests
// doesn't go through until the partition heals.  The leader in the original
// network ends up in the minority partition.
// 测试在网络分区情况下，少数派分区中的请求是否被阻塞，直到分区恢复。
// 特别关注初始领导者落入少数派分区时的行为。这是 Raft 测试 "3A" 部分的一个用例，验证分区容错性。
func TestOnePartition3A(t *testing.T) {
	const nservers = 5
	cfg := make_config(t, nservers, false, -1)
	defer cfg.cleanup()
	ck := cfg.makeClient(cfg.All())

	Put(cfg, ck, "1", "13", nil, -1)

	// 多数派分区中的进展
	cfg.begin("Test: progress in majority (3A)")
	p1, p2 := cfg.make_partition()
	cfg.partition(p1, p2)

	ckp1 := cfg.makeClient(p1)  // connect ckp1 to p1
	ckp2a := cfg.makeClient(p2) // connect ckp2a to p2
	ckp2b := cfg.makeClient(p2) // connect ckp2b to p2

	Put(cfg, ckp1, "1", "14", nil, -1)
	check(cfg, t, ckp1, "1", "14")

	cfg.end()

	done0 := make(chan bool)
	done1 := make(chan bool)

	// 少数派分区无进展
	cfg.begin("Test: no progress in minority (3A)")
	go func() {
		Put(cfg, ckp2a, "1", "15", nil, -1)
		done0 <- true
	}()
	go func() {
		Get(cfg, ckp2b, "1", nil, -1) // different clerk in p2
		done1 <- true
	}()

	select {
	case <-done0:
		t.Fatalf("Put in minority completed")
	case <-done1:
		t.Fatalf("Get in minority completed")
	case <-time.After(time.Second):
	}

	check(cfg, t, ckp1, "1", "14")
	Put(cfg, ckp1, "1", "16", nil, -1)
	check(cfg, t, ckp1, "1", "16")

	cfg.end()

	// 分区恢复后完成
	cfg.begin("Test: completion after heal (3A)")

	cfg.ConnectAll()
	cfg.ConnectClient(ckp2a, cfg.All())
	cfg.ConnectClient(ckp2b, cfg.All())

	time.Sleep(electionTimeout)

	select {
	case <-done0:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Put did not complete")
	}

	select {
	case <-done1:
	case <-time.After(30 * 100 * time.Millisecond):
		t.Fatalf("Get did not complete")
	default:
	}

	check(cfg, t, ck, "1", "15")

	cfg.end()
}

func TestManyPartitionsOneClient3A(t *testing.T) {
	// Test: partitions, one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, false, true, -1, false)
}

func TestManyPartitionsManyClients3A(t *testing.T) {
	// Test: partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, false, true, -1, false)
}

func TestPersistOneClient3A(t *testing.T) {
	// Test: restarts, one client (3A) ...
	GenericTest(t, "3A", 1, 5, false, true, false, -1, false)
}

func TestPersistConcurrent3A(t *testing.T) {
	// Test: restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, true, false, -1, false)
}

func TestPersistConcurrentUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, true, false, -1, false)
}

func TestPersistPartition3A(t *testing.T) {
	// Test: restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, false, true, true, -1, false)
}

func TestPersistPartitionUnreliable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, many clients (3A) ...
	GenericTest(t, "3A", 5, 5, true, true, true, -1, false)
}

func TestPersistPartitionUnreliableLinearizable3A(t *testing.T) {
	// Test: unreliable net, restarts, partitions, random keys, many clients (3A) ...
	GenericTest(t, "3A", 15, 7, true, true, true, -1, true)
}

// if one server falls behind, then rejoins, does it
// recover by using the InstallSnapshot RPC?
// also checks that majority discards committed log entries
// even if minority doesn't respond.
// 测试 Raft 的快照机制（InstallSnapshot RPC）是否能帮助落后服务器在重新加入集群时恢复状态。
// 同时验证多数派服务器是否能在少数派未响应的情况下丢弃已提交的日志条目。
func TestSnapshotRPC3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: InstallSnapshot RPC (3B)")

	log.Printf("TEST CLIENT=%d: Put operation, key=a, value=A\n", 0)
	Put(cfg, ck, "a", "A", nil, -1)
	log.Printf("TEST CLIENT=%d: Check operation, key=a, value=A\n", 0)
	check(cfg, t, ck, "a", "A")

	// a bunch of puts into the majority partition.
	// 在多数派中生成大量日志条目，使日志大小接近或超过 maxraftstate，触发快照。
	// 服务器 2（少数派）因分区而落后。
	log.Printf("TEST CLIENT Partitioning to region{0,1} and region{2}\n")
	cfg.partition([]int{0, 1}, []int{2})
	{
		ck1 := cfg.makeClient([]int{0, 1})
		for i := 0; i < 50; i++ {
			log.Printf("TEST CLIENT=%d: Put operation, key=%d, value=%d\n", 1, i, i)
			Put(cfg, ck1, strconv.Itoa(i), strconv.Itoa(i), nil, -1)
		}
		time.Sleep(electionTimeout)
		log.Printf("TEST CLIENT=%d: Put operation, key=b, value=B\n", 1)
		Put(cfg, ck1, "b", "B", nil, -1)
	}

	// check that the majority partition has thrown away
	// most of its log entries.
	sz := cfg.LogSize()
	if sz > 8*maxraftstate {
		t.Fatalf("TEST CLIENT logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// now make group that requires participation of
	// lagging server, so that it has to catch up.
	// 测试服务器 2 是否通过 InstallSnapshot RPC 从服务器 0 获取快照，恢复状态
	log.Printf("TEST CLIENT Partitioning to region{0,2} and region{1}\n")
	cfg.partition([]int{0, 2}, []int{1})
	{
		ck1 := cfg.makeClient([]int{0, 2})
		log.Printf("TEST CLIENT=%d: Put operation, key=c, value=C\n", 1)
		Put(cfg, ck1, "c", "C", nil, -1)
		log.Printf("TEST CLIENT=%d: Check operation, key=d, value=D\n", 1)
		Put(cfg, ck1, "d", "D", nil, -1)
		log.Printf("TEST CLIENT=%d: Check operation, key=a, value=A\n", 1)
		check(cfg, t, ck1, "a", "A")
		log.Printf("TEST CLIENT=%d: Check operation, key=b, value=B\n", 1)
		check(cfg, t, ck1, "b", "B")
		log.Printf("TEST CLIENT=%d: Check operation, key=1, value=1\n", 1)
		check(cfg, t, ck1, "1", "1")
		log.Printf("TEST CLIENT=%d: Check operation, key=49, value=49\n", 1)
		check(cfg, t, ck1, "49", "49")
	}

	// now everybody
	log.Printf("TEST CLIENT Partitioning to region{0,1,2}\n")
	cfg.partition([]int{0, 1, 2}, []int{})

	log.Printf("TEST CLIENT=%d:Put operation, key=e, value=E\n", 0)
	Put(cfg, ck, "e", "E", nil, -1)
	log.Printf("TEST CLIENT=%d:Check operation, key=c, value=C\n", 0)
	check(cfg, t, ck, "c", "C")
	log.Printf("TEST CLIENT=%d:Check operation, key=e, value=E\n", 0)
	check(cfg, t, ck, "e", "E")
	log.Printf("TEST CLIENT=%d:Check operation, key=1, value=1\n", 0)
	check(cfg, t, ck, "1", "1")

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
// 测试快照大小是否合理，确保其不超过预设上限（500 字节），验证 Raft 快照机制的效率。
func TestSnapshotSize3B(t *testing.T) {
	const nservers = 3
	maxraftstate := 1000
	maxsnapshotstate := 500
	cfg := make_config(t, nservers, false, maxraftstate)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	cfg.begin("Test: snapshot size is reasonable (3B)")
	// 生成大量日志条目（400 次 Put），触发快照，确保日志超限。
	for i := 0; i < 200; i++ {
		log.Printf("TEST CLIENT=%d: Put operation, key=x, value=0\n", 0)
		Put(cfg, ck, "x", "0", nil, -1)
		log.Printf("TEST CLIENT=%d: Check operation, key=x, value=0\n", 0)
		check(cfg, t, ck, "x", "0")
		log.Printf("TEST CLIENT=%d: Put operation, key=x, value=1\n", 0)
		Put(cfg, ck, "x", "1", nil, -1)
		log.Printf("TEST CLIENT=%d: Check operation, key=x, value=1\n", 0)
		check(cfg, t, ck, "x", "1")
	}

	// check that servers have thrown away most of their log entries
	log.Printf("TEST CLIENT check that servers have thrown away most of their log entries")
	sz := cfg.LogSize()
	log.Printf("TEST CLIENT snapshot size: %v\n", sz)
	if sz > 8*maxraftstate {
		t.Fatalf("TEST CLIENT logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
	}

	// check that the snapshots are not unreasonably large
	log.Printf("TEST CLIENT check that the snapshots are not unreasonably large")
	ssz := cfg.SnapshotSize()
	log.Printf("TEST CLIENT snapshot size: %v\n", ssz)
	if ssz > maxsnapshotstate {
		t.Fatalf("TEST CLIENT snapshot too large (%v > %v)", ssz, maxsnapshotstate)
	}

	cfg.end()
}

// are the snapshots not too huge? 500 bytes is a generous bound for the
// operations we're doing here.
// 测试快照大小是否合理，确保其不超过预设上限（500 字节），验证 Raft 快照机制的效率。
//func TestNoSnapshotSize3B(t *testing.T) {
//	const nservers = 3
//	maxraftstate := -1
//	maxsnapshotstate := 500
//	cfg := make_config(t, nservers, false, maxraftstate)
//	defer cfg.cleanup()
//
//	ck := cfg.makeClient(cfg.All())
//
//	cfg.begin("Test: snapshot size is reasonable (3B)")
//	// 生成大量日志条目（400 次 Put），触发快照，确保日志超限。
//	for i := 0; i < 200; i++ {
//		log.Printf("client=%d: Put operation, key=x, value=0\n", 0)
//		Put(cfg, ck, "x", "0", nil, -1)
//		log.Printf("client=%d: Check operation, key=x, value=0\n", 0)
//		check(cfg, t, ck, "x", "0")
//		log.Printf("client=%d: Put operation, key=x, value=1\n", 0)
//		Put(cfg, ck, "x", "1", nil, -1)
//		log.Printf("client=%d: Check operation, key=x, value=1\n", 0)
//		check(cfg, t, ck, "x", "1")
//	}
//
//	// check that servers have thrown away most of their log entries
//	log.Printf("check that servers have thrown away most of their log entries")
//	sz := cfg.LogSize()
//	log.Printf("snapshot size: %v\n", sz)
//	if sz > 8*maxraftstate {
//		t.Fatalf("logs were not trimmed (%v > 8*%v)", sz, maxraftstate)
//	}
//
//	// check that the snapshots are not unreasonably large
//	log.Printf("check that the snapshots are not unreasonably large")
//	ssz := cfg.SnapshotSize()
//	log.Printf("snapshot size: %v\n", ssz)
//	if ssz > maxsnapshotstate {
//		t.Fatalf("snapshot too large (%v > %v)", ssz, maxsnapshotstate)
//	}
//
//	cfg.end()
//}

func TestSpeed3B(t *testing.T) {
	GenericTestSpeed(t, "3B", 1000)
}

func TestSnapshotRecover3B(t *testing.T) {
	// Test: restarts, snapshots, one client (3B) ...
	GenericTest(t, "3B", 1, 5, false, true, false, 1000, false)
}

func TestSnapshotRecoverManyClients3B(t *testing.T) {
	// Test: restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 20, 5, false, true, false, 1000, false)
}

func TestSnapshotUnreliable3B(t *testing.T) {
	// Test: unreliable net, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, false, false, 1000, false)
}

func TestSnapshotUnreliableRecover3B(t *testing.T) {
	// Test: unreliable net, restarts, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, true, false, 1000, false)
}

func TestSnapshotUnreliableRecoverConcurrentPartition3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, many clients (3B) ...
	GenericTest(t, "3B", 5, 5, true, true, true, 1000, false)
}

func TestSnapshotUnreliableRecoverConcurrentPartitionLinearizable3B(t *testing.T) {
	// Test: unreliable net, restarts, partitions, snapshots, random keys, many clients (3B) ...
	GenericTest(t, "3B", 15, 7, true, true, true, 1000, true)
}
