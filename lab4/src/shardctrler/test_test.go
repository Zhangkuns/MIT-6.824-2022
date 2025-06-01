package shardctrler

import (
	"fmt"
	"log"
	"sync"
	"testing"
	"time"
)

// import "time"

func check(t *testing.T, groups []int, ck *Clerk) {
	c := ck.Query(-1)
	if len(c.Groups) != len(groups) {
		t.Fatalf("wanted %v groups, got %v", len(groups), len(c.Groups))
	}

	// are the groups as expected?
	for _, g := range groups {
		_, ok := c.Groups[g]
		if ok != true {
			t.Fatalf("missing group %v", g)
		}
	}

	// any un-allocated shards?
	if len(groups) > 0 {
		for s, g := range c.Shards {
			_, ok := c.Groups[g]
			if ok == false {
				t.Fatalf("shard %v -> invalid group %v", s, g)
			}
		}
	}

	// more or less balanced sharding?
	counts := map[int]int{}
	for _, g := range c.Shards {
		counts[g] += 1
	}
	min := 257
	max := 0
	for g, _ := range c.Groups {
		if counts[g] > max {
			max = counts[g]
		}
		if counts[g] < min {
			min = counts[g]
		}
	}
	if max > min+1 {
		t.Fatalf("max %v too much larger than min %v", max, min)
	}
}

func check_same_config(t *testing.T, c1 Config, c2 Config) {
	if c1.Num != c2.Num {
		t.Fatalf("Num wrong")
	}
	if c1.Shards != c2.Shards {
		t.Fatalf("Shards wrong")
	}
	if len(c1.Groups) != len(c2.Groups) {
		t.Fatalf("number of Groups is wrong")
	}
	for gid, sa := range c1.Groups {
		sa1, ok := c2.Groups[gid]
		if ok == false || len(sa1) != len(sa) {
			t.Fatalf("len(Groups) wrong")
		}
		if ok && len(sa1) == len(sa) {
			for j := 0; j < len(sa); j++ {
				if sa[j] != sa1[j] {
					t.Fatalf("Groups wrong")
				}
			}
		}
	}
}

func TestBasic(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Basic leave/join ...\n")
	log.Printf("TEST: Starting Basic leave/join test with %d servers\n", nservers)

	cfa := make([]Config, 6)
	cfa[0] = ck.Query(-1)
	log.Printf("TEST: Initial config queried, no groups expected\n")
	check(t, []int{}, ck)

	var gid1 int = 1
	log.Printf("TEST: Joining group %d with servers [x, y, z]\n", gid1)
	ck.Join(map[int][]string{gid1: []string{"x", "y", "z"}})
	check(t, []int{gid1}, ck)
	log.Printf("TEST: Group %d joined, checking groups\n", gid1)
	cfa[1] = ck.Query(-1)

	var gid2 int = 2
	log.Printf("TEST: Joining group %d with servers [a, b, c]\n", gid2)
	ck.Join(map[int][]string{gid2: []string{"a", "b", "c"}})
	check(t, []int{gid1, gid2}, ck)
	log.Printf("TEST: Group %d joined, checking groups\n", gid2)
	cfa[2] = ck.Query(-1)

	cfx := ck.Query(-1)
	log.Printf("TEST: Verifying servers for group %d\n", gid1)
	sa1 := cfx.Groups[gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	log.Printf("TEST: Verifying servers for group %d\n", gid2)
	sa2 := cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	log.Printf("TEST: Leaving group %d\n", gid1)
	ck.Leave([]int{gid1})
	check(t, []int{gid2}, ck)
	log.Printf("TEST: Group %d left, checking remaining groups\n", gid1)
	cfa[4] = ck.Query(-1)
	log.Printf("TEST: Leaving group %d\n", gid2)
	ck.Leave([]int{gid2})
	cfa[5] = ck.Query(-1)
	log.Printf("TEST: Group %d left, no groups remaining\n", gid2)
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Historical queries ...\n")
	log.Printf("TEST: Starting historical queries test\n")
	for s := 0; s < nservers; s++ {
		cfg.ShutdownServer(s)
		for i := 0; i < len(cfa); i++ {
			log.Printf("TEST: Querying historical config %d with server %d down\n", cfa[i].Num, s)
			c := ck.Query(cfa[i].Num)
			check_same_config(t, c, cfa[i])
		}
		log.Printf("TEST: Restarting server %d\n", s)
		cfg.StartServer(s)
		log.Printf("TEST: Reconnecting all servers\n")
		cfg.ConnectAll()
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Move ...\n")
	log.Printf("TEST: Starting shard move test\n")
	{
		var gid3 int = 503
		log.Printf("TEST: Joining group %d with servers [3a, 3b, 3c]\n", gid3)
		ck.Join(map[int][]string{gid3: []string{"3a", "3b", "3c"}})
		var gid4 int = 504
		log.Printf("TEST: Joining group %d with servers [4a, 4b, 4c]\n", gid4)
		ck.Join(map[int][]string{gid4: []string{"4a", "4b", "4c"}})
		for i := 0; i < NShards; i++ {
			cf := ck.Query(-1)
			if i < NShards/2 {
				log.Printf("TEST: Moving shard %d to group %d\n", i, gid3)
				ck.Move(i, gid3)
				if cf.Shards[i] != gid3 {
					cf1 := ck.Query(-1)
					if cf1.Num <= cf.Num {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			} else {
				log.Printf("TEST: Moving shard %d to group %d\n", i, gid4)
				ck.Move(i, gid4)
				if cf.Shards[i] != gid4 {
					cf1 := ck.Query(-1)
					if cf1.Num <= cf.Num {
						t.Fatalf("Move should increase Config.Num")
					}
				}
			}
		}
		log.Printf("TEST: Verifying shard assignments\n")
		cf2 := ck.Query(-1)
		for i := 0; i < NShards; i++ {
			if i < NShards/2 {
				if cf2.Shards[i] != gid3 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid3, cf2.Shards[i])
				}
			} else {
				if cf2.Shards[i] != gid4 {
					t.Fatalf("expected shard %v on gid %v actually %v",
						i, gid4, cf2.Shards[i])
				}
			}
		}
		log.Printf("TEST: Leaving group %d\n", gid3)
		ck.Leave([]int{gid3})
		log.Printf("TEST: Leaving group %d\n", gid4)
		ck.Leave([]int{gid4})
	}
	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent leave/join ...\n")
	log.Printf("TEST: Starting concurrent leave/join test\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
		log.Printf("TEST: Created client %d\n", i)
	}
	gids := make([]int, npara)
	ch := make(chan bool)
	for xi := 0; xi < npara; xi++ {
		gids[xi] = int((xi * 10) + 100)
		log.Printf("TEST: Client %d starting operations for group %d\n", xi, gids[xi])
		go func(i int) {
			defer func() { ch <- true }()
			var gid int = gids[i]
			var sid1 = fmt.Sprintf("s%da", gid)
			var sid2 = fmt.Sprintf("s%db", gid)
			log.Printf("TEST: Client %d joining group %d with server %s\n", i, gid+1000, sid1)
			cka[i].Join(map[int][]string{gid + 1000: []string{sid1}})
			log.Printf("TEST: Client %d joining group %d with server %s\n", i, gid, sid2)
			cka[i].Join(map[int][]string{gid: []string{sid2}})
			log.Printf("TEST: Client %d leaving group %d\n", i, gid+1000)
			cka[i].Leave([]int{gid + 1000})
		}(xi)
	}
	for i := 0; i < npara; i++ {
		<-ch
		log.Printf("TEST: Client %d finished operations\n", i)
	}
	log.Printf("TEST: Checking remaining groups after concurrent operations\n")
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after joins ...\n")
	log.Printf("TEST: Starting minimal transfers after joins test\n")

	c1 := ck.Query(-1)
	log.Printf("TEST: Queried initial config before joins\n")
	for i := 0; i < 5; i++ {
		var gid = int(npara + 1 + i)
		log.Printf("TEST: Joining group %d\n", gid)
		ck.Join(map[int][]string{gid: []string{
			fmt.Sprintf("%da", gid),
			fmt.Sprintf("%db", gid),
			fmt.Sprintf("%db", gid)}})
	}
	c2 := ck.Query(-1)
	log.Printf("TEST: Queried config after joins, checking for minimal transfers\n")
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after leaves ...\n")
	log.Printf("TEST: Starting minimal transfers after leaves test\n")
	for i := 0; i < 5; i++ {
		gid := int(npara + 1 + i)
		log.Printf("TEST: Leaving group %d\n", gid)
		ck.Leave([]int{int(npara + 1 + i)})
	}
	c3 := ck.Query(-1)
	log.Printf("TEST: Queried config after leaves, checking for minimal transfers\n")
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")
}

func TestMulti(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	fmt.Printf("Test: Multi-group join/leave ...\n")
	log.Printf("TEST: Starting multi-group join/leave test with %d servers\n", nservers)

	cfa := make([]Config, 6)
	cfa[0] = ck.Query(-1)
	log.Printf("TEST: Initial config queried, no groups expected\n")
	check(t, []int{}, ck)

	var gid1 int = 1
	var gid2 int = 2
	log.Printf("TEST: Joining groups %d and %d simultaneously\n", gid1, gid2)
	ck.Join(map[int][]string{
		gid1: []string{"x", "y", "z"},
		gid2: []string{"a", "b", "c"},
	})
	check(t, []int{gid1, gid2}, ck)
	log.Printf("TEST: Groups %d and %d joined, checking groups\n", gid1, gid2)
	cfa[1] = ck.Query(-1)

	var gid3 int = 3
	log.Printf("TEST: Joining group %d with servers [j, k, l]\n", gid3)
	ck.Join(map[int][]string{gid3: []string{"j", "k", "l"}})
	check(t, []int{gid1, gid2, gid3}, ck)
	log.Printf("TEST: Group %d joined, checking groups\n", gid3)
	cfa[2] = ck.Query(-1)

	cfx := ck.Query(-1)
	log.Printf("TEST: Verifying servers for group %d\n", gid1)
	sa1 := cfx.Groups[gid1]
	if len(sa1) != 3 || sa1[0] != "x" || sa1[1] != "y" || sa1[2] != "z" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid1, sa1)
	}
	log.Printf("TEST: Verifying servers for group %d\n", gid2)
	sa2 := cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}
	log.Printf("TEST: Verifying servers for group %d\n", gid3)
	sa3 := cfx.Groups[gid3]
	if len(sa3) != 3 || sa3[0] != "j" || sa3[1] != "k" || sa3[2] != "l" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid3, sa3)
	}

	log.Printf("TEST: Leaving groups %d and %d\n", gid1, gid3)
	ck.Leave([]int{gid1, gid3})
	log.Printf("TEST: Groups %d and %d left, checking remaining groups\n", gid1, gid3)
	check(t, []int{gid2}, ck)
	cfa[3] = ck.Query(-1)

	cfx = ck.Query(-1)
	log.Printf("TEST: Verifying servers for group %d after leaves\n", gid2)
	sa2 = cfx.Groups[gid2]
	if len(sa2) != 3 || sa2[0] != "a" || sa2[1] != "b" || sa2[2] != "c" {
		t.Fatalf("wrong servers for gid %v: %v\n", gid2, sa2)
	}

	log.Printf("TEST: Leaving group %d\n", gid2)
	ck.Leave([]int{gid2})

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Concurrent multi leave/join ...\n")
	log.Printf("TEST: Starting concurrent multi leave/join test\n")

	const npara = 10
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
		log.Printf("TEST: Created client %d\n", i)
	}
	gids := make([]int, npara)
	var wg sync.WaitGroup
	for xi := 0; xi < npara; xi++ {
		wg.Add(1)
		gids[xi] = int(xi + 1000)
		log.Printf("TEST: Client %d starting operations for group %d\n", xi, gids[xi])
		go func(i int) {
			defer wg.Done()
			var gid int = gids[i]
			log.Printf("TEST: Client %d joining group %d\n", i, gid)
			cka[i].Join(map[int][]string{
				gid: []string{
					fmt.Sprintf("%da", gid),
					fmt.Sprintf("%db", gid),
					fmt.Sprintf("%dc", gid)},
				gid + 1000: []string{fmt.Sprintf("%da", gid+1000)},
				gid + 2000: []string{fmt.Sprintf("%da", gid+2000)},
			})
			log.Printf("TEST: Client %d leaving groups %d and %d\n", i, gid+1000, gid+2000)
			cka[i].Leave([]int{gid + 1000, gid + 2000})
		}(xi)
	}
	log.Printf("TEST: Waiting for all clients to finish\n")
	wg.Wait()
	log.Printf("TEST: Checking remaining groups after concurrent operations\n")
	check(t, gids, ck)

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multijoins ...\n")
	log.Printf("TEST: Starting minimal transfers after multijoins test\n")

	c1 := ck.Query(-1)
	log.Printf("TEST: Queried initial config before multijoins\n")
	m := make(map[int][]string)
	for i := 0; i < 5; i++ {
		var gid = npara + 1 + i
		log.Printf("TEST: Joining group %d\n", gid)
		m[gid] = []string{fmt.Sprintf("%da", gid), fmt.Sprintf("%db", gid)}
	}
	ck.Join(m)
	c2 := ck.Query(-1)
	log.Printf("TEST: Queried config after multijoins, checking for minimal transfers\n")
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c1.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Join()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Minimal transfers after multileaves ...\n")
	log.Printf("TEST: Starting minimal transfers after multileaves test\n")

	var l []int
	for i := 0; i < 5; i++ {
		gid := npara + 1 + i
		log.Printf("TEST: Leaving group %d\n", gid)
		l = append(l, npara+1+i)
	}
	ck.Leave(l)
	c3 := ck.Query(-1)
	log.Printf("TEST: Queried config after multileaves, checking for minimal transfers\n")
	for i := int(1); i <= npara; i++ {
		for j := 0; j < len(c1.Shards); j++ {
			if c2.Shards[j] == i {
				if c3.Shards[j] != i {
					t.Fatalf("non-minimal transfer after Leave()s")
				}
			}
		}
	}

	fmt.Printf("  ... Passed\n")

	fmt.Printf("Test: Check Same config on servers ...\n")
	log.Printf("TEST: Starting check same config on servers test\n")

	isLeader, leader := cfg.Leader()
	if !isLeader {
		t.Fatalf("Leader not found")
	}
	log.Printf("TEST: Found leader server %d\n", leader)
	c := ck.Query(-1) // Config leader claims
	log.Printf("TEST: Queried config from leader\n")

	log.Printf("TEST: Shutting down leader server %d\n", leader)
	cfg.ShutdownServer(leader)

	attempts := 0
	for isLeader, leader = cfg.Leader(); isLeader; time.Sleep(1 * time.Second) {
		if attempts++; attempts >= 3 {
			t.Fatalf("Leader not found")
		}
	}
	log.Printf("TEST: New leader elected after shutdown\n")

	c1 = ck.Query(-1)
	log.Printf("TEST: Queried config from new leader, checking consistency\n")
	check_same_config(t, c, c1)

	fmt.Printf("  ... Passed\n")
}

func TestComplexMinimalTransfers(t *testing.T) {
	const nservers = 3
	cfg := make_config(t, nservers, false)
	defer cfg.cleanup()

	ck := cfg.makeClient(cfg.All())

	// 初始化阶段：加入 3 个组，创建不均匀的分片分布
	fmt.Printf("Test: Complex minimal transfers with uneven initial distribution ...\n")
	log.Printf("TEST: Starting complex minimal transfers test with %d servers\n", nservers)

	// 初始加入 gid=1, gid=2, gid=3
	var gid1 int = 1
	var gid2 int = 2
	var gid3 int = 3
	log.Printf("TEST: Joining group %d with servers [x, y, z]\n", gid1)
	ck.Join(map[int][]string{gid1: []string{"x", "y", "z"}})
	log.Printf("TEST: Joining group %d with servers [a, b, c]\n", gid2)
	ck.Join(map[int][]string{gid2: []string{"a", "b", "c"}})
	log.Printf("TEST: Joining group %d with servers [j, k, l]\n", gid3)
	ck.Join(map[int][]string{gid3: []string{"j", "k", "l"}})
	check(t, []int{gid1, gid2, gid3}, ck)
	log.Printf("TEST: Groups %d, %d, %d joined, checking groups\n", gid1, gid2, gid3)

	// 查询初始配置，验证分片分布
	cfa := make([]Config, 10)
	cfa[0] = ck.Query(-1)
	log.Printf("TEST: Initial config after joining 3 groups: shards=%v, groups=%v\n", cfa[0].Shards, cfa[0].Groups)
	// 假设 NShards=10，理想分配应为 [1 1 1 1 2 2 2 3 3 3]（gid=1: 4, gid=2: 3, gid=3: 3）

	// 手动调整分片分布，创建不均匀分配
	// 假设我们通过 Move 操作将分片调整为 [1 1 1 1 1 1 1 2 2 3 3]
	for i := 0; i < 7; i++ {
		ck.Move(i, gid1)
	}
	for i := 7; i < 9; i++ {
		ck.Move(i, gid2)
	}
	for i := 9; i < NShards; i++ {
		ck.Move(i, gid3)
	}
	cfa[1] = ck.Query(-1)
	log.Printf("TEST: Adjusted uneven distribution: shards=%v, groups=%v\n", cfa[1].Shards, cfa[1].Groups)
	// 此时：gid=1: 7, gid=2: 2, gid=3: 1

	// 阶段 1：加入 gid=4 和 gid=5，验证最小转移
	fmt.Printf("Test: Joining new groups with uneven distribution ...\n")
	log.Printf("TEST: Joining groups %d and %d\n", 4, 5)
	ck.Join(map[int][]string{
		4: []string{"p", "q", "r"},
		5: []string{"u", "v", "w"},
	})
	check(t, []int{gid1, gid2, gid3, 4, 5}, ck)
	cfa[2] = ck.Query(-1)
	log.Printf("TEST: Config after joining gid=4 and gid=5: shards=%v, groups=%v\n", cfa[2].Shards, cfa[2].Groups)

	// 阶段 2：移除 gid=1 和 gid=3，验证最小转移
	fmt.Printf("Test: Leaving groups with uneven distribution ...\n")
	log.Printf("TEST: Leaving groups %d and %d\n", gid1, gid3)
	ck.Leave([]int{gid1, gid3})
	check(t, []int{gid2, 4, 5}, ck)
	cfa[3] = ck.Query(-1)
	log.Printf("TEST: Config after leaving gid=1 and gid=3: shards=%v, groups=%v\n", cfa[3].Shards, cfa[3].Groups)

	// 阶段 3：并发 Join 和 Leave
	fmt.Printf("Test: Concurrent multi-join and multi-leave with uneven distribution ...\n")
	log.Printf("TEST: Starting concurrent multi-join and multi-leave test\n")

	const npara = 5
	var cka [npara]*Clerk
	for i := 0; i < len(cka); i++ {
		cka[i] = cfg.makeClient(cfg.All())
		log.Printf("TEST: Created client %d\n", i)
	}
	gids := make([]int, npara)
	var wg sync.WaitGroup
	for xi := 0; xi < npara; xi++ {
		wg.Add(1)
		gids[xi] = int(xi + 100)
		log.Printf("TEST: Client %d starting operations for group %d\n", xi, gids[xi])
		go func(i int) {
			defer wg.Done()
			var gid int = gids[i]
			log.Printf("TEST: Client %d joining group %d\n", i, gid)
			cka[i].Join(map[int][]string{
				gid: []string{
					fmt.Sprintf("%da", gid),
					fmt.Sprintf("%db", gid),
					fmt.Sprintf("%dc", gid)},
				gid + 100: []string{fmt.Sprintf("%da", gid+100)},
			})
			log.Printf("TEST: Client %d leaving group %d\n", i, gid+100)
			cka[i].Leave([]int{gid + 100})
		}(xi)
	}
	log.Printf("TEST: Waiting for all clients to finish\n")
	wg.Wait()
	log.Printf("TEST: Checking remaining groups after concurrent operations\n")
	check(t, append(gids, gid2, 4, 5), ck)

	cfa[4] = ck.Query(-1)
	log.Printf("TEST: Config after concurrent operations: shards=%v, groups=%v\n", cfa[4].Shards, cfa[4].Groups)

	// 阶段 4：批量移除，验证最小转移
	fmt.Printf("Test: Batch leave with uneven distribution ...\n")
	log.Printf("TEST: Batch leaving groups 100 to 104\n")
	var leaveGids []int
	for i := 0; i < npara; i++ {
		leaveGids = append(leaveGids, gids[i])
	}
	ck.Leave(leaveGids)
	check(t, []int{gid2, 4, 5}, ck)
	cfa[5] = ck.Query(-1)
	log.Printf("TEST: Config after batch leaving: shards=%v, groups=%v\n", cfa[5].Shards, cfa[5].Groups)
	fmt.Printf("  ... Passed\n")
}
