package shardkv

import (
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"mit6.5840/models"
	"mit6.5840/porcupine"
)

const linearizabilityCheckTimeout = 1 * time.Second
const colorWhite = "\033[38;2;255;255;255m"

func check(t *testing.T, ck *Clerk, key string, value string) {
	v := ck.Get(key)
	if v != value {
		t.Fatalf("Get(%v): expected:\n%v\nreceived:\n%v", key, value, v)
	}
}

// test static 2-way sharding, without shard movement.
func TestStaticShardsOne(t *testing.T) {
	fmt.Printf(colorWhite + "Test: static Shards ..." + colorReset)
	log.Printf(colorWhite + "TEST CLIENT Start TestStaticShards" + colorReset)

	log.Printf(colorWhite+"TEST CLIENT Created config with %d servers"+colorReset, 3)
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	log.Printf(colorWhite + "TEST CLIENT Created client" + colorReset)
	ck := cfg.makeClient()

	// 加入两个组
	log.Printf(colorWhite + "TEST CLIENT join group 0" + colorReset)
	cfg.join(0)
	log.Printf(colorWhite + "TEST CLIENT waiting for 2s" + colorReset)
	time.Sleep(time.Second * 2)
	log.Printf(colorWhite + "TEST CLIENT join group 1" + colorReset)
	cfg.join(1)

	// 执行 Put 操作
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(20)
		log.Printf(colorWhite+"TEST CLIENT perform Put operation, key=%s, value=%s"+colorReset, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}

	// 验证 Get 操作
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	// 关闭一个复制组并验证部分 Get 失败
	log.Printf(colorWhite + "TEST CLIENT shut down group 1" + colorReset)
	cfg.ShutdownGroup(1)
	log.Printf(colorWhite + "TEST CLIENT check forbid snapshots" + colorReset)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		log.Printf(colorWhite+"TEST CLIENT create client %d for Get operation, key=%s"+colorReset, xi, ka[xi])
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			log.Printf(colorWhite+"TEST CLIENT Client %d: perform Get operation, key=%s"+colorReset, i, ka[i])
			v := ck1.Get(ka[i])
			if v != va[i] {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get failed, key=%s, expected=%s, got=%s"+colorReset, i, ka[i], va[i], v)
				ch <- fmt.Sprintf("Get(%v): expected:\n%v\nreceived:\n%v", ka[i], va[i], v)
			} else {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get succeeded, key=%s, value=%s"+colorReset, i, ka[i], v)
				ch <- ""
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	// 统计成功的 Get 操作
	log.Printf(colorWhite + "TEST CLIENT wait for Get operations to complete" + colorReset)
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err != "" {
				t.Fatal(err)
			}
			ndone += 1
			log.Printf(colorWhite+"TEST CLIENT Completed Get operation, ndone=%d"+colorReset, ndone)
		case <-time.After(time.Second * 2):
			log.Printf(colorWhite+"TEST CLIENT Timeout after 2 seconds, ndone=%d"+colorReset, ndone)
			done = true
			break
		}
	}

	if ndone != 5 {
		log.Printf(colorWhite+"TEST CLIENT Expected 5 completions with one shard dead; got %v"+colorReset, ndone)
		t.Fatalf("expected 5 completions with one shard dead; got %v\n", ndone)
	}
	log.Printf(colorWhite+"TEST CLIENT Verified %d completions with one shard dead"+colorReset, ndone)

	// bring the crashed shard/group back to life.
	// 重启复制组并验证恢复
	log.Printf(colorWhite + "TEST CLIENT start group 1" + colorReset)
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT After restart, perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	log.Printf(colorWhite + "TEST CLIENT TestStaticShards passed" + colorReset)
	fmt.Printf(colorWhite + "  ... Passed" + colorReset)
}

// test static 2-way sharding, without shard movement.
func TestStaticShardsTwo(t *testing.T) {
	fmt.Printf("Test: static Shards ..." + colorReset)
	log.Printf(colorWhite + "TEST CLIENT Start TestStaticShards" + colorReset)

	log.Printf(colorWhite+"TEST CLIENT Created config with %d servers"+colorReset, 3)
	ngroups := 3
	npergroup := 3
	cfg := make_configmore(t, ngroups, npergroup, false, -1)
	defer cfg.cleanup()

	log.Printf(colorWhite + "TEST CLIENT Created client" + colorReset)
	ck := cfg.makeClient()

	// 加入两个组
	log.Printf(colorWhite + "TEST CLIENT join group 0" + colorReset)
	cfg.join(0)
	log.Printf(colorWhite + "TEST CLIENT waiting for 2s" + colorReset)
	time.Sleep(time.Second * 2)
	log.Printf(colorWhite + "TEST CLIENT join group 1" + colorReset)
	cfg.join(1)
	time.Sleep(time.Second * 2)
	log.Printf(colorWhite + "TEST CLIENT join group 2" + colorReset)
	cfg.join(2)
	time.Sleep(time.Second * 2) // Extra time for final configuration

	// 执行 Put 操作
	n := 12
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(20)
		log.Printf(colorWhite+"TEST CLIENT perform Put operation, key=%s, value=%s"+colorReset, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}

	// 验证 Get 操作
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(time.Second * 5)
	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	// 关闭一个复制组并验证部分 Get 失败
	log.Printf(colorWhite + "TEST CLIENT shut down group 1" + colorReset)
	cfg.ShutdownGroup(1)
	log.Printf(colorWhite + "TEST CLIENT check forbid snapshots" + colorReset)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		log.Printf(colorWhite+"TEST CLIENT create client %d for Get operation, key=%s"+colorReset, xi, ka[xi])
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			log.Printf(colorWhite+"TEST CLIENT Client %d: perform Get operation, key=%s"+colorReset, i, ka[i])
			v := ck1.Get(ka[i])
			if v != va[i] {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get failed, key=%s, expected=%s, got=%s"+colorReset, i, ka[i], va[i], v)
				errorMsg := fmt.Sprintf("Get(%v): expected:%v, received:%v", ka[i], va[i], v)
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get failed, %s\n", i, errorMsg)
				ch <- "error"
			} else {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get succeeded, key=%s, value=%s"+colorReset, i, ka[i], v)
				ch <- ka[i]
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	// 统计成功的 Get 操作
	log.Printf(colorWhite + "TEST CLIENT wait for Get operations to complete" + colorReset)
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err == "error" {
				t.Fatal(err)
			}
			ndone += 1
			log.Printf(colorWhite+"TEST CLIENT Completed Get key %s operation, ndone=%d"+colorReset, err, ndone)
		case <-time.After(time.Second * 2):
			log.Printf(colorWhite+"TEST CLIENT Timeout after 2 seconds, ndone=%d"+colorReset, ndone)
			done = true
			break
		}
	}

	if ndone != 9 {
		log.Printf(colorWhite+"TEST CLIENT Expected 8 completions with one shard dead; got %v"+colorReset, ndone)
		t.Fatalf("expected 8 completions with one shard dead; got %v"+colorReset, ndone)
	}
	log.Printf(colorWhite+"TEST CLIENT Verified %d completions with one shard dead"+colorReset, ndone)

	// bring the crashed shard/group back to life.
	// 重启复制组并验证恢复
	log.Printf(colorWhite + "TEST CLIENT start group 1" + colorReset)
	cfg.StartGroup(1)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT After restart, perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	log.Printf(colorWhite + "TEST CLIENT TestStaticShards passed" + colorReset)
	fmt.Printf(colorWhite + "  ... Passed" + colorReset)
}

// test static 2-way sharding, without shard movement.
func TestStaticShardsThree(t *testing.T) {
	fmt.Printf(colorWhite + "Test: static Shards ..." + colorReset)
	log.Printf(colorWhite + "TEST CLIENT Start TestStaticShards" + colorReset)

	log.Printf(colorWhite+"TEST CLIENT Created config with %d servers"+colorReset, 3)
	ngroups := 3
	npergroup := 3
	cfg := make_configmore(t, ngroups, npergroup, false, -1)
	defer cfg.cleanup()

	log.Printf(colorWhite + "TEST CLIENT Created client" + colorReset)
	ck := cfg.makeClient()

	// 加入一个组
	log.Printf(colorWhite + "TEST CLIENT join group 0" + colorReset)
	cfg.join(0)
	log.Printf(colorWhite + "TEST CLIENT waiting for 2s" + colorReset)
	time.Sleep(time.Second * 2)

	// 执行 Put 操作
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(20)
		log.Printf(colorWhite+"TEST CLIENT perform Put operation, key=%s, value=%s"+colorReset, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}

	// 验证 Get 操作
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	time.Sleep(time.Second * 5)
	// make sure that the data really is sharded by
	// shutting down one shard and checking that some
	// Get()s don't succeed.
	log.Printf(colorWhite + "TEST CLIENT check forbid snapshots" + colorReset)
	cfg.checklogs() // forbid snapshots

	ch := make(chan string)
	for xi := 0; xi < n; xi++ {
		log.Printf(colorWhite+"TEST CLIENT create client %d for Get operation, key=%s"+colorReset, xi, ka[xi])
		ck1 := cfg.makeClient() // only one call allowed per client
		go func(i int) {
			log.Printf(colorWhite+"TEST CLIENT Client %d: perform Get operation, key=%s"+colorReset, i, ka[i])
			v := ck1.Get(ka[i])
			if v != va[i] {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get failed, key=%s, expected=%s, got=%s"+colorReset, i, ka[i], va[i], v)
				errorMsg := fmt.Sprintf("Get(%v): expected:%v, received:%v", ka[i], va[i], v)
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get failed, %s"+colorReset, i, errorMsg)
				ch <- "error"
			} else {
				log.Printf(colorWhite+"TEST CLIENT Client %d: Get succeeded, key=%s, value=%s"+colorReset, i, ka[i], v)
				ch <- ka[i]
			}
		}(xi)
	}

	// wait a bit, only about half the Gets should succeed.
	// 统计成功的 Get 操作
	log.Printf(colorWhite + "TEST CLIENT wait for Get operations to complete" + colorReset)
	ndone := 0
	done := false
	for done == false {
		select {
		case err := <-ch:
			if err == "error" {
				t.Fatal(err)
			}
			ndone += 1
			log.Printf(colorWhite+"TEST CLIENT Completed Get key %s operation, ndone=%d"+colorReset, err, ndone)
		case <-time.After(time.Second * 2):
			log.Printf(colorWhite+"TEST CLIENT Timeout after 2 seconds, ndone=%d"+colorReset, ndone)
			done = true
			break
		}
	}

	if ndone != 10 {
		log.Printf(colorWhite+"TEST CLIENT Expected 8 completions with one shard dead; got %v"+colorReset, ndone)
		t.Fatalf(colorWhite+"expected 8 completions with one shard dead; got %v"+colorReset, ndone)
	}
	log.Printf(colorWhite+"TEST CLIENT Verified %d completions with one shard dead"+colorReset, ndone)

	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT After restart, perform Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	log.Printf(colorWhite + "TEST CLIENT TestStaticShards passed" + colorReset)
	fmt.Printf(colorWhite + "  ... Passed" + colorReset)
}

func TestJoinLeave(t *testing.T) {
	fmt.Printf(colorWhite + "Test: join then leave ..." + colorReset)
	log.Printf(colorWhite + "TEST CLIENT Start TestJoinLeave" + colorReset)
	stage := 0
	// 设置测试环境
	log.Printf(colorWhite+"TEST CLIENT Created config with %d servers"+colorReset, 3)
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	log.Printf(colorWhite + "TEST CLIENT Created client" + colorReset)
	ck := cfg.makeClient()

	// 加入第一个复制组
	stage = 1
	log.Printf(colorWhite+"TEST CLIENT stage %d: join group 0"+colorReset, stage)
	cfg.join(0)

	// 执行 Put 操作
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Put operation, key=%s, value=%s"+colorReset, stage, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}

	// 验证 Get 操作
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Get operation, key=%s, expected value=%s"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	// 加入第二个复制组
	stage = 2
	log.Printf(colorWhite+"TEST CLIENT stage %d: join group 1"+colorReset, stage)
	cfg.join(1)

	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Get operation after join, key=%s, expected value=%s"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Append operation, key=%s, value=%s"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}

	stage = 3
	log.Printf(colorWhite+"TEST CLIENT stage %d: leave group 0"+colorReset, stage)
	cfg.leave(0)

	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Get operation after leave, key=%s, expected value=%s"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d: perform Append operation after leave, key=%s, value=%s"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}

	// allow time for Shards to transfer.
	log.Printf(colorWhite + "TEST CLIENT wait for Shards to transfer" + colorReset)
	time.Sleep(1 * time.Second)

	// 检查日志和关闭组
	log.Printf(colorWhite + "TEST CLIENT check logs" + colorReset)
	cfg.checklogs()
	log.Printf(colorWhite + "TEST CLIENT shut down group 0" + colorReset)
	cfg.ShutdownGroup(0)

	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT perform final Get operation, key=%s, expected value=%s"+colorReset, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}

	log.Printf(colorWhite + "TEST CLIENT TestJoinLeave passed" + colorReset)
	fmt.Printf("  ... Passed")
}

func TestBatchOperationsSlow(t *testing.T) {
	fmt.Printf("Test: Batch Put and Append operations ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestBatchOperations" + colorReset)
	stage := 0

	// 1. 设置测试环境
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 servers per group"+colorReset, stage)
	cfg := make_config(t, 3, false, -1) // 假设每个组3个服务器
	defer cfg.cleanup()

	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating client"+colorReset, stage)
	ck := cfg.makeClient()

	// 2. 加入复制组，确保有多个分片组参与
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)
	time.Sleep(1 * time.Second) // 等待配置传播和Leader选举

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)
	time.Sleep(2 * time.Second) // 等待分片重新均衡

	// 3. 测试 PutBatch
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Testing PutBatch operation"+colorReset, stage)
	putBatchData := make(map[string]string)
	expectedValues := make(map[string]string)
	numBatchPuts := 15 // 测试15个键，确保能分散到不同分片和组

	for i := 0; i < numBatchPuts; i++ {
		key := strconv.Itoa(i)
		value := randstring(10) // 随机值
		putBatchData[key] = value
		expectedValues[key] = value // 记录期望值
		log.Printf(colorWhite+"TEST CLIENT stage %d: Preparing PutBatch - Key '%s', Value '%s'"+colorReset, stage, key, value)
	}

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing PutBatch with %d items"+colorReset, stage, len(putBatchData))
	putBatchErr := ck.PutBatch(putBatchData)
	if putBatchErr != OK { // 假设 OK 是您定义的成功 Err 值，或者用 nil error 判断
		t.Fatalf("PutBatch failed: %v", putBatchErr)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch completed."+colorReset, stage)

	// 验证 PutBatch 结果
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying PutBatch results"+colorReset, stage)
	for key, val := range expectedValues {
		check(t, ck, key, val)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch results verified."+colorReset, stage)

	// 4. 测试 AppendBatch
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Testing AppendBatch operation"+colorReset, stage)
	appendBatchData := make(map[string]string)
	numBatchAppends := 15 // 对所有之前的键和一些新键进行Append

	for i := 0; i < numBatchAppends; i++ {
		key := strconv.Itoa(i)          // 复用之前的键，也可能包含新键
		appendToAppend := randstring(6) // 追加一个短随机字符串
		appendBatchData[key] = appendToAppend

		// 更新期望值
		if originalValue, ok := expectedValues[key]; ok {
			expectedValues[key] = originalValue + appendToAppend
		} else { // 如果是新键，则期望值就是追加的值
			expectedValues[key] = appendToAppend
		}
		log.Printf(colorWhite+"TEST CLIENT stage %d: Preparing AppendBatch - Key '%s', ValueToAppend '%s'. Expected final: '%s'"+colorReset, stage, key, appendToAppend, expectedValues[key])
	}

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing AppendBatch with %d items"+colorReset, stage, len(appendBatchData))
	appendBatchErr := ck.AppendBatch(appendBatchData)
	if appendBatchErr != OK {
		t.Fatalf("AppendBatch failed: %v", appendBatchErr)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: AppendBatch completed."+colorReset, stage)

	// 验证 AppendBatch 结果
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying AppendBatch results"+colorReset, stage)
	for keyToVerify := range appendBatchData { // 只验证我们Append过的键
		check(t, ck, keyToVerify, expectedValues[keyToVerify])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: AppendBatch results verified."+colorReset, stage)

	// 5. （可选）模拟配置变更后再次测试
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.leave(0) // 让 GID 100 离开，其分片会迁移到 GID 101 (如果只有这两个组)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for shards to re-transfer after leave (e.g., 5 seconds)"+colorReset, stage)
	time.Sleep(5 * time.Second) // 给足够时间让分片迁移完成

	// 再次验证所有键（现在应该都由 GID 101 服务，如果它没离开的话）
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying all data after group 0 left"+colorReset, stage)
	for keyToVerify := range expectedValues {
		check(t, ck, keyToVerify, expectedValues[keyToVerify])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification after leave completed."+colorReset, stage)

	// 可以再执行一次 PutBatch 或 AppendBatch
	anotherPutBatch := make(map[string]string)
	keyForReconfigPut := strconv.Itoa(numBatchPuts) // 一个新键
	valForReconfigPut := randstring(7)
	anotherPutBatch[keyForReconfigPut] = valForReconfigPut
	expectedValues[keyForReconfigPut] = valForReconfigPut

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing PutBatch after group 0 left."+colorReset, stage)
	putBatchErrAfterLeave := ck.PutBatch(anotherPutBatch)
	if putBatchErrAfterLeave != OK {
		t.Fatalf(colorWhite+"PutBatch after leave failed: %v"+colorReset, putBatchErrAfterLeave)
	}
	check(t, ck, keyForReconfigPut, valForReconfigPut) // 验证新写入的
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch after leave verified."+colorReset, stage)

	log.Printf(colorWhite + "TEST CLIENT: TestBatchOperations PASSED" + colorReset)
	fmt.Printf("  ... TestBatchOperations Passed\n")
}

func TestBatchOperationsFast(t *testing.T) {
	fmt.Printf("Test: Batch Put and Append operations ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestBatchOperations" + colorReset)
	stage := 0

	// 1. 设置测试环境
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 servers per group"+colorReset, stage)
	cfg := make_config(t, 3, false, -1) // 假设每个组3个服务器
	defer cfg.cleanup()

	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating client"+colorReset, stage)
	ck := cfg.makeClient()

	// 2. 加入复制组，确保有多个分片组参与
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)

	// 3. 测试 PutBatch
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Testing PutBatch operation"+colorReset, stage)
	putBatchData := make(map[string]string)
	expectedValues := make(map[string]string)
	numBatchPuts := 15 // 测试15个键，确保能分散到不同分片和组

	for i := 0; i < numBatchPuts; i++ {
		key := strconv.Itoa(i)
		value := randstring(10) // 随机值
		putBatchData[key] = value
		expectedValues[key] = value // 记录期望值
		log.Printf(colorWhite+"TEST CLIENT stage %d: Preparing PutBatch - Key '%s', Value '%s'"+colorReset, stage, key, value)
	}

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing PutBatch with %d items"+colorReset, stage, len(putBatchData))
	putBatchErr := ck.PutBatch(putBatchData)
	if putBatchErr != OK { // 假设 OK 是您定义的成功 Err 值，或者用 nil error 判断
		t.Fatalf("PutBatch failed: %v", putBatchErr)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch completed."+colorReset, stage)

	// 验证 PutBatch 结果
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying PutBatch results"+colorReset, stage)
	for key, val := range expectedValues {
		check(t, ck, key, val)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch results verified."+colorReset, stage)

	// 4. 测试 AppendBatch
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Testing AppendBatch operation"+colorReset, stage)
	appendBatchData := make(map[string]string)
	numBatchAppends := 15 // 对所有之前的键和一些新键进行Append

	for i := 0; i < numBatchAppends; i++ {
		key := strconv.Itoa(i)          // 复用之前的键，也可能包含新键
		appendToAppend := randstring(6) // 追加一个短随机字符串
		appendBatchData[key] = appendToAppend

		// 更新期望值
		if originalValue, ok := expectedValues[key]; ok {
			expectedValues[key] = originalValue + appendToAppend
		} else { // 如果是新键，则期望值就是追加的值
			expectedValues[key] = appendToAppend
		}
		log.Printf(colorWhite+"TEST CLIENT stage %d: Preparing AppendBatch - Key '%s', ValueToAppend '%s'. Expected final: '%s'"+colorReset, stage, key, appendToAppend, expectedValues[key])
	}

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing AppendBatch with %d items"+colorReset, stage, len(appendBatchData))
	appendBatchErr := ck.AppendBatch(appendBatchData)
	if appendBatchErr != OK {
		t.Fatalf("AppendBatch failed: %v", appendBatchErr)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: AppendBatch completed."+colorReset, stage)

	// 验证 AppendBatch 结果
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying AppendBatch results"+colorReset, stage)
	for keyToVerify := range appendBatchData { // 只验证我们Append过的键
		check(t, ck, keyToVerify, expectedValues[keyToVerify])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: AppendBatch results verified."+colorReset, stage)

	// 5. （可选）模拟配置变更后再次测试
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.leave(0) // 让 GID 100 离开，其分片会迁移到 GID 101 (如果只有这两个组)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for shards to re-transfer after leave (e.g., 5 seconds)"+colorReset, stage)
	time.Sleep(5 * time.Second) // 给足够时间让分片迁移完成

	// 再次验证所有键（现在应该都由 GID 101 服务，如果它没离开的话）
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying all data after group 0 left"+colorReset, stage)
	for keyToVerify := range expectedValues {
		check(t, ck, keyToVerify, expectedValues[keyToVerify])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification after leave completed."+colorReset, stage)

	// 可以再执行一次 PutBatch 或 AppendBatch
	anotherPutBatch := make(map[string]string)
	keyForReconfigPut := strconv.Itoa(numBatchPuts) // 一个新键
	valForReconfigPut := randstring(7)
	anotherPutBatch[keyForReconfigPut] = valForReconfigPut
	expectedValues[keyForReconfigPut] = valForReconfigPut

	log.Printf(colorWhite+"TEST CLIENT stage %d: Executing PutBatch after group 0 left."+colorReset, stage)
	putBatchErrAfterLeave := ck.PutBatch(anotherPutBatch)
	if putBatchErrAfterLeave != OK {
		t.Fatalf("PutBatch after leave failed: %v", putBatchErrAfterLeave)
	}
	check(t, ck, keyForReconfigPut, valForReconfigPut) // 验证新写入的
	log.Printf(colorWhite+"TEST CLIENT stage %d: PutBatch after leave verified."+colorReset, stage)

	log.Printf(colorWhite + "TEST CLIENT: TestBatchOperations PASSED" + colorReset)
	fmt.Printf("  ... TestBatchOperations Passed\n")
}

func TestSnapshot(t *testing.T) {
	fmt.Printf("Test: snapshots, join, and leave ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestSnapshot" + colorReset) // 测试开始标记
	stage := 0

	// --- 阶段 1: 初始化测试环境 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 servers per group and maxraftstate=1000"+colorReset, stage)
	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating client"+colorReset, stage)
	ck := cfg.makeClient()

	// --- 阶段 2: 第一个组加入并进行初始数据写入 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)

	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, stage, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying initial %d Put operations"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get operation - Key '%s', Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations verified."+colorReset, stage)

	// --- 阶段 3: 其他组加入，触发分片迁移 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, stage, cfg.groups[2].gid)
	cfg.join(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing Appends after group 0 left"+colorReset, stage)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	// --- 阶段 4: 更多组的变动，触发进一步迁移 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.leave(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0) // 组0重新加入，分片会再次均衡
	log.Printf(colorWhite+"TEST CLIENT stage %d: Allowing time for shard transfers after group 1 left and group 0 rejoined (e.g. 2 seconds)"+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing Appends after group 1 left and group 0 rejoined"+colorReset, stage)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second for system to stabilize further."+colorReset, stage)
	time.Sleep(1 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Final verification of all keys before log check"+colorReset, stage)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second before checking logs (for snapshotting)."+colorReset, stage)
	time.Sleep(1 * time.Second)

	// --- 阶段 5: 检查日志大小（间接测试快照）并模拟故障恢复 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Checking server logs for Raft state size (snapshot test). Maxraftstate: %d"+colorReset, stage, cfg.maxraftstate)
	cfg.checklogs()

	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down all groups (0, 1, 2)"+colorReset, stage)
	cfg.ShutdownGroup(0)
	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: All groups shut down."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting all groups (0, 1, 2) again to test recovery."+colorReset, stage)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: All groups restarted."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying all data after restarting all groups"+colorReset, stage)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification after restart completed."+colorReset, stage)

	log.Printf(colorWhite + "TEST CLIENT: TestSnapshot PASSED" + colorReset)
	fmt.Printf("  ... Passed\n")
}

func TestMissChange(t *testing.T) {
	fmt.Printf("Test: servers miss configuration changes...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestMissChange" + colorReset) // Test start marker
	stage := 0

	// --- 阶段 1: 初始化测试环境 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, and assuming default servers per group, maxraftstate=1000"+colorReset, stage)
	cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating client"+colorReset, stage)
	ck := cfg.makeClient()

	// --- 阶段 2: 第一个组加入并进行初始数据写入 ---
	stage++
	// Assuming cfg.groups is populated by make_config and GIDs are accessible.
	// If cfg.groups[X].gid is not available or causes a panic, replace with a placeholder or groupIndex.
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, stage, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying initial %d Put operations"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get operation - Key '%s', Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations verified."+colorReset, stage)

	// --- 阶段 3: 第二个组加入，然后关闭部分服务器 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down server 0 in group 0, server 0 in group 1, and server 0 in group 2"+colorReset, stage)
	cfg.ShutdownServer(0, 0) // Shutdown server 0 of group 0
	cfg.ShutdownServer(1, 0) // Shutdown server 0 of group 1
	cfg.ShutdownServer(2, 0) // Shutdown server 0 of group 2
	log.Printf(colorWhite+"TEST CLIENT stage %d: Specified servers shut down."+colorReset, stage)

	// --- 阶段 4: 在部分服务器关闭的情况下进行组变动和数据操作 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, stage, cfg.groups[2].gid)
	cfg.join(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.leave(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing %d Appends while some servers are down"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	// --- 阶段 5: 一个组重新加入，继续数据操作 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing %d Appends after group 1 rejoined"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	// --- 阶段 6: 重启之前关闭的服务器 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting server 0 in group 0, server 0 in group 1, and server 0 in group 2"+colorReset, stage)
	cfg.StartServer(0, 0)
	cfg.StartServer(1, 0)
	cfg.StartServer(2, 0)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Servers restarted."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing %d Appends after restarting servers"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	// --- 阶段 7: 等待系统稳定 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 2 seconds for system to stabilize."+colorReset, stage)
	time.Sleep(2 * time.Second)

	// --- 阶段 8: 关闭另一批服务器 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down server 1 in group 0, server 1 in group 1, and server 1 in group 2"+colorReset, stage)
	cfg.ShutdownServer(0, 1) // Shutdown server 1 of group 0
	cfg.ShutdownServer(1, 1) // Shutdown server 1 of group 1
	cfg.ShutdownServer(2, 1) // Shutdown server 1 of group 2
	log.Printf(colorWhite+"TEST CLIENT stage %d: Second set of specified servers shut down."+colorReset, stage)

	// --- 阶段 9: 在另一批服务器关闭的情况下进行组变动和数据操作 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d)"+colorReset, stage, cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying data and performing %d Appends while another set of servers are down"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(20)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, stage, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Data verification and Appends completed."+colorReset, stage)

	// --- 阶段 10: 重启第二批关闭的服务器 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting server 1 in group 0, server 1 in group 1, and server 1 in group 2"+colorReset, stage)
	cfg.StartServer(0, 1)
	cfg.StartServer(1, 1)
	cfg.StartServer(2, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Second set of servers restarted."+colorReset, stage)

	// --- 阶段 11: 最终数据验证 ---
	stage++
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final verification of all %d keys"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get Key '%s' Expected '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, stage)

	log.Printf(colorWhite + "TEST CLIENT: TestMissChange PASSED" + colorReset)
	fmt.Printf("  ... Passed\n")
}

func TestConcurrent1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestConcurrent1" + colorReset) // 测试开始标记
	var stage int32
	stage = 0 // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, not unreliable, maxraftstate=100"+colorReset, stage)
	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, stage)
	ck := cfg.makeClient()

	// --- 阶段 2: 加入初始组并进行数据初始化 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, stage, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, stage, ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, stage)

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		localValue := va[i] // Goroutine 本地跟踪的value副本，避免直接修改共享的va[keyIndex]导致race
		currentStage := atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentStage, ka[i])
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			currentStage = atomic.LoadInt32(&stage)
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s'"+colorReset, currentStage, ka[i], x)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
		currentStage = atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations. Final local value: %s"+colorReset, currentStage, ka[i], localValue)
	}

	// --- 阶段 3: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, stage, n)
	for i := 0; i < n; i++ {
		go ff(i)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, stage)

	// --- 阶段 4: 在并发 Append 运行时执行一系列配置变更 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing configuration changes while Appends are running."+colorReset, stage)
	time.Sleep(150 * time.Millisecond) // 等待 Append 操作运行一段时间
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1) // 组1加入

	time.Sleep(500 * time.Millisecond) // 操作间的等待
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, stage, cfg.groups[2].gid)
	cfg.join(2) // 组2加入

	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.leave(0) // 组0离开

	// --- 阶段 5: 模拟服务组故障和恢复 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Simulating group shutdowns."+colorReset, stage)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 0 (GID %d)"+colorReset, stage, cfg.groups[0].gid)
	cfg.ShutdownGroup(0) // 关闭组0
	time.Sleep(100 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.ShutdownGroup(1) // 关闭组1
	time.Sleep(100 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 2 (GID %d)"+colorReset, stage, cfg.groups[2].gid)
	cfg.ShutdownGroup(2) // 关闭组2
	log.Printf(colorWhite+"TEST CLIENT stage %d: All groups shut down."+colorReset, stage)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d) while it's down (testing config change robustness)"+colorReset, stage, cfg.groups[2].gid)
	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting all groups again."+colorReset, stage)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: All groups restarted."+colorReset, stage)

	// --- 阶段 6: 更多配置变更 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing more configuration changes."+colorReset, stage)
	time.Sleep(100 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, stage, cfg.groups[0].gid)
	cfg.join(0) // 组0重新加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, stage, cfg.groups[1].gid)
	cfg.leave(1) // 组1离开

	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, stage, cfg.groups[1].gid)
	cfg.join(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Configuration changes completed."+colorReset, stage)

	// --- 阶段 7: 停止并发 Append 操作并等待完成 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 1 second for system to stabilize and Appends to continue."+colorReset, stage)
	time.Sleep(1 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, stage)
	atomic.StoreInt32(&done, 1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, stage, n)
	for i := 0; i < n; i++ {
		<-ch
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, stage)

	// --- 阶段 8: 最终数据验证 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys."+colorReset, stage, n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, stage, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, stage)

	log.Printf(colorWhite + "TEST CLIENT: TestConcurrent1 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentOriginal1(t *testing.T) {
	fmt.Printf("Test: concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(10 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)

	cfg.ShutdownGroup(0)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(1)
	time.Sleep(100 * time.Millisecond)
	cfg.ShutdownGroup(2)

	cfg.leave(2)

	time.Sleep(100 * time.Millisecond)
	cfg.StartGroup(0)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(100 * time.Millisecond)
	cfg.join(0)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)

	time.Sleep(1 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
func TestConcurrent2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestConcurrent2" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0

	// --- 阶段 1: 初始化测试环境和客户端 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, not unreliable, maxraftstate=-1 (no snapshots)"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, false, -1)
	// cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组加入操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial group joins completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 进行初始数据写入 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		currentLocalStage := atomic.LoadInt32(&stage) // 读取 goroutine 启动时（或首次循环时）的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentLocalStage, ka[i])
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			currentLoopStage := atomic.LoadInt32(&stage) // 每次循环前读取当前 stage
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s' (goroutine for Key '%s')"+colorReset, currentLoopStage, ka[i], x, ka[i])
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
		currentLocalStage = atomic.LoadInt32(&stage) // 读取 goroutine 结束时的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations."+colorReset, currentLocalStage, ka[i])
	}

	// --- 阶段 4: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 在并发 Append 运行时执行一系列配置变更 (Part 1) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 6: 在并发 Append 运行时执行一系列配置变更 (Part 2) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after more joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 7: 模拟服务组故障和恢复 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Simulating group shutdowns."+colorReset, atomic.LoadInt32(&stage))
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.ShutdownGroup(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.ShutdownGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Specified groups shut down."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 1 second after shutdowns."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(1000 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting group 1 (GID %d) again."+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.StartGroup(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting group 2 (GID %d) again."+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.StartGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Specified groups restarted."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 8: 等待系统稳定并停止并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize further."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(2 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 9: 最终数据验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite + "TEST CLIENT: TestConcurrent2 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentRestart2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestConcurrent2" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0

	// --- 阶段 1: 初始化测试环境和客户端 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, not unreliable, maxraftstate=-1 (no snapshots)"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, false, -1)
	// cfg := make_config(t, 3, false, 1000)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组加入操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial group joins completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 进行初始数据写入 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		currentLocalStage := atomic.LoadInt32(&stage) // 读取 goroutine 启动时（或首次循环时）的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentLocalStage, ka[i])
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			currentLoopStage := atomic.LoadInt32(&stage) // 每次循环前读取当前 stage
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s' (goroutine for Key '%s')"+colorReset, currentLoopStage, ka[i], x, ka[i])
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
		currentLocalStage = atomic.LoadInt32(&stage) // 读取 goroutine 结束时的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations."+colorReset, currentLocalStage, ka[i])
	}

	// --- 阶段 4: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 在并发 Append 运行时执行一系列配置变更 (Part 1) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 6: 在并发 Append 运行时执行一系列配置变更 (Part 2) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after more joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 7: 等待系统稳定并停止并发 Append 操作 ---
	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1)

	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize further."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(4 * time.Second)

	// --- 阶段 8: 模拟服务组故障和恢复 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Simulating group shutdowns."+colorReset, atomic.LoadInt32(&stage))
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.ShutdownGroup(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.ShutdownGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Specified groups shut down."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 1 second after shutdowns."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(1000 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting group 1 (GID %d) again."+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.StartGroup(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting group 2 (GID %d) again."+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.StartGroup(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Specified groups restarted."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 9: 等待系统稳定并停止并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 9 seconds for system to stabilize further."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(4 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 10: 最终数据验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite + "TEST CLIENT: TestConcurrent2 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

func TestConcurrentNoShutdown2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestConcurrent2" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0

	// --- 阶段 1: 初始化测试环境和客户端 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, not unreliable, maxraftstate=-1 (no snapshots)"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组加入操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial group joins completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 进行初始数据写入 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		currentLocalStage := atomic.LoadInt32(&stage) // 读取 goroutine 启动时（或首次循环时）的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentLocalStage, ka[i])
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			currentLoopStage := atomic.LoadInt32(&stage) // 每次循环前读取当前 stage
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s' (goroutine for Key '%s')"+colorReset, currentLoopStage, ka[i], x, ka[i])
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
		currentLocalStage = atomic.LoadInt32(&stage) // 读取 goroutine 结束时的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations."+colorReset, currentLocalStage, ka[i])
	}

	// --- 阶段 4: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 在并发 Append 运行时执行一系列配置变更 (Part 1) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 6: 在并发 Append 运行时执行一系列配置变更 (Part 2) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 2 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.leave(2)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 3 seconds after more joins/leaves."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(3000 * time.Millisecond)

	// --- 阶段 7: 模拟服务组故障和恢复 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 1 second after shutdowns."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(1000 * time.Millisecond)

	// --- 阶段 8: 等待系统稳定并停止并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize further."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(2 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 9: 最终数据验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite + "TEST CLIENT: TestConcurrent2 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

// this tests the various sources from which a re-starting
// group might need to fetch shard contents.
func TestConcurrentOriginal2(t *testing.T) {
	fmt.Printf("Test: more concurrent puts and configuration changes...\n")

	cfg := make_config(t, 3, false, -1)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(1)
	cfg.join(0)
	cfg.join(2)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple shards
		va[i] = randstring(1)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			ck1.Append(ka[i], x)
			va[i] += x
			time.Sleep(50 * time.Millisecond)
		}
	}

	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}

	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(0)
	cfg.join(2)
	cfg.leave(1)
	time.Sleep(3000 * time.Millisecond)
	cfg.join(1)
	cfg.leave(0)
	cfg.leave(2)
	time.Sleep(3000 * time.Millisecond)

	cfg.ShutdownGroup(1)
	cfg.ShutdownGroup(2)
	time.Sleep(1000 * time.Millisecond)
	cfg.StartGroup(1)
	cfg.StartGroup(2)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestConcurrent3(t *testing.T) {
	fmt.Printf("Test: concurrent configuration change and restart...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestConcurrent3" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0

	// --- 阶段 1: 初始化测试环境和客户端 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, not unreliable, maxraftstate=300"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, false, 300)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 加入初始组并进行数据初始化 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	var done int32
	ch := make(chan bool)

	ff := func(i int, ck1 *Clerk) {
		defer func() { ch <- true }()
		currentLocalStage := atomic.LoadInt32(&stage) // 如果需要记录启动时的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentLocalStage, ka[i])
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(1)
			currentLoopStage := atomic.LoadInt32(&stage) // 如果需要在每次循环打印实时 stage
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s' (goroutine for Key '%s')"+colorReset, currentLoopStage, ka[i], x, ka[i])
			ck1.Append(ka[i], x)
			va[i] += x
		}
		currentLocalStage = atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations."+colorReset, currentLocalStage, ka[i])
	}

	// --- 阶段 3: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ck1 := cfg.makeClient()
		go ff(i, ck1)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 在并发 Append 运行时执行长时间的、随机的配置变更和组重启循环 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting long loop of concurrent configuration changes and group restarts (approx 12 seconds)."+colorReset, atomic.LoadInt32(&stage))
	t0 := time.Now()
	loopIteration := 0
	for time.Since(t0) < 12*time.Second {
		loopIteration++
		currentLoopStage := atomic.LoadInt32(&stage) // 获取当前 stage 值用于日志
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Iteration started."+colorReset, currentLoopStage, loopIteration)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Joining group 2 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[2].gid)
		cfg.join(2)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Joining group 1 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[1].gid)
		cfg.join(1)
		sleepDuration1 := time.Duration(rand.Int()%900) * time.Millisecond // 随机休眠 0-899毫秒
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Sleeping for %v."+colorReset, currentLoopStage, loopIteration, sleepDuration1)
		time.Sleep(sleepDuration1)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Shutting down group 0 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[0].gid)
		cfg.ShutdownGroup(0)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Shutting down group 1 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[1].gid)
		cfg.ShutdownGroup(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Shutting down group 2 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[2].gid)
		cfg.ShutdownGroup(2)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): All groups shut down."+colorReset, currentLoopStage, loopIteration)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Starting group 0 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[0].gid)
		cfg.StartGroup(0)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Starting group 1 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[1].gid)
		cfg.StartGroup(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Starting group 2 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[2].gid)
		cfg.StartGroup(2)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): All groups restarted."+colorReset, currentLoopStage, loopIteration)

		sleepDuration2 := time.Duration(rand.Int()%900) * time.Millisecond
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Sleeping for %v."+colorReset, currentLoopStage, loopIteration, sleepDuration2)
		time.Sleep(sleepDuration2)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Leaving group 1 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[1].gid)
		cfg.leave(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Leaving group 2 (GID %d)"+colorReset, currentLoopStage, loopIteration, cfg.groups[2].gid)
		cfg.leave(2)

		sleepDuration3 := time.Duration(rand.Int()%900) * time.Millisecond
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Sleeping for %v."+colorReset, currentLoopStage, loopIteration, sleepDuration3)
		time.Sleep(sleepDuration3)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Loop %d): Iteration finished."+colorReset, currentLoopStage, loopIteration)
		// 每次循环内部不改变 stage，stage 的递增主要标记测试的大阶段
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Finished long loop of concurrent changes."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 等待系统稳定并停止并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize after intensive loop."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(2 * time.Second)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1) // 设置 'done' 标志位为1，通知 goroutine 停止

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 6: 最终数据验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite + "TEST CLIENT: TestConcurrent3 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

func TestUnreliable1(t *testing.T) {
	fmt.Printf("Test: unreliable 1...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestUnreliable1" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0                                                                  // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (特别注意：网络设置为不可靠) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, UNRELIABLE network, maxraftstate=100"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组加入并写入初始数据 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 执行第一轮配置变更，并在不可靠网络下进行读写 (Check 和 Append) 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting first round of configuration changes."+colorReset, atomic.LoadInt32(&stage))
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2) // 组2加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0) // 组0离开
	log.Printf(colorWhite+"TEST CLIENT stage %d: Configuration changes completed. Now performing %d checks and appends under UNRELIABLE network."+colorReset, atomic.LoadInt32(&stage), n*2)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		currentOpStage := atomic.LoadInt32(&stage) // 获取当前 stage 值用于日志
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d/%d): Checking Key '%s', Expected '%s'"+colorReset, currentOpStage, ii+1, n*2, ka[i], va[i])
		check(t, ck, ka[i], va[i])
		x := randstring(5)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d/%d): Appending Key '%s', Value '%s'"+colorReset, currentOpStage, ii+1, n*2, ka[i], x)
		ck.Append(ka[i], x)
		va[i] += x
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: First round of checks and appends completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 执行第二轮配置变更，并在不可靠网络下进行只读验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting second round of configuration changes."+colorReset, atomic.LoadInt32(&stage))
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1) // 组1离开
	log.Printf(colorWhite+"TEST CLIENT stage %d: Configuration changes completed. Now performing %d read-only checks under UNRELIABLE network."+colorReset, atomic.LoadInt32(&stage), n*2)

	for ii := 0; ii < n*2; ii++ {
		i := ii % n
		currentOpStage := atomic.LoadInt32(&stage) // 获取当前 stage 值用于日志
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d/%d): Read-only Checking Key '%s', Expected '%s'"+colorReset, currentOpStage, ii+1, n*2, ka[i], va[i])
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Second round of read-only checks completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 测试结束 ---
	atomic.AddInt32(&stage, 1)                                                                                 // 标记测试结束阶段
	log.Printf(colorWhite+"TEST CLIENT stage %d: TestUnreliable1 PASSED"+colorReset, atomic.LoadInt32(&stage)) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

func TestUnreliableOriginal2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(5)
		ck.Put(ka[i], va[i])
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			x := randstring(5)
			ck1.Append(ka[i], x)
			va[i] += x
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	for i := 0; i < n; i++ {
		check(t, ck, ka[i], va[i])
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable2(t *testing.T) {
	fmt.Printf("Test: unreliable 2...\n")                                      // 测试用例的描述信息
	log.Printf(colorWhite + "TEST CLIENT: Start TestUnreliable2" + colorReset) // 测试开始标记
	var stage int32                                                            // 定义原子类型的 stage 计数器
	stage = 0                                                                  // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (特别注意：网络设置为不可靠) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, UNRELIABLE network, maxraftstate=100"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, true, 100) // 第三个参数 'true' 表示网络不可靠
	defer cfg.cleanup()                 // 注册清理函数，在测试结束时执行
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient() // 创建主客户端

	// --- 阶段 2: 初始组加入并写入初始数据 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0) // 组0加入服务

	n := 10                 // 定义操作的键值对数量
	ka := make([]string, n) // 存储键
	va := make([]string, n) // va 用于在客户端侧跟踪期望的最终值
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations"+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // 键名，确保分散到多个 Shard
		va[i] = randstring(5)   // 初始值
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i]) // 执行 Put 操作
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	var done int32           // 原子操作的标志位，用于通知并发 goroutine 停止
	ch := make(chan bool, n) // 用于等待所有并发 goroutine 完成的 channel

	// ff 是并发执行的函数，对指定的键进行持续的 Append 操作
	ff := func(i int) {
		defer func() { ch <- true }() // goroutine 结束时发送信号
		ck1 := cfg.makeClient()       // 每个 goroutine 使用独立的客户端实例
		localValue := va[i]           // 如果需要在 goroutine 本地安全地跟踪累加值，以避免直接修改共享的 va[i]

		currentGoroutineInitialStage := atomic.LoadInt32(&stage) // 读取 goroutine 启动时（或首次循环时）的 stage
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Starting concurrent Append operations."+colorReset, currentGoroutineInitialStage, ka[i])

		for atomic.LoadInt32(&done) == 0 { // 只要 'done' 标志位为0，就继续循环
			x := randstring(5)                           // 生成随机字符串用于 Append
			currentLoopStage := atomic.LoadInt32(&stage) // 如果需要在每次循环打印实时 stage
			log.Printf(colorWhite+"TEST CLIENT stage %d: Append Key '%s', Value '%s' (goroutine for Key '%s')"+colorReset, currentLoopStage, ka[i], x, ka[i])
			ck1.Append(ka[i], x) // 执行 Append 操作
			va[i] += x
		}
		currentGoroutineFinalStage := atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d: goroutine for Key '%s': Finished concurrent Append operations. Final local value: %s"+colorReset, currentGoroutineFinalStage, ka[i], localValue)
	}

	// --- 阶段 3: 启动并发 Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent Append goroutines."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		go ff(i) // 启动 n 个 goroutine
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent Append goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 在并发 Append 运行时，在不可靠网络下执行一系列配置变更 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing configuration changes under UNRELIABLE network while Appends are running."+colorReset, atomic.LoadInt32(&stage))
	log.Printf(colorWhite+"TEST CLIENT stage %d: Sleeping for 150ms."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(150 * time.Millisecond) // 等待 Append 操作运行一段时间

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1) // 组1加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Sleeping for 500ms."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(500 * time.Millisecond) // 操作间的等待

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2) // 组2加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Sleeping for 500ms."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(500 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0) // 组0离开
	log.Printf(colorWhite+"TEST CLIENT stage %d: Sleeping for 500ms."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(500 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1) // 组1离开
	log.Printf(colorWhite+"TEST CLIENT stage %d: Sleeping for 500ms."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(500 * time.Millisecond)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1) // 组1重新加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0) // 组0重新加入
	log.Printf(colorWhite+"TEST CLIENT stage %d: Configuration changes under UNRELIABLE network completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 等待系统稳定，停止并发 Append，将网络恢复为可靠 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize after configuration changes."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(2 * time.Second) // 等待系统在配置变更和不可靠网络影响后稳定

	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent Append goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1) // 设置 'done' 标志位为1，通知 goroutine 停止

	log.Printf(colorWhite+"TEST CLIENT stage %d: Setting network to RELIABLE mode for final checks."+colorReset, atomic.LoadInt32(&stage))
	cfg.net.Reliable(true) // 将网络设置回可靠模式，确保最终检查的 RPC 通信顺畅

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d Append goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch // 等待每个 goroutine 发送完成信号
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All Append goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 6: 在可靠网络下进行最终数据验证 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of %d keys under RELIABLE network."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		// 再次强调，va[i] 的值是在 ff 函数中并发累加的，存在数据竞争的固有风险。
		// 测试的通过可能依赖于特定的执行时序或 Append 操作的幂等性。
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected accumulated value for '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		check(t, ck, ka[i], va[i]) // 验证最终结果
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite + "TEST CLIENT: TestUnreliable2 PASSED" + colorReset) // 测试通过标记
	fmt.Printf("  ... Passed\n")                                                // 测试用例通过的简短输出
}

func TestUnreliableOriginal3(t *testing.T) {
	fmt.Printf("Test: unreliable 3...\n")

	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()

	begin := time.Now()
	var operations []porcupine.Operation
	var opMu sync.Mutex

	ck := cfg.makeClient()

	cfg.join(0)

	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = randstring(5)
		start := int64(time.Since(begin))
		ck.Put(ka[i], va[i])
		end := int64(time.Since(begin))
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op)
	}

	var done int32
	ch := make(chan bool)

	ff := func(i int) {
		defer func() { ch <- true }()
		ck1 := cfg.makeClient()
		for atomic.LoadInt32(&done) == 0 {
			ki := rand.Int() % n
			nv := randstring(5)
			var inp models.KvInput
			var out models.KvOutput
			start := int64(time.Since(begin))
			if (rand.Int() % 1000) < 500 {
				ck1.Append(ka[ki], nv)
				inp = models.KvInput{Op: 2, Key: ka[ki], Value: nv}
			} else if (rand.Int() % 1000) < 100 {
				ck1.Put(ka[ki], nv)
				inp = models.KvInput{Op: 1, Key: ka[ki], Value: nv}
			} else {
				v := ck1.Get(ka[ki])
				inp = models.KvInput{Op: 0, Key: ka[ki]}
				out = models.KvOutput{Value: v}
			}
			end := int64(time.Since(begin))
			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: i}
			opMu.Lock()
			operations = append(operations, op)
			opMu.Unlock()
		}
	}

	for i := 0; i < n; i++ {
		go ff(i)
	}

	time.Sleep(150 * time.Millisecond)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	cfg.join(1)
	cfg.join(0)

	time.Sleep(2 * time.Second)

	atomic.StoreInt32(&done, 1)
	cfg.net.Reliable(true)
	for i := 0; i < n; i++ {
		<-ch
	}

	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)
	if res == porcupine.Illegal {
		file, err := ioutil.TempFile("", "*.html")
		if err != nil {
			fmt.Printf("info: failed to create temp file for visualization")
		} else {
			err = porcupine.Visualize(models.KvModel, info, file)
			if err != nil {
				fmt.Printf("info: failed to write history visualization to %s\n", file.Name())
			} else {
				fmt.Printf("info: wrote history visualization to %s\n", file.Name())
			}
		}
		t.Fatal("history is not linearizable")
	} else if res == porcupine.Unknown {
		fmt.Println("info: linearizability check timed out, assuming history is ok")
	}

	fmt.Printf("  ... Passed\n")
}

func TestUnreliable3(t *testing.T) {
	fmt.Printf("Test: unreliable 3 (linearizability check with Porcupine)...\n") // 测试用例的描述信息
	log.Printf(colorWhite + "TEST CLIENT: Start TestUnreliable3" + colorReset)   // 测试开始标记
	var stage int32                                                              // 定义原子类型的 stage 计数器
	stage = 0                                                                    // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (不可靠网络) 和 Porcupine 相关变量 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, UNRELIABLE network, maxraftstate=100"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, true, 100) // 第三个参数 'true' 表示网络不可靠
	defer cfg.cleanup()                 // 注册清理函数

	begin := time.Now()                  // 记录测试开始时间，用于 Porcupine 操作时间戳
	var operations []porcupine.Operation // 用于存储所有操作记录以供 Porcupine 分析
	var opMu sync.Mutex                  // 用于保护对 'operations' 切片的并发访问
	log.Printf(colorWhite+"TEST CLIENT stage %d: Porcupine operation logging initialized."+colorReset, atomic.LoadInt32(&stage))

	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient() // 创建主客户端

	// --- 阶段 2: 初始组加入并写入初始数据 (同时记录操作供 Porcupine) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0) // 组0加入服务

	n := 10                 // 定义初始 Put 操作的键值对数量，也用作并发 goroutine 数量
	ka := make([]string, n) // 存储键
	va := make([]string, n) // 存储初始 Put 的值 (主要用于记录到 Porcupine)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations and recording for Porcupine."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // 键名
		va[i] = randstring(5)   // 初始值
		log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])

		start := int64(time.Since(begin).Microseconds()) // Porcupine 操作开始时间 (微秒)
		ck.Put(ka[i], va[i])                             // 执行 Put 操作
		end := int64(time.Since(begin).Microseconds())   // Porcupine 操作结束时间 (微秒)

		// 准备 Porcupine 操作记录
		// 假设 models.KvInput Op: 1=Put, 2=Append, 0=Get
		inp := models.KvInput{Op: 1, Key: ka[i], Value: va[i]}
		var out models.KvOutput // Put 操作的 KvOutput 通常为空或不关键
		// ClientId 0 用于表示这些初始的、非并发的设置操作
		op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: 0}
		operations = append(operations, op) // 记录操作 (此时是单线程访问 operations)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed and recorded."+colorReset, atomic.LoadInt32(&stage))

	var done int32           // 原子标志位，通知并发 goroutine 停止
	ch := make(chan bool, n) // Channel 用于等待所有并发 goroutine 完成

	// ff 是并发执行的函数，执行随机的 Get/Put/Append 操作并记录供 Porcupine
	ff := func(clientId int) { // clientId 用于 Porcupine 区分不同的并发客户端历史
		defer func() { ch <- true }() // goroutine 结束时发送信号
		ck1 := cfg.makeClient()       // 每个 goroutine 使用独立的客户端实例

		currentGoroutineInitialStage := atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d: Goroutine ClientId %d for random operations: Starting."+colorReset, currentGoroutineInitialStage, clientId)

		for atomic.LoadInt32(&done) == 0 { // 只要 'done' 未被设置
			ki := rand.Int() % n // 随机选择一个键索引
			key := ka[ki]        // 获取键名
			nv := randstring(5)  // 生成随机值 (用于 Put/Append)

			var inp models.KvInput  // Porcupine 操作输入
			var out models.KvOutput // Porcupine 操作输出
			opTypeForLog := ""      // 用于日志记录的操作类型

			start := int64(time.Since(begin).Microseconds()) // 操作开始时间戳

			randomOpChoice := rand.Int() % 1000 // 生成一个0-999的随机数来决定操作类型
			if randomOpChoice < 500 {           // ~50% 概率执行 Append
				opTypeForLog = "Append"
				ck1.Append(key, nv)
				inp = models.KvInput{Op: 2, Key: key, Value: nv} // 假设 Op=2 是 Append
			} else if randomOpChoice < 600 { // ~10% 概率执行 Put (500-599)
				opTypeForLog = "Put"
				ck1.Put(key, nv)
				inp = models.KvInput{Op: 1, Key: key, Value: nv} // 假设 Op=1 是 Put
			} else { // ~40% 概率执行 Get
				opTypeForLog = "Get"
				v := ck1.Get(key)
				inp = models.KvInput{Op: 0, Key: key} // 假设 Op=0 是 Get
				out = models.KvOutput{Value: v}       // Get 操作的输出是获取到的值
			}
			end := int64(time.Since(begin).Microseconds()) // 操作结束时间戳

			// （可选）详细日志记录每个并发操作
			log.Printf(colorWhite+"TEST CLIENT stage %d: Goroutine ClientId %d executed %s on Key '%s'"+colorReset, atomic.LoadInt32(&stage), clientId, opTypeForLog, key)

			op := porcupine.Operation{Input: inp, Call: start, Output: out, Return: end, ClientId: clientId}
			opMu.Lock() // 加锁以保护对共享 'operations' 切片的并发写入
			operations = append(operations, op)
			opMu.Unlock()
		}
		log.Printf(colorWhite+"TEST CLIENT stage %d: Goroutine ClientId %d: Finished random operations loop."+colorReset, atomic.LoadInt32(&stage), clientId)
	}

	// --- 阶段 3: 启动并发的 Get/Put/Append 操作 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Starting %d concurrent goroutines for random Get/Put/Append operations."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ { // 启动 n 个并发 goroutine
		go ff(i + 1) // ClientId 从 1 开始，以区别于初始设置的 ClientId 0
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Concurrent operation goroutines started."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 在并发操作运行时，在不可靠网络下执行一系列配置变更 ---
	// (这部分配置变更序列与 TestUnreliable2 中的相似)
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing configuration changes under UNRELIABLE network while random operations are running."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(150 * time.Millisecond) // 短暂等待让并发操作开始执行
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)
	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[2].gid)
	cfg.join(2)
	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.leave(0)
	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 1 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.leave(1)
	time.Sleep(500 * time.Millisecond)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[1].gid)
	cfg.join(1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d) again"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Configuration changes under UNRELIABLE network completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 等待系统稳定，停止并发操作，并将网络恢复为可靠 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for 2 seconds for system to stabilize after config changes."+colorReset, atomic.LoadInt32(&stage))
	time.Sleep(2 * time.Second) // 等待系统在配置变更和不可靠网络影响后趋于稳定

	log.Printf(colorWhite+"TEST CLIENT stage %d: Signaling concurrent goroutines to stop."+colorReset, atomic.LoadInt32(&stage))
	atomic.StoreInt32(&done, 1) // 设置 'done' 标志位为1，通知所有 ff goroutine 停止其操作循环

	log.Printf(colorWhite+"TEST CLIENT stage %d: Setting network to RELIABLE mode for final operations and check."+colorReset, atomic.LoadInt32(&stage))
	cfg.net.Reliable(true) // 将网络设置回可靠模式，确保 Porcupine 分析前最后的操作能被稳定记录

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting for all %d concurrent goroutines to complete."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		<-ch // 等待每个 ff goroutine 执行完毕并发送信号
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: All concurrent goroutines completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 6: 使用 Porcupine 对记录的操作历史进行线性一致性检查 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing linearizability check with Porcupine on %d recorded operations."+colorReset, atomic.LoadInt32(&stage), len(operations))
	res, info := porcupine.CheckOperationsVerbose(models.KvModel, operations, linearizabilityCheckTimeout)

	if res == porcupine.Illegal { // 如果历史记录被检测为非法（非线性一致）
		log.Printf(colorWhite+"TEST CLIENT stage %d: Porcupine check result: HISTORY IS NOT LINEARIZABLE."+colorReset, atomic.LoadInt32(&stage))
		// 尝试创建临时文件来保存线性一致性问题的可视化结果
		file, err := ioutil.TempFile("", "linearizability-*.html") // 使用 ioutil.TempFile 或 os.CreateTemp
		if err != nil {
			log.Printf(colorWhite+"TEST CLIENT stage %d: Info: failed to create temp file for Porcupine visualization: %v"+colorReset, atomic.LoadInt32(&stage), err)
		} else {
			defer file.Close()
			err = porcupine.Visualize(models.KvModel, info, file) // 生成可视化HTML文件
			if err != nil {
				log.Printf(colorWhite+"TEST CLIENT stage %d: Info: failed to write Porcupine history visualization to '%s': %v"+colorReset, atomic.LoadInt32(&stage), file.Name(), err)
			} else {
				log.Printf(colorWhite+"TEST CLIENT stage %d: Info: Porcupine history visualization saved to '%s'"+colorReset, atomic.LoadInt32(&stage), file.Name())
			}
		}
		t.Fatalf("History is not linearizable. Check Porcupine visualization if created.") // 测试失败
	} else if res == porcupine.Unknown { // 如果检查因超时等原因未能确定结果
		log.Printf(colorWhite+"TEST CLIENT stage %d: Porcupine check result: UNKNOWN (check timed out). Assuming OK for this test."+colorReset, atomic.LoadInt32(&stage))
		fmt.Println("info: linearizability check timed out, assuming history is ok") // 通常在实验中，超时会作为通过处理
	} else { // porcupine.Ok，历史记录是线性一致的
		log.Printf(colorWhite+"TEST CLIENT stage %d: Porcupine check result: HISTORY IS LINEARIZABLE."+colorReset, atomic.LoadInt32(&stage))
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Linearizability check completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 7: 测试结束 ---
	atomic.AddInt32(&stage, 1)                                                                                 // 标记测试结束阶段
	log.Printf(colorWhite+"TEST CLIENT stage %d: TestUnreliable3 PASSED"+colorReset, atomic.LoadInt32(&stage)) // 测试通过标记
	fmt.Printf("  ... Passed\n")                                                                               // 测试用例通过的简短输出
}

// optional test to see whether servers are deleting
// Shards for which they are no longer responsible.
func TestChallenge1Delete(t *testing.T) {
	fmt.Printf("Test: shard deletion (challenge 1) ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestChallenge1Delete" + colorReset) // 测试开始标记
	var stage int32                                                                 // 定义原子类型的 stage 计数器
	stage = 0                                                                       // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (特别注意：maxraftstate=1 强制频繁快照) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, reliable network, maxraftstate=1 (force frequent snapshots)"+colorReset, atomic.LoadInt32(&stage))
	// maxraftstate=1 意味着几乎每个Raft日志条目应用后都会触发快照，这对于测试分片数据是否被及时清理至关重要
	// "1" means force snapshot after every log entry.
	cfg := make_config(t, 3, false, 1)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组加入并写入大量数据 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0 (GID %d)"+colorReset, atomic.LoadInt32(&stage), cfg.groups[0].gid)
	cfg.join(0)

	// 30,000 bytes of total values.
	n := 30
	ka := make([]string, n)
	va := make([]string, n)
	totalDataSize := 0
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations (each value ~1000 bytes)."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i)
		va[i] = randstring(1000)
		totalDataSize += len(va[i]) // 累积数据大小
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value size %d bytes"+colorReset, atomic.LoadInt32(&stage), ka[i], len(va[i]))
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed. Total value size: %d bytes."+colorReset, atomic.LoadInt32(&stage), totalDataSize)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial checks for first 3 keys."+colorReset, atomic.LoadInt32(&stage))
	for i := 0; i < 3; i++ {
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial checks completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 执行两轮复杂的分片迁移操作，并进行数据校验 ---
	// 这个循环的目的是通过多次 join/leave 操作，使得分片在不同组之间迁移，
	// 以测试服务器是否会在不再负责某个分片后，从其快照中删除该分片的数据。
	for iters := 0; iters < 2; iters++ {
		atomic.AddInt32(&stage, 1) // 为每一轮迁移迭代递增阶段
		currentIterStage := atomic.LoadInt32(&stage)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iteration %d): Starting shard migration sequence."+colorReset, currentIterStage, iters+1)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Joining group 1 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[1].gid)
		cfg.join(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Leaving group 0 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[0].gid)
		cfg.leave(0)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Joining group 2 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[2].gid)
		cfg.join(2)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Sleeping for 3 seconds to allow migrations and snapshotting."+colorReset, currentIterStage, iters+1)
		time.Sleep(3 * time.Second) // 等待分片迁移和由于 maxraftstate=1 导致的频繁快照

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Checking first 3 keys after migrations."+colorReset, currentIterStage, iters+1)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Leaving group 1 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[1].gid)
		cfg.leave(1)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Joining group 0 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[0].gid)
		cfg.join(0)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Leaving group 2 (GID %d)"+colorReset, currentIterStage, iters+1, cfg.groups[2].gid)
		cfg.leave(2)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Sleeping for 3 seconds for further migrations and snapshotting."+colorReset, currentIterStage, iters+1)
		time.Sleep(3 * time.Second)

		log.Printf(colorWhite+"TEST CLIENT stage %d (Iter %d): Checking first 3 keys after more migrations."+colorReset, currentIterStage, iters+1)
		for i := 0; i < 3; i++ {
			check(t, ck, ka[i], va[i])
		}
		log.Printf(colorWhite+"TEST CLIENT stage %d (Iteration %d): Shard migration sequence completed."+colorReset, currentIterStage, iters+1)
	}

	// --- 阶段 4: 最终配置和稳定性检查 ---
	// 确保所有组都加入，分片重新分配并稳定下来。
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Finalizing group configuration (all groups join)."+colorReset, atomic.LoadInt32(&stage))
	cfg.join(1)
	cfg.join(2)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing multiple stability checks for first 3 keys with delays."+colorReset, atomic.LoadInt32(&stage))
	for i := 0; i < 3; i++ { // 进行三次带间隔的检查，确保数据稳定可读
		log.Printf(colorWhite+"TEST CLIENT stage %d (Stability Check %d): Sleeping for 1 second."+colorReset, atomic.LoadInt32(&stage), i+1)
		time.Sleep(1 * time.Second)
		log.Printf(colorWhite+"TEST CLIENT stage %d (Stability Check %d): Checking first 3 keys."+colorReset, atomic.LoadInt32(&stage), i+1)
		for j := 0; j < 3; j++ {
			check(t, ck, ka[j], va[j])
		}
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Stability checks completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 5: 检查持久化状态的总大小 ---
	// 这是测试的核心：验证服务器是否删除了不再负责的分片数据，从而控制了存储膨胀。
	// 由于 maxraftstate=1，快照会非常频繁，因此快照大小能较好地反映当前服务器负责的数据量。
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Calculating total persisted state size (Raft state + Snapshot)."+colorReset, atomic.LoadInt32(&stage))
	total := 0
	for gi := 0; gi < cfg.ngroups; gi++ {
		for i := 0; i < cfg.npergroup; i++ {
			raft := cfg.groups[gi].saved[i].RaftStateSize()
			snap := len(cfg.groups[gi].saved[i].ReadSnapshot())
			raftSize := cfg.groups[gi].saved[i].RaftStateSize()
			snapSize := len(cfg.groups[gi].saved[i].ReadSnapshot())
			log.Printf(colorWhite+"TEST CLIENT stage %d: Group %d Server %d - RaftSize: %d, SnapshotSize: %d"+colorReset, atomic.LoadInt32(&stage), cfg.groups[gi].gid, i, raftSize, snapSize)
			total += raft + snap
		}
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Total persisted size across all servers: %d bytes."+colorReset, atomic.LoadInt32(&stage), total)

	// 27 keys should be stored once.
	// 3 keys should also be stored in client dup tables.
	// everything on 3 replicas.
	// plus slop.
	expected := 15 * (((n - 3) * 1000) + 2*3*1000 + 6000)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Expected maximum total persisted size: %d bytes."+colorReset, atomic.LoadInt32(&stage), expected)
	if total > expected {
		t.Fatalf("snapshot + persisted Raft state are too big: %v > %v\n", total, expected)
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Persisted state size check passed (Total: %d <= Expected: %d)."+colorReset, atomic.LoadInt32(&stage), total, expected)

	// --- 阶段 6: 最终数据完整性验证 ---
	// 在所有操作和大小检查之后，再次确认所有数据仍然正确可读。
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing final verification of all %d keys."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Final Get/Check - Key '%s', Expected Value (size %d bytes)"+colorReset, atomic.LoadInt32(&stage), ka[i], len(va[i]))
		check(t, ck, ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Final data verification completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 7: 测试结束 ---
	atomic.AddInt32(&stage, 1)
	log.Printf(colorWhite+"TEST CLIENT stage %d: TestChallenge1Delete PASSED"+colorReset, atomic.LoadInt32(&stage)) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle
// Shards that are not affected by a config change
// while the config change is underway
func TestChallenge2Unaffected(t *testing.T) {
	fmt.Printf("Test: unaffected shard access (challenge 2) ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestChallenge2Unaffected" + colorReset) // 测试开始标记
	var stage int32                                                                     // 定义原子类型的 stage 计数器
	stage = 0                                                                           // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (不可靠网络) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, UNRELIABLE network, maxraftstate=100"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// --- 阶段 2: 初始组 (group 0, GID 100) 加入并写入初始数据 ---
	// 假设 cfg.groups[0].gid 是 100, cfg.groups[1].gid 是 101 (如此类推)
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 0"+colorReset, atomic.LoadInt32(&stage))
	cfg.join(0)

	// Do a bunch of puts to keys in all Shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations to all shards ."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = "100"
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 3: 组1 (GID 101) 加入，部分分片迁移至组1 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Joining group 1 . Some shards will migrate."+colorReset, atomic.LoadInt32(&stage))
	cfg.join(1)

	// QUERY to find Shards now owned by 101
	log.Printf(colorWhite+"TEST CLIENT stage %d: Querying config to find shards now owned by GID 101."+colorReset, atomic.LoadInt32(&stage))
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[1].gid
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shard ownership by GID 101: %v"+colorReset, atomic.LoadInt32(&stage), owned)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second for migration to GID 101 to complete and clients to see new config."+colorReset, atomic.LoadInt32(&stage))
	// Wait for migration to new config to complete, and for clients to
	// start using this updated config. Gets to any key k such that
	// owned[shard(k)] == true should now be served by group 101.
	<-time.After(1 * time.Second)

	log.Printf(colorWhite+"TEST CLIENT stage %d: Updating keys in shards now owned by GID 101."+colorReset, atomic.LoadInt32(&stage))
	for i := 0; i < n; i++ {
		shardIdxForKeyI := key2shard(ka[i])
		if owned[i] {
			va[i] = "101"
			log.Printf(colorWhite+"TEST CLIENT stage %d: Key '%s' (shard %d) now owned by GID 101. Putting new value '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], shardIdxForKeyI, va[i])
			ck.Put(ka[i], va[i])
		}
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Updates to GID 101 owned shards completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 关闭组0 (GID 100) 并将其从配置中移除 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 0."+colorReset, atomic.LoadInt32(&stage))
	cfg.ShutdownGroup(0)

	// LEAVE 100
	// 101 doesn't get a chance to migrate things previously owned by 100
	log.Printf(colorWhite+"TEST CLIENT stage %d: Leaving group 0. "+colorReset, atomic.LoadInt32(&stage))
	cfg.leave(0)

	// Wait to make sure clients see new config
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second for clients to see new config."+colorReset, atomic.LoadInt32(&stage))
	<-time.After(1 * time.Second)

	// --- 阶段 5: 验证那些由组1 (GID 101) 负责的分片是否仍然正常工作 ---
	// 这是测试的核心：即使组0故障并离开，组1负责的分片应该不受影响。
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying access to shards owned by GID 101 after GID 100 shutdown/leave."+colorReset, atomic.LoadInt32(&stage))
	successfulAccessToGid1Shards := 0
	// And finally: check that gets/puts for 101-owned keys still complete
	for i := 0; i < n; i++ {
		shard := int(ka[i][0]) % 10
		if owned[shard] {
			log.Printf(colorWhite+"TEST CLIENT stage %d: Accessing Key '%s' (shard %d, owned by GID 101). Expected value: '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], shard, va[i])
			check(t, ck, ka[i], va[i])

			newValueSuffix := "-1"
			log.Printf(colorWhite+"TEST CLIENT stage %d: Putting new value for Key '%s' (owned by GID 101): '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], va[i]+newValueSuffix)
			ck.Put(ka[i], va[i]+"-1")

			log.Printf(colorWhite+"TEST CLIENT stage %d: Checking updated value for Key '%s' (owned by GID 101): Expected '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], va[i]+newValueSuffix)
			check(t, ck, ka[i], va[i]+"-1")
			successfulAccessToGid1Shards++
		}
	}
	if successfulAccessToGid1Shards == 0 {
		log.Printf(colorWhite+"TEST CLIENT stage %d: WARNING - No shards were identified as owned by GID 101 for verification. Check sharding logic or test setup."+colorReset, atomic.LoadInt32(&stage))
	} else {
		log.Printf(colorWhite+"TEST CLIENT stage %d: Access verification for GID 101 owned shards completed. Accessed %d keys successfully."+colorReset, atomic.LoadInt32(&stage), successfulAccessToGid1Shards)
	}

	// --- 阶段 6: 测试结束 ---
	atomic.AddInt32(&stage, 1)                                                                                          // 标记测试结束阶段
	log.Printf(colorWhite+"TEST CLIENT stage %d: TestChallenge2Unaffected PASSED"+colorReset, atomic.LoadInt32(&stage)) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}

// optional test to see whether servers can handle operations on Shards that
// have been received as a part of a config migration when the entire migration
// has not yet completed.
func TestChallenge2Partial(t *testing.T) {
	fmt.Printf("Test: partial migration shard access (challenge 2) ...\n")
	log.Printf(colorWhite + "TEST CLIENT: Start TestChallenge2Partial" + colorReset) // 测试开始标记
	var stage int32                                                                  // 定义原子类型的 stage 计数器
	stage = 0                                                                        // 初始化阶段计数器

	// --- 阶段 1: 初始化测试环境 (不可靠网络) ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating config with 3 groups, UNRELIABLE network, maxraftstate=100"+colorReset, atomic.LoadInt32(&stage))
	cfg := make_config(t, 3, true, 100)
	defer cfg.cleanup()
	log.Printf(colorWhite+"TEST CLIENT stage %d: Creating main client"+colorReset, atomic.LoadInt32(&stage))
	ck := cfg.makeClient()

	// JOIN 100 + 101 + 102
	// --- 阶段 2: 初始设置 - 所有组 (0, 1, 2) 同时加入 ---
	// 假设 cfg.groups[0].gid=100, cfg.groups[1].gid=101, cfg.groups[2].gid=102
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	gid0 := cfg.groups[0].gid
	gid1 := cfg.groups[1].gid
	gid2 := cfg.groups[2].gid
	log.Printf(colorWhite+"TEST CLIENT stage %d: Simultaneously joining groups 0 (GID %d), 1 (GID %d), 2 (GID %d)"+colorReset, atomic.LoadInt32(&stage), gid0, gid1, gid2)
	cfg.joinm([]int{0, 1, 2})

	// Give the implementation some time to reconfigure
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second for initial reconfiguration to settle."+colorReset, atomic.LoadInt32(&stage))
	<-time.After(1 * time.Second)

	// --- 阶段 3: 向所有分片写入初始数据 ---
	// 此时数据会根据初始配置（所有组都在）写入到 GID 100, 101, 或 102 中的某一个组
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	// Do a bunch of puts to keys in all Shards
	n := 10
	ka := make([]string, n)
	va := make([]string, n)
	log.Printf(colorWhite+"TEST CLIENT stage %d: Performing initial %d Put operations to various shards."+colorReset, atomic.LoadInt32(&stage), n)
	for i := 0; i < n; i++ {
		ka[i] = strconv.Itoa(i) // ensure multiple Shards
		va[i] = "100"
		log.Printf(colorWhite+"TEST CLIENT stage %d: Put operation - Key '%s', Value '%s'"+colorReset, atomic.LoadInt32(&stage), ka[i], va[i])
		ck.Put(ka[i], va[i])
	}
	log.Printf(colorWhite+"TEST CLIENT stage %d: Initial Put operations completed."+colorReset, atomic.LoadInt32(&stage))

	// --- 阶段 4: 查询并记录初始时由 GID 102 (组2) 负责的分片 ---
	// 这些分片在后续步骤中预计会从 GID 102 成功迁移到 GID 101
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Querying current config to identify shards initially owned by GID %d."+colorReset, atomic.LoadInt32(&stage), gid2)
	// QUERY to find Shards owned by 102
	c := cfg.mck.Query(-1)
	owned := make(map[int]bool, n)
	for s, gid := range c.Shards {
		owned[s] = gid == cfg.groups[2].gid
	}

	// --- 阶段 5: 创建部分迁移场景 - 关闭 GID 100，然后让 GID 100 (已关闭) 和 GID 102 (存活) 同时离开 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Shutting down group 0 (GID %d). This group will not be able to serve migration requests for its shards."+colorReset, atomic.LoadInt32(&stage), gid0)
	cfg.ShutdownGroup(0)

	// LEAVE 100 + 102
	// 101 can get old Shards from 102, but not from 100. 101 should start
	// serving Shards that used to belong to 102 as soon as possible
	log.Printf(colorWhite+"TEST CLIENT stage %d: Issuing leave command for group 0 (GID %d, which is down) and group 2 (GID %d, which is up)."+colorReset, atomic.LoadInt32(&stage), gid0, gid2)
	cfg.leavem([]int{0, 2})

	// Give the implementation some time to start reconfiguration
	// And to migrate 102 -> 101
	log.Printf(colorWhite+"TEST CLIENT stage %d: Waiting 1 second for reconfiguration (e.g., GID %d leaving) to begin and for GID %d to potentially migrate shards from GID %d."+colorReset, atomic.LoadInt32(&stage), gid2, gid1, gid2)
	<-time.After(1 * time.Second)

	// And finally: check that gets/puts for 101-owned keys now complete
	// --- 阶段 6: 验证 GID 101 是否能服务那些（预期从 GID 102 成功迁移过来的）分片 ---
	atomic.AddInt32(&stage, 1) // 递增阶段计数器
	log.Printf(colorWhite+"TEST CLIENT stage %d: Verifying access to shards that should have migrated from GID %d to GID %d (now served by GID %d)."+colorReset, atomic.LoadInt32(&stage), gid2, gid1, gid1)
	for i := 0; i < n; i++ {
		shard := key2shard(ka[i])
		if owned[shard] {
			log.Printf(colorWhite+"TEST CLIENT stage %d: Accessing Key '%s' (shard %d, originally GID %d, expected now on GID %d). Expected original value: '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], shard, gid2, gid1, va[i])
			check(t, ck, ka[i], va[i])

			newValueSuffix := "-2" // 为这个键准备一个新值后缀
			updatedValue := va[i] + newValueSuffix
			log.Printf(colorWhite+"TEST CLIENT stage %d: Key '%s' (shard %d, on GID %d): Putting new value '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], shard, gid1, updatedValue)
			ck.Put(ka[i], va[i]+"-2")

			log.Printf(colorWhite+"TEST CLIENT stage %d: Key '%s' (shard %d, on GID %d): Checking updated value '%s'."+colorReset, atomic.LoadInt32(&stage), ka[i], shard, gid1, updatedValue)
			check(t, ck, ka[i], va[i]+"-2")
		}
	}

	// --- 阶段 7: 测试结束 ---
	atomic.AddInt32(&stage, 1)                                                                                       // 标记测试结束阶段
	log.Printf(colorWhite+"TEST CLIENT stage %d: TestChallenge2Partial PASSED"+colorReset, atomic.LoadInt32(&stage)) // 测试通过标记
	fmt.Printf("  ... Passed\n")
}
