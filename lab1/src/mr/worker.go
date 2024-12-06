package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"math/rand"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// KeyValue
// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Worker
// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// 生成唯一的 worker ID
	workerId := generateWorkerId()
	// 注册 worker
	if !registerWorker(workerId) {
		log.Printf("Failed to register worker %s", workerId)
		return
	}
	// Your worker implementation here.
	for {
		// 请求任务
		task := getTask(workerId)
		if task.Done {
			log.Printf("worker %s exit", workerId)
			break
		}
		if task.TaskType == InvalidPhase {
			// 设置最小和最大睡眠时间（单位：毫秒）
			minSleep := 10
			maxSleep := 1000

			// 生成一个随机延迟，范围在minSleep到maxSleep之间
			sleepDuration := time.Duration(rand.Intn(maxSleep-minSleep+1)+minSleep) * time.Millisecond

			// 随机睡眠
			time.Sleep(sleepDuration)

			continue
		}
		task.Notified = true
		// uncomment to send the Example RPC to the coordinator.
		// CallExample()
		// 根据任务类型执行相应的处理
		if task.InputFile == "" {
			time.Sleep(100 * time.Millisecond)
			continue
		} else {
			switch task.TaskType {
			case MapPhase:
				doMap(workerId, task, mapf)
			case ReducePhase:
				doReduce(workerId, task, reducef)
			default:
				log.Printf("Worker %s received unknown task type: %v", workerId, task.TaskType)
				continue
			}
			// 报告任务完成
			reportTask(workerId, task)
		}
	}
}

// generateWorkerId 生成唯一的 worker ID
func generateWorkerId() string {
	// 使用时间戳和随机数组合生成唯一 ID
	return fmt.Sprintf("worker-%d-%d",
		time.Now().UnixNano(),
		rand.Intn(10000))
}

// registerWorker 向 coordinator 注册新的 worker
func registerWorker(workerId string) bool {
	args := RegisterArgs{WorkerId: workerId}
	reply := RegisterReply{}

	ok := call("Coordinator.RegisterWorker", &args, &reply)
	return ok
}

// 获取任务
func getTask(workerId string) GetTaskReply {
	//log.Printf("worker %s try to get task", workerId)
	args := GetTaskArgs{WorkerId: workerId}
	reply := GetTaskReply{}
	ok := call("Coordinator.GetTask", &args, &reply)
	reply.Notified = true
	if !ok {
		reply.Done = true
	}
	return reply
}

// 执行 Map 任务
func doMap(workerId string, task GetTaskReply, mapf func(string, string) []KeyValue) {
	log.Printf("worker %s do map task %d (TaskType: %v, InputFile: %v, NReduce: %v, NMap: %v, Notified: %v, Done: %v)",
		workerId, task.TaskId, task.TaskType, task.InputFile, task.NReduce, task.NMap, task.Notified, task.Done)
	// 读取输入文件
	content, err := os.ReadFile(task.InputFile)
	if err != nil {
		log.Fatalf("cannot read %v in Map", task.InputFile)
	}

	// 调用用户定义的 map 函数
	kva := mapf(task.InputFile, string(content))

	// 创建 nReduce 个中间文件
	intermediates := make([]*os.File, task.NReduce)
	encoders := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.TaskId, i)
		file, err := os.Create(filename)
		if err != nil {
			log.Fatalf("cannot create %v in Map", filename)
		}
		intermediates[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	// 将 key-value 对写入对应的中间文件
	for _, kv := range kva {
		reduceTask := ihash(kv.Key) % task.NReduce
		err := encoders[reduceTask].Encode(&kv)
		if err != nil {
			log.Fatalf("cannot encode %v", kv)
		}
	}
	// 关闭所有中间文件
	for _, f := range intermediates {
		err := f.Close()
		if err != nil {
			log.Fatalf("cannot close %v in Map", f.Name())
		}
	}
}

// 执行 Reduce 任务
func doReduce(workerId string, task GetTaskReply, reducef func(string, []string) string) {
	log.Printf("worker %s do reduce task %d (TaskType: %v, InputFile: %v, NReduce: %v, NMap: %v, Notified: %v, Done: %v)",
		workerId, task.TaskId, task.TaskType, task.InputFile, task.NReduce, task.NMap, task.Notified, task.Done)
	// 收集所有中间文件中的key-value对
	intermediate := []KeyValue{}
	for i := 0; i < task.NMap; i++ {
		inputFile := fmt.Sprintf("mr-%d-%d", i, task.TaskId)
		file, err := os.Open(inputFile)
		if err != nil {
			log.Fatalf("cannot open %v in Reduce", inputFile)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("cannot close %v in Reduce", file.Name())
		}
	}

	// 按key排序
	sort.Sort(ByKey(intermediate))

	// 创建输出文件
	// 每次创建的文件给与不同的任务ID和时间戳
	timestamp := time.Now().UnixNano()
	outputFilename := fmt.Sprintf("mr-out-%d-%s-%d", task.TaskId, workerId, timestamp)
	// outFile, _ := os.Create(fmt.Sprintf("mr-out-%d", task.TaskId))
	outFile, _ := os.Create(outputFilename)
	defer func(outFile *os.File) {
		err := outFile.Close()
		if err != nil {
			log.Fatalf("cannot close %v in Reduce", outFile.Name())
		}
	}(outFile)

	// 对每个不同的key调用Reduce函数
	i := 0
	for i < len(intermediate) {
		j := i + 1
		// 找到相同key的范围
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		var values []string
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		// 调用用户提供的Reduce函数
		output := reducef(intermediate[i].Key, values)

		// 将结果写入输出文件
		_, err := fmt.Fprintf(outFile, "%v %v\n", intermediate[i].Key, output)
		if err != nil {
			log.Fatalf("cannot write to %v in Reduce", outFile.Name())
		}

		i = j
	}
	log.Printf("worker %s finish reduce task %d (TaskType: %v, InputFile: %v, NReduce: %v, NMap: %v, Notified: %v, Done: %v)",
		workerId, task.TaskId, task.TaskType, task.InputFile, task.NReduce, task.NMap, task.Notified, task.Done)
}

// 报告任务完成
func reportTask(workerId string, task GetTaskReply) {
	args := ReportTaskArgs{
		TaskType:  task.TaskType,
		TaskId:    task.TaskId,
		InputFile: task.InputFile,
		NReduce:   task.NReduce,
		NMap:      task.NMap,
		Notified:  task.Notified,
		Done:      task.Done,
		WorkerId:  workerId,
	}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// CallExample
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
