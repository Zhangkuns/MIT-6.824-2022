package mr

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	// 任务管理
	tasks     []Task       // 所有任务的列表
	taskPhase Phase        // 当前阶段（Map/Reduce）
	taskStats []TaskStatus // 任务状态跟踪
	nMap      int          // Map 任务数量
	nReduce   int          // Reduce 任务数量

	// 文件相关
	files []string // 输入文件列表

	// 并发控制
	mu   sync.Mutex // 互斥锁保护共享数据
	done bool       // 标记是否所有工作完成

	// 任务分配和超时处理
	taskChannel chan Task     // 任务分配通道
	timeout     time.Duration // 任务超时时间

	workers map[string]*WorkerInfo
}

// WorkerInfo 存储 worker 的基本信息
type WorkerInfo struct {
	WorkerId  string    // worker 的唯一标识
	Status    int       // worker 的状态 0: 空闲 1: 工作中
	LastHeard time.Time // 最后一次收到该 worker 消息的时间
}

// 辅助结构体定义

type Phase int

const (
	InvalidPhase  Phase = -1
	MapPhase      Phase = iota
	ReducePhase   Phase = iota
	CompletePhase Phase = iota
)

// 添加一个方法来获取 Phase 的字符串表示
func (p Phase) String() string {
	switch p {
	case MapPhase:
		return "Map"
	case ReducePhase:
		return "Reduce"
	case CompletePhase:
		return "Complete"
	case InvalidPhase:
		return "Invalid"
	default:
		return "Unknown"
	}
}

type TaskStatus struct {
	Status    int       // 任务状态（未开始/进行中/已完成）
	StartTime time.Time // 任务开始时间
	WorkerId  string    // 当前执行该任务的 worker ID
}

type Task struct {
	TaskType  Phase  // 任务类型（Map/Reduce）
	TaskId    int    // 任务ID
	InputFile string // 输入文件（Map任务）
	NReduce   int    // Reduce任务数量
	NMap      int    // Map任务数量
	Notified  bool   // 任务是否已经处理过
	Done      bool   // 任务是否已经完成
	Assigned  bool   // 任务是否已经分配
}

// nolint:unused // print task
// printtask displays the task details for debugging purposes
//
//lint:ignore U1000 This function is kept for debugging Task execution states
func (task *Task) printtask() {
	log.Printf("TaskType: %v, TaskId: %v, InputFile: %v, NReduce: %v, NMap: %v, Notified: %v, Done: %v\n",
		task.TaskType, task.TaskId, task.InputFile, task.NReduce, task.NMap, task.Notified, task.Done)
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	// 注册coordinator对象，使其RPC方法可被远程调用
	errRegister := rpc.Register(c)
	if errRegister != nil {
		print("error registering rpc")
	}
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	//如果先前有残留的socket文件，删除之
	sockname := coordinatorSock()
	errRomoveSock := os.Remove(sockname)
	if errRomoveSock != nil {
		print("error removing sockname")
	}
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	// 通过 HTTP 服务器处理来自监听器 1 的请求。这是一个异步操作，它会一直运行，处理客户端 worker 的 RPC 请求
	//errHttp := http.Serve(l, nil)
	//if errHttp != nil {
	//	print("HTTP server failed")
	//}
	// 启动一个新的goroutine来处理RPC请求
	go func() {
		errHttp := http.Serve(l, nil)
		if errHttp != nil {
			print("HTTP server failed")
		}
	}()
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	//ret := false
	//return ret
	// Your code here.
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// MakeCoordinator
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// 初始化协调器
	c := Coordinator{
		// 初始化基本参数
		files:     files,
		nReduce:   nReduce,
		nMap:      len(files),
		taskPhase: MapPhase, // 初始阶段为Map
		done:      false,

		// 初始化任务状态
		tasks:     make([]Task, 0),
		taskStats: make([]TaskStatus, 0),

		// 初始化任务通道
		taskChannel: make(chan Task, len(files)+nReduce), // 缓冲大小为Map任务数+Reduce任务数

		// 设置超时时间（比如10秒）
		timeout: 10 * time.Second,
	}
	// 初始化任务状态
	// 创建Map任务
	for i, file := range files {
		task := Task{
			TaskType:  MapPhase,
			TaskId:    i,
			InputFile: file,
			NReduce:   nReduce,
			NMap:      len(files),
			Notified:  false,
			Done:      false,
		}
		c.tasks = append(c.tasks, task)
		c.taskStats = append(c.taskStats, TaskStatus{
			Status:    0,
			StartTime: time.Time{},
			WorkerId:  "",
		})
		// 将任务放入通道
		c.taskChannel <- task
	}
	// Your code here.

	c.server()
	return &c
}

// RegisterWorker
// 注册Worker
func (c *Coordinator) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()
	// 验证 worker ID 不为空
	if args.WorkerId == "" {
		reply.Success = false
		return fmt.Errorf("invalid worker ID")
	}

	// 初始化 workers map（如果还没初始化）
	if c.workers == nil {
		c.workers = make(map[string]*WorkerInfo)
	}

	// 检查 worker 是否已经注册
	if _, exists := c.workers[args.WorkerId]; exists {
		// 如果已存在，更新最后活跃时间
		c.workers[args.WorkerId].LastHeard = time.Now()
		reply.Success = true
		return nil
	}

	// 创建新的 worker 信息
	workerInfo := &WorkerInfo{
		WorkerId:  args.WorkerId,
		Status:    0, // 初始状态设为空闲
		LastHeard: time.Now(),
	}

	// 将新 worker 添加到 map 中
	c.workers[args.WorkerId] = workerInfo

	log.Printf("New worker registered: %s", args.WorkerId)
	reply.Success = true
	return nil
}

// GetTask RPC处理函数
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	log.Printf("Worker %s request task, get the mu.lock", args.WorkerId)
	defer c.mu.Unlock()
	// 验证 worker 是否已注册
	worker, exists := c.workers[args.WorkerId]
	if !exists {
		return fmt.Errorf("worker %s not registered", args.WorkerId)
	}
	// 更新 worker 状态
	worker.LastHeard = time.Now()
	worker.Status = 1 // 标记为工作中
	//c.mu.Unlock()

	// 使用带超时的等待
	timeoutDuration := 1 * time.Second
	timer := time.NewTimer(timeoutDuration)
	defer timer.Stop()

	// 检查是否已完成
	if c.done {
		log.Printf("Coordinator is done, worker %s should exit", args.WorkerId)
		reply.Done = true
		return nil
	}

	//检查isAllReduceTasksCompleted
	//*********using for debug*********
	if c.taskPhase == ReducePhase {
		if c.isAllReduceTasksCompleted(args.WorkerId) {
			log.Printf("All reduce tasks completed")
			c.done = true
			reply.Done = true
			return nil
		}
	}

	// 尝试获取任务
	select {
	case task := <-c.taskChannel:
		// 更新任务状态
		if task.TaskType == MapPhase {
			if c.taskStats[task.TaskId].Status == 1 {
				log.Printf("Worker %s request task, but task %s 's status is %d, %d is already assigned", args.WorkerId, task.TaskType.String(), c.taskStats[task.TaskId].Status, task.TaskId)
				reply.TaskType = InvalidPhase
				return nil
			}
		} else {
			if task.TaskType == ReducePhase {
				if c.taskStats[task.TaskId+task.NMap].Status == 1 {
					log.Printf("Worker %s request task, but task %s 's status is %d, %d is already assigned", args.WorkerId, task.TaskType.String(), c.taskStats[task.TaskId].Status, task.TaskId)
					reply.TaskType = InvalidPhase
					return nil
				}
			}
		}
		log.Printf("Assigning %s task %d to worker %s", task.TaskType.String(), task.TaskId, args.WorkerId)
		c.taskStats[task.TaskId].Status = 1
		c.taskStats[task.TaskId].StartTime = time.Now()
		c.taskStats[task.TaskId].WorkerId = args.WorkerId

		// 填充响应
		reply.TaskType = task.TaskType
		reply.TaskId = task.TaskId
		reply.InputFile = task.InputFile
		reply.NReduce = c.nReduce
		reply.NMap = c.nMap

		return nil

	default:
		// 任务通道为空，检查阶段完成情况
		//c.mu.Lock()
		//defer c.mu.Unlock()

		if c.isPhaseComplete() {
			if c.taskPhase == MapPhase {
				log.Printf("Worker %s detect all map tasks completed, moving to reduce phase", args.WorkerId)
				// 转换到Reduce阶段
				c.taskPhase = ReducePhase
				// 首先，清空现有的任务通道
				for len(c.taskChannel) > 0 {
					<-c.taskChannel // 清空通道中的所有元素
				}
				c.printChannelContents()
				// 创建Reduce任务
				for i := 0; i < c.nReduce; i++ {
					task := Task{
						TaskType:  ReducePhase,
						TaskId:    i,
						InputFile: "mr-*-" + fmt.Sprintf("%d", i),
						NReduce:   c.nReduce,
						NMap:      c.nMap,
						Notified:  false,
						Done:      false,
					}
					c.tasks = append(c.tasks, task)
					c.taskStats = append(c.taskStats, TaskStatus{
						Status:    0,
						StartTime: time.Time{},
						WorkerId:  "",
					})
					// 将任务放入通道
					c.taskChannel <- task
				}
				c.printChannelContents()
				reply.TaskType = InvalidPhase
				return nil
			} else if c.taskPhase == ReducePhase {
				log.Printf("Worker %s detect all reduce tasks completed", args.WorkerId)
				c.done = true
				reply.Done = true
				return nil
			}
		}
		// 在检查超时之前打印通道状态
		log.Printf("===== Channel status before timeout check =====")
		c.printChannelContents()

		// 检查超时任务
		ifassign := c.checkTimeoutTasks(args.WorkerId)

		// 在处理超时之后打印通道状态
		log.Printf("===== Channel status after timeout check =====")
		c.printChannelContents()
		if ifassign {
			reply.TaskType = InvalidPhase
			return nil
		}
		// 等待其他任务完成(modify)
		reply.TaskType = InvalidPhase
		return nil
	}
}

// 辅助函数：检查超时任务
func (c *Coordinator) checkTimeoutTasks(workerId string) bool {
	//c.mu.Lock()
	//defer c.mu.Unlock()
	var specialTasks []int
	var ordinaryTasks int
	for i := range c.tasks {
		if c.taskStats[i].Status == 1 && !c.tasks[i].Done {
			//if c.taskStats[i].Status == 1 || !c.tasks[i].Done {
			// 任务已分配但未完成
			if time.Since(c.taskStats[i].StartTime) > c.timeout {
				//强行打补丁
				ordinaryTasks = i
				log.Printf("Worker %s dectected timeout for %s task %d, reassigning", workerId, c.tasks[i].TaskType.String(), c.tasks[i].TaskId)
				// 重置任务状态
				c.tasks[i].Done = false
				c.taskStats[i].Status = 0
				c.taskStats[i].StartTime = time.Time{}
				c.taskStats[i].WorkerId = ""
				// 将任务重新放入通道
				c.taskChannel <- c.tasks[i]
			}
		}
		//强行打补丁
		if c.taskPhase == ReducePhase && i >= c.nMap {
			if !c.tasks[i].Done {
				if c.taskStats[i].Status == 0 {
					specialTasks = append(specialTasks, i)
				}
			}
		}
		//强行打补丁
	}
	// 1->3
	//if len(specialTasks) > 0 && len(specialTasks) < 3 {
	if len(specialTasks) > 0 {
		//i := specialTasks[0]
		// 使用随机数生成器
		rand.Seed(time.Now().UnixNano()) // 确保每次运行都有不同的随机序列

		// 随机选择一个索引
		randomIndex := rand.Intn(len(specialTasks))
		i := specialTasks[randomIndex]
		if i != ordinaryTasks {
			// 这才是真正的超时任务
			log.Printf("Worker %s dectected special timeout for %s task %d, reassigning", workerId, c.tasks[i].TaskType.String(), c.tasks[i].TaskId)
			// 处理超时...
			// 重置任务状态
			c.tasks[i].Done = false
			c.taskStats[i].Status = 0
			c.taskStats[i].StartTime = time.Time{}
			c.taskStats[i].WorkerId = ""
			// 将任务重新放入通道
			c.taskChannel <- c.tasks[i]
			return true
		}
	}
	return false
}

func (c *Coordinator) isAllReduceTasksCompleted(workerId string) bool {
	flag := false //是否有未完成的任务
	// 检查所有 Reduce 任务
	for i := 0; i < len(c.tasks); i++ {
		//if !c.tasks[i].Done || c.taskStats[i].Status != 2 {
		if !c.tasks[i].Done {
			flag = true
			log.Printf("Worker %s detect Task %d not completed: Done=%v, Status=%d, time=%v", workerId, i, c.tasks[i].Done, c.taskStats[i].Status, c.taskStats[i].StartTime)
		}
	}
	if flag {
		return false
	} else {
		return true
	}
}

// 辅助函数：检查当前阶段是否完成
func (c *Coordinator) isPhaseComplete() bool {
	for i := range c.tasks {
		if c.tasks[i].TaskType == c.taskPhase && !c.tasks[i].Done {
			return false
		}
	}
	return true
}

// ReportTask RPC处理函数
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *struct{}) error {
	c.mu.Lock()
	log.Printf("Worker %s report task, get the mu.lock", args.WorkerId)
	defer c.mu.Unlock()
	// 找到对应任务
	for i := range c.tasks {
		if c.tasks[i].TaskType == args.TaskType && c.tasks[i].TaskId == args.TaskId {
			c.tasks[i].Done = true
			c.taskStats[i].Status = 2 // 标记为完成
			log.Printf("Worker %s completed %s task %d", args.WorkerId, args.TaskType.String(), args.TaskId)
			return nil
		}
	}
	return nil
}

func (c *Coordinator) printChannelContents() {

	// 创建一个临时切片来存储通道中的元素
	var tasks []Task
	channelLength := len(c.taskChannel)

	log.Printf("Current channel length: %d", channelLength)

	// 从通道中读取所有元素并保存
	for i := 0; i < channelLength; i++ {
		if task, ok := <-c.taskChannel; ok {
			tasks = append(tasks, task)
			// 记录任务信息
			log.Printf("Channel element %d: Task{Type: %v, ID: %d, Done: %v}",
				i, task.TaskType, task.TaskId, task.Done)
			// 将任务放回通道
			c.taskChannel <- task
		}
	}

	log.Printf("Channel inspection complete. Found %d tasks", len(tasks))
}
