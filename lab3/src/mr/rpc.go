package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"log"
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type RegisterArgs struct {
	WorkerId string
}
type RegisterReply struct {
	Success bool
}

type GetTaskArgs struct {
	WorkerId string
}
type GetTaskReply struct {
	TaskType  Phase  // 任务类型（Map/Reduce）
	TaskId    int    // 任务ID
	InputFile string // 输入文件（Map任务）
	NReduce   int    // Reduce任务数量
	NMap      int    // Map任务数量
	Notified  bool   // 任务是否已经注意到
	Done      bool   // 任务是否已经完成
}

// nolint:unused // 用于打印日志
// print displays the task reply details for debugging purposes
//
//lint:ignore U1000 This function is kept for debugging MapReduce task states
func (reply *GetTaskReply) print() {
	log.Printf("TaskType: %v, TaskId: %v, InputFile: %v, NReduce: %v, NMap: %v, Notified: %v, Done: %v\n",
		reply.TaskType, reply.TaskId, reply.InputFile, reply.NReduce, reply.NMap, reply.Notified, reply.Done)
}

type ReportTaskArgs struct {
	TaskType  Phase  // 任务类型（Map/Reduce）
	TaskId    int    // 任务ID
	InputFile string // 输入文件（Map任务）
	NReduce   int    // Reduce任务数量
	NMap      int    // Map任务数量
	Notified  bool   // 任务是否已经注意到
	Done      bool   // 任务是否已经完成
	WorkerId  string // Worker ID
}
type ReportTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
