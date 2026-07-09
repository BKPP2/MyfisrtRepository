package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

// 定义任务状态 //
type TaskStatus int //单个任务的生命周期

const (
	Idle       TaskStatus = iota //任务还没人做
	InProgress                   //任务正在被某个worker做
	Completed                    //任务已经完成
)

type Phase int //整个集群作业的宏观进度

const (
	MapPhase    Phase = iota //映射阶段
	ReducePhase              //归约阶段
	DonePhase                //完成阶段
)

type TaskInfo struct {
	Status    TaskStatus
	StartTime time.Time
}

// Coordinator 结构体//
type Coordinator struct {
	mu          sync.Mutex
	files       []string //输入文件列表
	nReduce     int      //reduce任务数量
	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	phase       Phase //当前阶段
}

// Your code here -- RPC handlers for the worker to call.

// Worker 请求任务
const TaskTimeout = 10 * time.Second

func (c *Coordinator) AskTask(args *AskTaskArgs, reply *AskTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock() //defer确保函数结束时释放锁

	// Map 全部完成后进入 Reduce 阶段
	if c.phase == MapPhase && allCompleted(c.mapTasks) {
		c.phase = ReducePhase
	}
	// Reduce 全部完成后结束
	if c.phase == ReducePhase && allCompleted(c.reduceTasks) {
		c.phase = DonePhase
	}

	switch c.phase {
	case MapPhase:
		for id := range c.mapTasks {
			task := &c.mapTasks[id]
			//空闲任务，或执行超时的任务，都可以重新分配。
			if task.Status == Idle || (time.Since(task.StartTime) > TaskTimeout) {
				task.Status = InProgress
				task.StartTime = time.Now()
				reply.TaskType = MapTask
				reply.TaskId = id
				reply.FileName = c.files[id]
				reply.NReduce = c.nReduce //reduce任务数量
				reply.NMap = len(c.files) //map任务数量等于输入文件数量
				return nil
			}
		}
		reply.TaskType = WaitTask //没有可分配的任务，告诉Worker等待

	case ReducePhase:
		for id := range c.reduceTasks {
			task := &c.reduceTasks[id]
			if task.Status == Idle || (time.Since(task.StartTime) > TaskTimeout) {
				task.Status = InProgress
				task.StartTime = time.Now()
				reply.TaskType = ReduceTask
				reply.TaskId = id
				reply.NReduce = c.nReduce
				reply.NMap = len(c.files)
				return nil
			}
		}
		reply.TaskType = WaitTask
	case DonePhase:
		reply.TaskType = ExitTask
	}
	return nil
}
func allCompleted(tasks []TaskInfo) bool {
	for _, task := range tasks {
		if task.Status != Completed {
			return false
		}
	}
	return true
}

// Worker 完成一个任务后，通知 Coordinator 将它标记为 Completed
func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.TaskType {
	case MapTask:
		if args.TaskId >= 0 && args.TaskId < len(c.mapTasks) {
			c.mapTasks[args.TaskId].Status = Completed
		}
	case ReduceTask:
		if args.TaskId >= 0 && args.TaskId < len(c.reduceTasks) {
			c.reduceTasks[args.TaskId].Status = Completed
		}
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
// server() 给 Coordinator 建立一个后台 RPC 服务入口，使多个 Worker 可以通过 Unix socket 请求和汇报任务。
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)  //把c注册为RPC服务
	rpc.HandleHTTP() //让Go RPC使用HTTP协议处理请求。
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.phase == DonePhase
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
// 按照 Go 惯例，首字母大写的函数是公开的（可以被其他包调用）
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:       files,
		nReduce:     nReduce,
		mapTasks:    make([]TaskInfo, len(files)),
		reduceTasks: make([]TaskInfo, nReduce),
		phase:       MapPhase,
	}

	c.server(sockname) //启动RPC服务器，监听来自Worker的请求

	return &c
}
