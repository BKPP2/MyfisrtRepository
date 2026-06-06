package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

// 表示 Coordinator 可能返回给 Worker 的四种状态：
type TaskType int //底层是 int
const (           //块定义常量
	MapTask TaskType = iota //iota从0开始每行自动加1
	ReduceTask
	WaitTask
	ExitTask
)

// Worker 向 Coordinator 请求任务;
type AskTaskArgs struct {
}
type AskTaskReplay struct {
	TaskType TaskType
	TaskId   int
	FileName string
	NReduce  int
	NMap     int
}

// Worker 完成任务后通知 Coordinator。
type FinishTaskArgs struct {
	TaskType TaskType
	TaskId   int
}
type FinishTaskReply struct {
}
