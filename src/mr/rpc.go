package mr

// RPC definitions.
//定义 Worker 和 Coordinator 之间通信的数据结构

// 表示 Coordinator 可能返回给 Worker 的四种状态：
type TaskType int //底层是 int
const (           //块定义常量
	MapTask    TaskType = iota //iota从0开始每行自动加1
	ReduceTask                 //reduce阶段
	WaitTask                   //没有任务可分配，Worker需要等待
	ExitTask
)

// Worker 向 Coordinator 请求任务;
type AskTaskArgs struct {
}
type AskTaskReply struct {
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
