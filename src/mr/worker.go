package mr

import (
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var coordSockName string // socket for coordinator

// main/mrworker.go calls this function.

// Worker 不断向 Coordinator 请求任务，然后根据任务类型采取行动。
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname
	for {
		args := AskTaskArgs{}
		reply := AskTaskReply{}
		ok := call("Coordinator.AskTask", &args, &reply) //RPC调用成功
		if !ok {
			time.Sleep(time.Second)
			continue
		}
		switch reply.TaskType {
		case MapTask:
			doMapTask(reply, mapf)
			finishTask(MapTask, reply.TaskId)
		case ReduceTask:
			doReduceTask(reply, reducef)
			finishTask(ReduceTask, reply.TaskId)
		case WaitTask:
			time.Sleep(time.Second)
		case ExitTask:
			return
		}
	}
	// Your worker implementation here.
	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func finishTask(taskType TaskType, taskId int) {
	args := FinishTaskArgs{
		TaskType: taskType, TaskId: taskId,
	}
	reply := FinishTaskReply{}
	call("Coordinator.FinishTask", &args, &reply)
}

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
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
