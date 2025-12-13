package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//
type Empty struct{}
type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type WorkerTask struct {
	TaskType int
	MapTask WorkerMapTask
	ReduceTask WorkerReduceTask
}

type WorkerMapTask struct {
	TaskId int
	Filename string
	NReduce int
}

type WorkerTaskComplete struct {
	TaskId int
	TaskType int
}

type WorkerReduceTask struct {
	TaskId int
	Input []string
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
