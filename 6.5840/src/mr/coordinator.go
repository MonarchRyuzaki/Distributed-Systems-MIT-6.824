package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// status 0 -> not completed 1 -> in progress 2 -> completed
type Coordinator struct {
	nReduce int
	nMap    int
	input   []string
	mu      sync.Mutex

	taskTimeRef   [][]time.Time
	status        [][]int
	num_task_done []int
}

// Your code here -- RPC handlers for the worker to call.

func getAllReduceInput(taskId int) []string {
	pattern := fmt.Sprintf("mr-temp-*-%d", taskId)
	matches, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatal("Cant Find intermediateFiles", err)
		os.Exit(1)
	}
	return matches
}

func (c *Coordinator) ticker() {
	for {
		time.Sleep(1 * time.Second)
		c.mu.Lock()

		// Check Map Tasks
		for i := 0; i < c.nMap; i++ {
			if c.status[0][i] == 1 {
				// 10 second timeout
				if time.Since(c.taskTimeRef[0][i]) > 10*time.Second {
					c.status[0][i] = 0 // Reset to Idle
				}
			}
		}

		// Check Reduce Tasks (Copy logic above for c.nReduce)
		for i := 0; i < c.nReduce; i++ {
			if c.status[1][i] == 1 {
				if time.Since(c.taskTimeRef[1][i]) > 10*time.Second {
					c.status[1][i] = 0
				}
			}
		}

		c.mu.Unlock()
	}
}

func (c *Coordinator) AssignWorkerTask(args *Empty, reply *WorkerTask) error {
	c.mu.Lock()
	map_done := c.num_task_done[0]
	defer c.mu.Unlock()
	if map_done < c.nMap {
		for i, filename := range c.input {
			status := c.status[0][i] == 0
			if status {
				c.status[0][i] = 1
				c.taskTimeRef[0][i] = time.Now()
				reply.TaskType = 0
				reply.MapTask = WorkerMapTask{
					TaskId:   i,
					Filename: filename,
					NReduce:  c.nReduce,
				}
				// log.Println(reply)
				return nil
			}
		}
	} else {
		for i := range c.nReduce {
			status := c.status[1][i] == 0
			if status {
				c.status[1][i] = 1
				c.taskTimeRef[1][i] = time.Now()
				reply.TaskType = 1
				reply.ReduceTask = WorkerReduceTask{
					TaskId: i,
					Input:  getAllReduceInput(i),
				}
				// log.Println(reply)
				return nil
			}
		}
	}
	reply.TaskType = -1
	return nil
}

func (c *Coordinator) WorkerTaskComplete(args *WorkerTaskComplete, reply *Empty) error {

	taskType, taskId := args.TaskType, args.TaskId
	// log.Printf("TaskType = %d, TaskId = %d", taskType, taskId)
	// log.Println("Before Passing to channel")
	// log.Println("After Passing to channel")

	c.mu.Lock()
	defer c.mu.Unlock()
	if c.status[taskType][taskId] != 2 {
		c.status[taskType][taskId] = 2
		c.num_task_done[taskType]++
	}

	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	ret = c.num_task_done[1] == c.nReduce

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	n := len(files)
	c.nMap = n
	c.input = files
	c.nReduce = nReduce

	c.taskTimeRef = make([][]time.Time, 2)
	c.taskTimeRef[0] = make([]time.Time, n)
	c.taskTimeRef[1] = make([]time.Time, nReduce)
	c.status = make([][]int, 2)
	c.status[0] = make([]int, n)
	c.status[1] = make([]int, nReduce)
	c.num_task_done = []int{0, 0}

	// fmt.Println(c)
	go c.ticker()

	c.server()
	return &c
}
