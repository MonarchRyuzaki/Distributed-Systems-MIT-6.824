package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
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

func workerMapTask(workerTask WorkerMapTask, mapf func(string, string) []KeyValue) {
	contents, err := os.ReadFile(workerTask.Filename)
	if err != nil {
		log.Fatal("Cant Read File ", err)
		os.Exit(1)
	}

	intermediateData := mapf(workerTask.Filename, string(contents))
	tmpFile := make([]*os.File, workerTask.NReduce)
	encoders := make([]*json.Encoder, workerTask.NReduce)

	for i := range workerTask.NReduce {
		filename := fmt.Sprintf("mr-temp-%v-%v", workerTask.TaskId, i)

		file, err := os.Create(filename)
		if err != nil {
			log.Fatal("Cant Create File ", file)
			os.Exit(1)
		}

		tmpFile[i] = file
		encoders[i] = json.NewEncoder(file)
	}

	for _, pair := range intermediateData {
		bucket := ihash(pair.Key) % workerTask.NReduce

		err := encoders[bucket].Encode(&pair)
		if err != nil {
			log.Fatal("Cant Encode", err)
			os.Exit(1)
		}
	}

	for _, file := range tmpFile {
		file.Close()
	}
}

func workerReduceTask(workerTask WorkerReduceTask, reducef func(string, []string) string) {

	// Read the files
	// Put the data in intermediateValues
	// Sort it
	// Pass it to reduce
	// Write to temp file
	// Rename it

	intermediateData := make([]KeyValue, 0)
	for _, filename := range workerTask.Input {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatal("Cant Read Intermediate File", err)
			os.Exit(1)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediateData = append(intermediateData, kv)
		}

		file.Close()

	}

	sort.Slice(intermediateData, func(i, j int) bool {
		return intermediateData[i].Key < intermediateData[j].Key
	})

	oname := fmt.Sprintf("mr-out-tmp-%d", workerTask.TaskId)
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediateData) {
		j := i + 1
		for j < len(intermediateData) && intermediateData[j].Key == intermediateData[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediateData[k].Value)
		}
		output := reducef(intermediateData[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediateData[i].Key, output)

		i = j
	}

	ofile.Close()

	os.Rename(fmt.Sprintf("mr-out-tmp-%d", workerTask.TaskId), fmt.Sprintf("mr-out-%d", workerTask.TaskId))

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	for {
		task := askMasterForATask()
		// fmt.Println(task, task.MapTask, task.ReduceTask)
		if task.TaskType == -1 {
			time.Sleep(1 * time.Second)
			continue
		}
		if task.TaskType == 0 {
			workerMapTask(task.MapTask, mapf)
			tellMasterTaskDone(task.MapTask.TaskId, task.TaskType)
		} else {
			workerReduceTask(task.ReduceTask, reducef)
			tellMasterTaskDone(task.ReduceTask.TaskId, task.TaskType)
		}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

func askMasterForATask() WorkerTask {
	workerTask := WorkerTask{}
	args := Empty{}
	ok := call("Coordinator.AssignWorkerTask", &args, &workerTask)
	if !ok {
		fmt.Println("Cant get task from worker")
	}
	return workerTask
}

func tellMasterTaskDone(taskId int, taskType int) {
	args := WorkerTaskComplete{
		TaskId:   taskId,
		TaskType: taskType,
	}
	reply := Empty{}
	ok := call("Coordinator.WorkerTaskComplete", &args, &reply)
	if !ok {
		log.Fatal("Coordinate Handle Intermediate Output error")
	}
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
