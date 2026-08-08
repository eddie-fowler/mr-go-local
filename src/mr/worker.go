package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
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

type ByKey []KeyValue
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

var coordSockName string // socket for coordinator
var workerId int // unique worker id for each worker

// main/mrworker.go calls this function.
/*
	1. ask for work from the coordinator until no more work is available
	2. (map) with RequestTask response from coordinator execute mapf with filename and contents 
	3. (reduce) with results from mapf execute reducef
	4. send results back to coordinator
*/
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	coordSockName = sockname
	workerId = os.Getpid()
	fmt.Printf("Worker %v started \n", workerId)

	var reply *RequestTaskReply
	var ce error
	for reply, ce = CallRequestTask(); reply.Filename != "" && ce == nil; reply, ce = CallRequestTask() {
		fmt.Printf("filename reply: %v \n", reply.Filename)

		//get contents of file
		file, err := os.Open(reply.Filename)
		if err != nil {
			signalError(workerId, reply.Filename)
			log.Fatalf("cannot open %v", reply.Filename)
		}
		contents, err := ioutil.ReadAll(file)
		if err != nil {
			signalError(workerId, reply.Filename)
			log.Fatalf("cannot read %v", reply.Filename)
		}
		file.Close()

		//call mapf with filename and contents
		var kvs []KeyValue
		kvs = mapf(reply.Filename, string(contents))
		// fmt.Printf("kvs: %v \n", kvs)

		//call reducef with results from mapf
		intermediate := []KeyValue{}
		intermediate = append(intermediate, kvs...)
		sort.Sort(ByKey(intermediate))

		i := 0
		for i < len(intermediate) {
			j := i + 1
			for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, intermediate[k].Value)
			}
			// output := reducef(intermediate[i].Key, values)
			// fmt.Printf("%v %v \n", intermediate[i].Key, output)

			i = j
		}

		CallUpdateTaskStatus(reply.Filename, "completed", workerId)
		time.Sleep(time.Second * 2)
	}

	if ce != nil {
		signalError(workerId, reply.Filename)
		log.Fatal(ce)
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

func CallRequestTask() (*RequestTaskReply, error) {
	args := RequestTaskArgs{}
	args.WorkerId = workerId

	reply := RequestTaskReply{}

	ok := call("Coordinator.RequestTask", &args, &reply)
	skip := (reply.Filename == "pg-being_ernest.txt" && workerId % 2 == 0) //simulate worker failure for testing
	if ok && !skip {
		return &reply, nil
	} else {
		return nil, fmt.Errorf("call failed")
	}
}

func CallUpdateTaskStatus(taskID string, status string, assignedWorkerId int) error {
	args := UpdateTaskStatusArgs{TaskID: taskID, Status: status, AssignedWorkerId: assignedWorkerId}
	reply := UpdateTaskStatusReply{}

	ok := call("Coordinator.UpdateTaskStatus", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("call failed")
	}
}

func CallUpdateWorkerStatus(assignedWorkerId int, status string) error {
	args := UpdateWorkerStatusArgs{WorkerId: workerId, Status: status}
	reply := UpdateWorkerStatusReply{}

	ok := call("Coordinator.UpdateWorkerStatus", &args, &reply)
	if ok {
		return nil
	} else {
		return fmt.Errorf("call failed")
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


func signalError(assignedWorkerId int, taskID string) {
	CallUpdateWorkerStatus(assignedWorkerId, "dead")
	CallUpdateTaskStatus(taskID, "not completed", assignedWorkerId)
}
