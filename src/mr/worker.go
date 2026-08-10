package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
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
		fmt.Printf("WorkerId: %v | Begin processing task: %v of type: %v reduceCount: %v\n", workerId, reply.Filename, reply.TaskType, reply.ReduceCount)

		if reply.TaskType == "map" {
			executeMapf(reply.Filename, mapf)
		} else {
			executeReducef(reply.Filename, reducef, reply.ReduceCount)
		}

		CallUpdateTaskStatus(reply.Filename, "completed", workerId)
		// time.Sleep(time.Second * 2)
	}

	if ce != nil {
		signalError(workerId, reply.Filename)
		// log.Fatal(ce)
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
	// skip := (reply.Filename == "pg-being_ernest.txt" && workerId % 2 == 0) //simulate worker failure for testing
	skip := false
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

func executeMapf(filename string, mapf func(string, string) []KeyValue) {
		file, err := os.Open(filename)
		if err != nil {
			signalError(workerId, filename)
			log.Fatalf("cannot open %v", filename)
		}
		contents, err := ioutil.ReadAll(file)
		if err != nil {
			signalError(workerId, filename)
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		var kvs []KeyValue
		kvs = mapf(filename, string(contents))

		fileNameOnly := getFileNameOnly(filename)
		oname := "tmp-map-" + fmt.Sprint(fileNameOnly)
		ofile, _ := os.Create(oname)

		for _, kv := range kvs {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}

		ofile.Close()
}

func executeReducef(filename string, reducef func(string, []string) string, nReduceCount int) string {
	fmt.Printf("reduce count for reducef %v \n", nReduceCount)
	fileNameOnly := getFileNameOnly(filename)
	data, err := os.ReadFile("tmp-map-"+fileNameOnly)
	if err != nil {
		signalError(workerId, filename)
		log.Fatalf("cannot read %v", "tmp-map-"+fileNameOnly)
	}

	text := strings.TrimSuffix(string(data), "\n")
	lines := strings.Split(text, "\n")
	intermediate := make([]KeyValue, len(lines))
	
	for i, l := range lines {
		intermediate[i] = toKeyValue(l)
	}

	sort.Sort(ByKey(intermediate))

	oname := "mr-out-"+ strconv.Itoa(nReduceCount)
	oname, err = filepath.Abs(oname)
	if err != nil {
		signalError(workerId, filename)
		log.Fatalf("cannot resolve absolute path for %v", oname)
	}
	ofile, _ := os.Create(oname)

	i := 0
	for i < len(intermediate) {
		j := i + 1

		for j < len(intermediate) && intermediate[i].Key == intermediate[j].Key {
			j++
		}

		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}

		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	ofile.Close()

	return oname
}

func getFileNameOnly(filename string) string {
	fileNameParts := strings.Split(filename, "/")
	fileName := fileNameParts[len(fileNameParts)-1]
	return fileName[:len(fileName)-4]
}

func toKeyValue(line string) KeyValue {
	tmp := strings.Split(line, " ")
	kv := KeyValue{}
	kv.Key = tmp[0]
	kv.Value = tmp[1]

	return kv
}
