package mr

import (
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strings"
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
var workerInitDir string // directory the worker command was run from

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	coordSockName = sockname
	workerId = os.Getpid()
	fmt.Printf("Worker %v started \n", workerId)

	if wd, err := os.Getwd(); err == nil {
		workerInitDir = wd
	} else {
		log.Printf("cannot determine worker init dir: %v", err)
	}

	var reply *RequestTaskReply
	var ce error
	for reply, ce = CallRequestTask(); reply.Task.ReadLocation != "" && ce == nil; reply, ce = CallRequestTask() {
		fmt.Printf("WorkerId: %v | Begin processing task: %v of type: %v \n", workerId, reply.Task.ReadLocation, reply.Task.Type)

		intermediate := make([]KeyValue, 0)
		var err error
		if reply.Task.Type == "map" {
			err = executeMapf(reply.Task.ReadLocation, reply.Task.WriteLocation, &intermediate, mapf)
		} else {
			err = executeReducef(reply.Task.ReadLocation, reply.Task.WriteLocation, reducef)
		}

		if err != nil {
			CallUpdateTaskStatus(reply.Task.Id, "not started", reply.Task.WriteLocation, -1, intermediate)
		} else {
			CallUpdateTaskStatus(reply.Task.Id, "completed", reply.Task.WriteLocation, reply.Task.AssignedWorkerId, intermediate)
			time.Sleep(time.Second * 2)
		}
	}

	if ce != nil {
		log.Fatal(ce)
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

func CallUpdateTaskStatus(taskID string, status string, writeLocation string, assignedWorkerId int, intermediate []KeyValue) error {
	args := UpdateTaskStatusArgs{TaskId: taskID, Status: status, WriteLocation: writeLocation, AssignedWorkerId: assignedWorkerId, Intermediate: intermediate}
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


func signalError(task Task) {
	// CallUpdateWorkerStatus(task.AssignedWorkerId, "dead")
	// CallUpdateTaskStatus(task.Id, "not completed", task.WriteLocation, task.AssignedWorkerId, nil)
}


//Read from location 
//Write to location 
//Store results in intermediate [Will sort intermediate when all maps done then partition by ReducerCount]
//Update task status 
func executeMapf(readLocation string, writeLocation string, intermediate *[]KeyValue, mapf func(string, string) []KeyValue) error {
		file, err := os.Open(readLocation)
		if err != nil {
			log.Printf("cannot open %v", readLocation)
			return err
		}
		contents, err := ioutil.ReadAll(file)
		if err != nil {
			log.Printf("cannot read %v", readLocation)
			return err
		}
		file.Close()

		var kvs []KeyValue
		kvs = mapf(readLocation, string(contents))

		*intermediate = append(*intermediate, kvs...)

		fileNameOnly := getFileNameOnly(writeLocation)
		writeToLocation(fileNameOnly, writeLocation, kvs)
		writeToWorkerInitDir(writeLocation, kvs)

		return nil
}

func executeReducef(readLocation string, writeLocation string, reducef func(string, []string) string) error {
	data, err := os.ReadFile(readLocation)
	if err != nil {
		log.Printf("cannot read %v", readLocation)
		return  err
	}

	text := strings.TrimSuffix(string(data), "\n")
	lines := strings.Split(text, "\n")
	intermediate := make([]KeyValue, len(lines))
	
	for i, l := range lines {
		intermediate[i] = toKeyValue(l)
	}

	sort.Sort(ByKey(intermediate))

	ofile, _ := os.Create(writeLocation)
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
	copyToWorkerInitDir(writeLocation)

	return nil
}

func writeToWorkerInitDir(writeLocation string, kvs []KeyValue) {
	if workerInitDir == "" {
		return
	}

	baseFileName := filepath.Base(writeLocation)
	if filepath.Clean(workerInitDir) == filepath.Clean(filepath.Dir(writeLocation)) {
		return
	}

	writeToLocation(baseFileName, workerInitDir, kvs)
}

func copyToWorkerInitDir(writeLocation string) {
	if workerInitDir == "" {
		return
	}

	if filepath.Clean(workerInitDir) == filepath.Clean(filepath.Dir(writeLocation)) {
		return
	}

	data, err := os.ReadFile(writeLocation)
	if err != nil {
		log.Printf("cannot read %v to duplicate into worker init dir: %v", writeLocation, err)
		return
	}

	dupPath := filepath.Join(workerInitDir, filepath.Base(writeLocation))
	if err := os.WriteFile(dupPath, data, 0644); err != nil {
		log.Printf("cannot write duplicate %v: %v", dupPath, err)
	}
}

func writeToLocation(fileName string, writeLocation string, kvs []KeyValue){
		ofile, _ := os.Create(path.Join(writeLocation, fileName))

		for _, kv := range kvs {
			fmt.Fprintf(ofile, "%v %v\n", kv.Key, kv.Value)
		}

		ofile.Close()
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
