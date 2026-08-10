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
)

/*
	Coordinator's responsibility is to
	1. Give tasks to workers upon request
	2. Keep track of which tasks are completed and which are not
	3. Keep track of which workers are alive and which are dead
	4. Reassign tasks to workers if a worker dies before completing the task
*/

type Coordinator struct {
	tasks 	map[string][]Task
	isMapDone bool 
	workers map[int]bool
	nReduceCount int
	mu sync.Mutex
}

type Task struct {
	taskID string //filename
	taskType string //map or reduce
	taskStatus string //not started, in progress, completed, skipped
	assignedWorkerId int //index of worker assigned to task
}

var inputFilesCount int

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mu.Lock()

	fmt.Printf("Task request from worker %v \n", args.WorkerId)

	var nextTask *Task
	for key := range c.tasks {
		tasks := c.tasks[key]
		for i := range tasks {
			t := tasks[i]
			if (!c.isMapDone && t.taskType == "map" && t.taskStatus == "not started") ||
				(c.isMapDone && t.taskType == "reduce" && t.taskStatus == "not started") {
				nextTask = &tasks[i]
			}
		}
	}

	if nextTask != nil {
		nextTask.assignedWorkerId = args.WorkerId
		nextTask.taskStatus = "in progress"
	
		fmt.Printf("Allocating task: %v type: %v to worker: %v \n", nextTask.taskID, nextTask.taskType, nextTask.assignedWorkerId)

		reply.Filename = nextTask.taskID
		reply.TaskType = nextTask.taskType
		if(nextTask.taskType == "reduce"){
			reply.ReduceCount = c.nReduceCount
		}
	}

	c.mu.Unlock()
	return nil
}

func (c *Coordinator) UpdateWorkerStatus(args *UpdateWorkerStatusArgs, reply *UpdateWorkerStatusReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	c.workers[args.WorkerId] = (args.Status == "alive")

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.mu.Lock() 
	defer c.mu.Unlock()

	task := c.tasks[args.TaskID]

	for i := range task {
		if task[i].assignedWorkerId == args.AssignedWorkerId {
			fmt.Printf("WorkerId: %v updating task: %v type: %v status: %v \n", args.AssignedWorkerId, args.TaskID, task[i].taskType, args.Status)
			task[i].taskStatus = args.Status

			if task[i].taskStatus == "completed" && task[i].taskType == "map" {
				task[i].assignedWorkerId = -1
				c.checkIfAllMapsDone()
				break
			} else if task[i].taskStatus == "completed" && task[i].taskType == "reduce" {
				c.nReduceCount -= 1
			}

		}
	}

	return nil
}


//This is called on loop by coordinator setups break dependency on struct state to avoid race conditions
func (c *Coordinator) Done() bool {
	outputFiles, err := filepath.Glob("mr-out-*")
	if err != nil {
		log.Fatalf("error globbing mr-out-* files: %v", err)
	}

	fmt.Printf("output:%v input:%v \n", len(outputFiles), inputFilesCount)

	return len(outputFiles) == inputFilesCount
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	cleanUpOutput()

	c := Coordinator{}
	c.nReduceCount = nReduce
	fmt.Printf("start nReduce with %v \n", nReduce)
	c.tasks = make(map[string][]Task)
	inputFilesCount = len(files)
	for _, filename := range files {
		c.tasks[filename] = make([]Task, 2)
		c.tasks[filename][0] = Task{taskID: filename, taskType: "map", taskStatus: "not started", assignedWorkerId: -1}
		c.tasks[filename][1] = Task{taskID: filename, taskType: "reduce", taskStatus: "not started", assignedWorkerId: -1}
	}
	c.workers = make(map[int]bool)
	fmt.Printf("Tasks ready %v \n", c.tasks)

	c.server(sockname)
	return &c
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

func (c *Coordinator) checkIfAllMapsDone() {
	for _, task := range c.tasks {
		for _, t := range task {
			if t.taskType == "map" && t.taskStatus != "completed" {
				c.isMapDone = false
				return
			}
		}
	}
	c.isMapDone = true
}

func cleanUpOutput() {
	outputFiles, err := filepath.Glob("mr-out-*")
	if err != nil {
		log.Fatalf("error globbing mr-out-* files: %v", err)
	}

	for _, f := range outputFiles {
		if err := os.Remove(f); err != nil {
			log.Fatalf("error removing %v: %v", f, err)
		}
	}
}