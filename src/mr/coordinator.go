package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

/*
	Coordinator's responsibility is to
	1. Give tasks to workers upon request
	2. Keep track of which tasks are completed and which are not
	3. Keep track of which workers are alive and which are dead
	4. Reassign tasks to workers if a worker dies before completing the task
*/
type Coordinator struct {
	// Your definitions here.
	tasks 	map[string]Task //map of taskId to task
	workers map[int]bool //map of workerId to worker status (alive or dead)
}

type Task struct {
	taskID string //filename
	taskStatus string //not started, in progress, completed, skipped
	assignedWorkerId int //index of worker assigned to task
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	fmt.Printf("Task request from worker \n")

	nextTask := Task{}
	for _, task := range c.tasks {
		if task.taskStatus == "not started" {
			nextTask = task
			break
		}
	}
	nextTask.assignedWorkerId = args.WorkerId
	nextTask.taskStatus = "in progress"

	fmt.Printf("Allocating task: %v to worker: %v \n", nextTask.taskID, nextTask.assignedWorkerId)

	reply.Filename = nextTask.taskID

	return nil
}

func (c *Coordinator) UpdateWorkerStatus(args *UpdateWorkerStatusArgs, reply *UpdateWorkerStatusReply) error {
	c.workers[args.WorkerId] = (args.Status == "alive")

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.tasks[args.TaskID] = Task{taskID: args.TaskID, taskStatus: args.Status, assignedWorkerId: args.AssignedWorkerId}

	return nil
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, task := range c.tasks {
		if task.taskStatus != "completed" && task.taskStatus != "skipped" {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.tasks = make(map[string]Task)
	for _, filename := range files {
		c.tasks[filename] = Task{taskID: filename, taskStatus: "not started", assignedWorkerId: -1}
	}
	c.workers = make(map[int]bool)

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