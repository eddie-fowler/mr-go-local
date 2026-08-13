package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"runtime"
	"sort"
	"sync"
	"time"
)

type Coordinator struct {
	MainDir				string
	IntermediateResults []KeyValue
	ReducerCount		int
	Tasks				[]Task
	IsReduceGenerated	bool
	Mutex				sync.Mutex
}

type Task struct {
	Id 					string
	Type				string //map, reduce
	Status				string //not started, in progress, completed, skipped 
	AssignedWorkerId	int
	ReadLocation		string
	WriteLocation		string
	Retries				int
	LastUpdated			time.Time
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	taskIndex := -1
	for i, t := range c.Tasks {
		if t.Status == "not started" && t.AssignedWorkerId == -1 {
			taskIndex = i
			break
		}
	}

	if taskIndex == -1 {
		reply.Task = Task{}
		return nil
	}

	c.Tasks[taskIndex].Status = "in progress"
	c.Tasks[taskIndex].AssignedWorkerId = args.WorkerId
	c.Tasks[taskIndex].LastUpdated = time.Now()

	reply.Task = c.Tasks[taskIndex]

	return nil
}

func (c *Coordinator) UpdateTaskStatus(args *UpdateTaskStatusArgs, reply *UpdateTaskStatusReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for i, t := range c.Tasks {
		if (t.Id == args.TaskId){
			c.IntermediateResults = append(c.IntermediateResults, args.Intermediate...)

			c.Tasks[i].Status = args.Status
			c.Tasks[i].WriteLocation = args.WriteLocation
			c.Tasks[i].Retries = args.Retries
			c.Tasks[i].LastUpdated = time.Now()
			break
		}
	}

	generateReduceTasksIfApplicable(c)
	
	return nil
}


//This is called on loop by coordinator setups break dependency on struct state to avoid race conditions
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()

	for i := range c.Tasks {
		setSkippedIfApplicable(i, c.Tasks)
		setNotStartedIfApplicable(i, c.Tasks)

		if c.Tasks[i].Status != "completed" && c.Tasks[i].Status != "skipped" {
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
	setMainDir(&c)

	cleanUp(&c)
	initializeMapTasks(&c, files)
	initializeCoordinator(&c, nReduce)
	fmt.Printf("tasks %v \n", c.Tasks)

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

func setMainDir(coordinator *Coordinator){
	_, sourceFile, _, ok := runtime.Caller(0)
	if !ok {
		log.Fatalf("Couldn't find source file")
	}

	dir := filepath.Dir(sourceFile)
	dir = filepath.Join(dir, "../main")

	coordinator.MainDir = dir
	fmt.Printf("main dir %v \n", coordinator.MainDir)
}

func initializeMapTasks(coordinator *Coordinator, files []string){
	coordinator.Tasks = make([]Task, 0)
	for i, f := range files {
		// fileNameOnly := filepath.Base(f)
		t := Task{Id: fmt.Sprintf("%v", i), Type: "map", Status: "not started", AssignedWorkerId: -1, ReadLocation: f, WriteLocation: filepath.Join(coordinator.MainDir, fmt.Sprintf("mr-%v", i)), Retries: 0, LastUpdated: time.Now()}
		coordinator.Tasks = append(coordinator.Tasks, t)
	}
}

func initializeReduceTasks(coordinator *Coordinator, partitions map[int][]KeyValue){
	for p := range partitions {
		readLocation := store(coordinator, p, partitions[p])
		coordinator.Tasks = append(coordinator.Tasks, Task{Id: 
			fmt.Sprintf("p-%v", p), 
			Type: "reduce", 
			Status: "not started", 
			AssignedWorkerId: -1, 
			ReadLocation: readLocation, 
			WriteLocation: filepath.Join(coordinator.MainDir, fmt.Sprintf("mr-out-%v", p)),
			Retries: 0,
			LastUpdated: time.Now()})
	}
}

func initializeCoordinator(coordinator *Coordinator, reducerCount int){
	coordinator.IsReduceGenerated = false
	coordinator.IntermediateResults = make([]KeyValue, 0)
	coordinator.ReducerCount = reducerCount
}

func generateReduceTasksIfApplicable(coordinator *Coordinator) {
	if !coordinator.IsReduceGenerated {
		isAllMappingDone := true 
		for _, t := range coordinator.Tasks {
			if t.Type == "map" && t.Status != "completed" {
				isAllMappingDone = false
				break
			}
		}
		if isAllMappingDone {
			coordinator.IsReduceGenerated = true
			
			partitions := shuffle(coordinator)
			fmt.Printf("partitions %v \n", len(partitions))
			initializeReduceTasks(coordinator, partitions)
			clear(coordinator.IntermediateResults)
		}
	}
}

func shuffle(coordinator *Coordinator) map[int][]KeyValue {
	sort.Sort(ByKey(coordinator.IntermediateResults))

	partitions := make(map[int][]KeyValue)
	for _, kv := range coordinator.IntermediateResults {
		partition := ihash(kv.Key) % coordinator.ReducerCount
		if partitions[partition] == nil {
			partitions[partition] = make([]KeyValue, 0)
			partitions[partition] = append(partitions[partition], kv)
		} else {
			partitions[partition] = append(partitions[partition], kv)
		}
	}

	return partitions
}

func store(coordinator *Coordinator, i int, partition []KeyValue) string {
	writeLocation := filepath.Join(coordinator.MainDir, fmt.Sprintf("p-%v", i))
	pfile, _ := os.Create(writeLocation)

	for _, kv := range partition {
		fmt.Fprintf(pfile, "%v %v\n", kv.Key, kv.Value)
	}

	pfile.Close()

	return writeLocation
}

func cleanUp(coordinator *Coordinator) {
	patterns := []string{"mr-*", "p-*"}
	for _, pattern := range patterns {
		matches, err := filepath.Glob(filepath.Join(coordinator.MainDir, pattern))
		if err != nil {
			log.Printf("glob %v failed err %v", pattern, err)
			continue
		}
		for _, f := range matches {
			os.Remove(f)
		}
	}
}

func setSkippedIfApplicable(i int, tasks []Task){
	if tasks[i].Status == "in progress" && tasks[i].Retries > 3 {
		tasks[i].AssignedWorkerId = -1
		tasks[i].Status = "skipped"
		tasks[i].LastUpdated = time.Now()
	}
}

func setNotStartedIfApplicable(i int, tasks []Task){
	if tasks[i].Status == "in progress" && time.Since(tasks[i].LastUpdated).Seconds() > 5 {
		tasks[i].AssignedWorkerId = -1
		tasks[i].Status = "not started"
		tasks[i].Retries += 1
		tasks[i].LastUpdated = time.Now()
	}
}