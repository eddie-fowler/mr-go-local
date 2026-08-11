package mr
type RequestTaskArgs struct {
	WorkerId int
}

type RequestTaskReply struct {
	Task				Task
}

type UpdateWorkerStatusArgs struct {
	WorkerId int
	Status string
}

type UpdateWorkerStatusReply struct {
}

type UpdateTaskStatusArgs struct {
	TaskId				string
	AssignedWorkerId	int
	WriteLocation	 	string
	Status				string
	Intermediate		[]KeyValue
}

type UpdateTaskStatusReply struct {
}
