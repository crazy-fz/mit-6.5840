package mr

import (
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	mapTask    taskSet
	reduceTask taskSet
	files      []string
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	task, cmd := c.mapTask.GetIdleTask()
	reply.Cmd = cmd
	reply.TaskIndex = task
	reply.NMapTask = c.mapTask.Len()
	reply.NReduce = c.reduceTask.Len()

	if cmd != WorkerCmdDone {
		reply.TaskType = MapType
		if task >= 0 && task < c.mapTask.Len() {
			reply.FilePath = c.files[task]
		}
		log.Println(reply)
		return nil
	}

	task, cmd = c.reduceTask.GetIdleTask()
	reply.TaskIndex = task
	reply.Cmd = cmd
	if cmd != WorkerCmdDone {
		reply.TaskType = ReduceType
		log.Println(reply)
		return nil
	}

	reply.TaskType = NonType
	log.Println(reply)

	return nil
}

func (c *Coordinator) NReduce() int {
	return c.reduceTask.Len()
}

type mvOutputFunc func(c *Coordinator, args *FinishTaskArgs)

func (c *Coordinator) mvMapOutput(args *FinishTaskArgs) {
	if ok := c.mapTask.SetMoving(args.TaskIndex); !ok {
		return
	}
	for i := 0; i < c.NReduce(); i++ {
		if err := os.Rename(
			fmtTmpIntermediateFile(args.TaskIndex, i, args.Pid),
			fmtIntermediateFile(args.TaskIndex, i),
		); err != nil {
			log.Printf("%s", err.Error())
			return
		}
	}
	// log.Printf("map set done: %d", args.TaskIndex)
	c.mapTask.SetDone(args.TaskIndex)
}

func (c *Coordinator) mvReduceOutput(args *FinishTaskArgs) {
	if ok := c.reduceTask.SetMoving(args.TaskIndex); !ok {
		return
	}
	if err := os.Rename(
		fmtTmpOutPutFile(args.TaskIndex, args.Pid),
		fmtOutPutFile(args.TaskIndex),
	); err != nil {
		c.reduceTask.MovingFail(args.TaskIndex)
		log.Printf("%s", err.Error())
		return
	}
	// log.Printf("reduce set done: %d", args.TaskIndex)
	c.reduceTask.SetDone(args.TaskIndex)
}

var mvFuncTable = map[string]mvOutputFunc{
	MapType:    (*Coordinator).mvMapOutput,
	ReduceType: (*Coordinator).mvReduceOutput,
}

func (c *Coordinator) FinishTask(args *FinishTaskArgs, reply *FinishTaskReply) error {
	// async mv
	go func(args *FinishTaskArgs) {
		mvFunc := mvFuncTable[args.TaskType]
		if mvFunc == nil {
			log.Printf("unknown TaskType: %s", args.TaskType)
			return
		}
		mvFunc(c, args)
	}(args)
	log.Printf("map status: %t, reduce status: %t", c.mapTask.Done(), c.reduceTask.Done())
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
	log.SetOutput(ioutil.Discard) // disable debug log
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
	// Your code here.
	return c.reduceTask.Done()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = append(c.files, files...)
	c.mapTask.Init(len(files))
	c.reduceTask.Init(nReduce)
	log.Printf("size map: %d, reduce: %d", c.mapTask.Len(), c.reduceTask.Len())
	c.server()
	return &c
}
