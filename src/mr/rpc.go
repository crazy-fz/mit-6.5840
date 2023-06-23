package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

const (
	MapType    string = "MapType"
	ReduceType string = "ReduceType"
	NonType    string = "NonType"
)

type WorkerCommand string

const (
	WorkerCmdProcess = "WorkerCmdProcess"
	WorkerCmdWait    = "WorkerCmdWait"
	WorkerCmdDone    = "WorkerCmdDone"
)

// Add your RPC definitions here.
type GetTaskArgs struct {
	Pid       int    // for debug
	RequestId string // for debug
}

type GetTaskReply struct {
	Cmd       WorkerCommand
	TaskType  string // MapType or ReduceType. If map-reduce is finished, reply with NonType.
	FilePath  string // input file for MapType, or intermediate file for ReduceType.
	TaskIndex int    // X for Mapper, Y for Reducer.
	NMapTask  int    // number of map files, each map file correspond to a map task
	NReduce   int    // number of intermediate buckets
}

type FinishTaskArgs struct {
	Pid       int
	TaskType  string
	TaskIndex int    // X for Mapper, Y for Reducer.
	RequestId string // for debug
}

type FinishTaskReply struct{} // empty

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
