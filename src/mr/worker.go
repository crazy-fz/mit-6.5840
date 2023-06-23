package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
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

func fmtIntermediateFile(x, y int) string {
	return fmt.Sprintf("mr-%d-%d", x, y)
}
func fmtTmpIntermediateFile(x, y, pid int) string {
	return fmt.Sprintf("mr-%d-%d.%d", x, y, pid)
}

func fmtOutPutFile(y int) string {
	return fmt.Sprintf("mr-out-%d", y)
}
func fmtTmpOutPutFile(y, pid int) string {
	return fmt.Sprintf("mr-tmp-out-%d.%d", y, pid)
}

type MapFunc func(string, string) []KeyValue
type ReduceFunc func(string, []string) string
type WorkerFunc func(MapFunc, ReduceFunc, *GetTaskReply) bool

func MapWorker(mapf MapFunc, reducef ReduceFunc, reply *GetTaskReply) (canExit bool) {
	// Read contents.
	file, err := os.Open(reply.FilePath)
	if err != nil {
		log.Fatalf("cannot open %s", reply.FilePath)
		return false
	}
	defer file.Close()
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %s", reply.FilePath)
		return false
	}

	intermediate := mapf(reply.FilePath, string(content))

	// Create tmp files to write intermediate content.
	arrEncoder := make([]*json.Encoder, reply.NReduce)
	pid := os.Getpid()
	for i := 0; i < reply.NReduce; i++ {
		tmpFile, err := os.OpenFile(
			fmtTmpIntermediateFile(reply.TaskIndex, i, pid),
			os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0644,
		)
		if err != nil {
			log.Fatalf("%s", err.Error())
			return false
		}
		defer tmpFile.Close()
		arrEncoder[i] = json.NewEncoder(tmpFile)
	}
	writeKV := func(hashIndex int, kv *KeyValue) error {
		return arrEncoder[hashIndex].Encode(kv)
	}

	// append all content to tmp files
	var hashIndex int
	for i, kv := range intermediate {
		if i == 0 || kv.Key != intermediate[i-1].Key {
			hashIndex = ihash(kv.Key) % reply.NReduce
		}
		if err := writeKV(hashIndex, &kv); err != nil {
			log.Fatalf("%s", err.Error())
			return false
		}
	}

	return false
}
func ReduceWorker(mapf MapFunc, reducef ReduceFunc, reply *GetTaskReply) (canExit bool) {
	// open files and read all intermediate content.
	intermediate := make([]KeyValue, 0)
	pid := os.Getpid()
	for i := 0; i < reply.NMapTask; i++ {
		file, err := os.Open(fmtIntermediateFile(i, reply.TaskIndex))
		if err != nil {
			log.Fatalf("%s", err.Error())
			return false
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			err := decoder.Decode(&kv)
			if err != nil {
				if err == io.EOF {
					break
				}
				log.Fatal(err.Error())
				return false
			}
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(intermediate))

	// open tmp file to write output
	file, err := os.OpenFile(
		fmtTmpOutPutFile(reply.TaskIndex, pid),
		os.O_WRONLY|os.O_CREATE|os.O_APPEND|os.O_TRUNC, 0644,
	)
	if err != nil {
		log.Fatal(err.Error())
		return false
	}
	log.Printf("writing to file: %s", fmtTmpOutPutFile(reply.TaskIndex, pid))
	defer file.Close()
	writeOutput := func(key, value string) {
		file.WriteString(key)
		file.WriteString(" ")
		file.WriteString(value)
		file.WriteString("\n")
	}
	curArr := make([]string, 0)
	for i, kv := range intermediate {
		if i != 0 && kv.Key != intermediate[i-1].Key {
			result := reducef(intermediate[i-1].Key, curArr)
			writeOutput(intermediate[i-1].Key, result)
			curArr = make([]string, 0)
		}
		curArr = append(curArr, kv.Value)
	}
	if len(curArr) != 0 {
		lastKey := intermediate[len(intermediate)-1].Key
		result := reducef(lastKey, curArr)
		writeOutput(lastKey, result)
	}
	return false
}

// In this case, all tasks are done.
func NonTypeWorker(mapf MapFunc, reducef ReduceFunc, reply *GetTaskReply) (canExit bool) { return true }

var workFuncTable = map[string]WorkerFunc{
	MapType:    MapWorker,
	ReduceType: ReduceWorker,
	NonType:    NonTypeWorker,
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	log.SetOutput(ioutil.Discard) // disable debug log
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if ok := call("Coordinator.GetTask", &args, &reply); !ok {
			// No retry strategy.
			return
		}
		log.Println(reply)
		if reply.Cmd == WorkerCmdWait {
			// time.Sleep(time.Microsecond * 10)
			continue
		} else if reply.Cmd == WorkerCmdDone {
			os.Exit(0)
		}
		workerFunc := workFuncTable[reply.TaskType]
		if workerFunc == nil {
			log.Fatalf("coordinator return unkown TaskType: %s", reply.TaskType)
			return
		}
		if canExit := workerFunc(mapf, reducef, &reply); canExit {
			return
		}
		finishArgs := FinishTaskArgs{
			Pid:       os.Getpid(),
			TaskType:  reply.TaskType,
			TaskIndex: reply.TaskIndex,
		}
		finishReply := FinishTaskReply{}
		call("Coordinator.FinishTask", &finishArgs, &finishReply)
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

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
