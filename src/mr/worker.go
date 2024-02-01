package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

func saveKeyValues(keyValues []KeyValue, workerId string, nReduce int) []string {
	var files = make([]string, nReduce)
	for i := range files {
		files[i] = "mr-" + workerId + "-" + strconv.Itoa(i)
	}
	tmpFiles := make([]*os.File, nReduce)
	tmpEncoders := make([]*json.Encoder, nReduce)
	for i := range tmpFiles {
		tmpFile, err := os.CreateTemp("./", "map-tmp")
		if err != nil {
			log.Fatal(err)
			return nil
		}
		tmpFiles[i] = tmpFile
		tmpEncoders[i] = json.NewEncoder(tmpFile)
	}
	for _, kv := range keyValues {
		reduceId := ihash(kv.Key) % nReduce
		err := tmpEncoders[reduceId].Encode(&kv)
		if err != nil {
			log.Fatal(err)
		}
	}
	for i := range tmpFiles {
		tmpFiles[i].Close()
		os.Rename(tmpFiles[i].Name(), files[i])
	}
	return files
}

func readKeyValues(files []string) []KeyValue {
	var res []KeyValue
	tryTimes := 0
	size := len(files)
	for len(files) > 0 {
		for _, path := range files {
			file, err := os.Open(path)
			if err != nil {
				log.Fatal(err)
				files = append(files[1:], files[0])
				if tryTimes > size {
					time.Sleep(time.Second)
					tryTimes = size
				}
				tryTimes++
				continue
			} else {
				files = files[1:]
			}
			decoder := json.NewDecoder(file)
			var kv KeyValue
			for {
				if err := decoder.Decode(&kv); err != nil {
					break
				}
				res = append(res, kv)
			}
		}
	}
	return res
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	idle := true
	run := true
	ch := make(chan bool)
	var workerId, taskType int
	var finishFiles []string
	for run {
		if idle {
			reply := callGetTask()
			workerId = reply.WorkerId
			taskType = reply.TaskType
			if reply.TaskType == Map {
				idle = false
				go func() {
					content, err := os.ReadFile(reply.FilePath[0])
					if err != nil {
						log.Fatal(err)
					}
					kvRes := mapf(reply.FilePath[0], string(content))
					finishFiles = saveKeyValues(kvRes, strconv.Itoa(workerId), reply.NReduce)
					ch <- true
				}()
			} else if reply.TaskType == Reduce {
				idle = false
				go func() {
					input := readKeyValues(reply.FilePath)
					sort.Sort(ByKey(input))
					i := 0
					ofile, _ := os.Create("mr-out-" + strconv.Itoa(workerId))
					for i < len(input) {
						j := i + 1
						for j < len(input) && input[j].Key == input[i].Key {
							j++
						}
						values := []string{}
						for k := i; k < j; k++ {
							values = append(values, input[k].Value)
						}
						output := reducef(input[i].Key, values)

						fmt.Fprintf(ofile, "%v %v\n", input[i].Key, output)
						i = j
					}
					ch <- true
				}()
			} else if reply.TaskType == Wait {
				idle = true
				time.Sleep(time.Second)
			} else if reply.TaskType == Terminate {
				return
			} else {
				fmt.Println("Error: unknown work type: ", reply.TaskType)
			}
		} else {
			select {
			case done := <-ch:
				if done {
					callFinish(workerId, taskType, finishFiles)
					idle = true
				}
			default:
				callAlive(workerId, taskType)
				time.Sleep(time.Second)
			}
		}
	}
	// uncomment to send the Example RPC to the coordinator.
}

func callGetTask() GetTaskReply {
	args := GetTaskArgs{}
	var reply GetTaskReply
	call("Coordinator.GetTask", &args, &reply)
	return reply
}

func callAlive(workerId int, taskType int) {
	args := AliveArgs{workerId, taskType, time.Now().Format(time.RFC3339)}
	var reply AliveReply
	call("Coordinator.Alive", &args, &reply)
}
func callFinish(workerId int, taskType int, result []string) {
	args := FinishArgs{workerId, taskType, result, time.Now().Format(time.RFC3339)}
	var reply FinishReply
	call("Coordinator.Finish", &args, &reply)
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
