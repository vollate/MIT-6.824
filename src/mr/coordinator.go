package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

const (
	Running = iota
	Idle
	Crash
)

type TaskQueue struct {
	queue []interface{}
	mutex sync.Mutex
}

func InitTaskQueue(length int) *TaskQueue {
	res := &TaskQueue{}
	res.queue = make([]interface{}, length)
	for i := range res.queue {
		res.queue[i] = i
	}
	return res
}

func (q *TaskQueue) Push(task interface{}) {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	q.queue = append(q.queue, task)
}

func (q *TaskQueue) Pop() interface{} {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	if len(q.queue) == 0 {
		return nil
	}
	task := q.queue[0]
	q.queue = q.queue[1:]
	return task
}

func (q *TaskQueue) Len() int {
	q.mutex.Lock()
	defer q.mutex.Unlock()
	return len(q.queue)
}

type WorkerStatus struct {
	timeout   time.Duration
	statusMap map[int]time.Time
	mutex     sync.Mutex
}

func (s *WorkerStatus) remove(workerId int) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.statusMap, workerId)
}

func (s *WorkerStatus) updateTimeStamp(workerId int, timeStamp time.Time) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	s.statusMap[workerId] = timeStamp
}

func (s *WorkerStatus) updateTimeStampStr(workerId int, timeStampStr string) {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	parsedTime, err := time.Parse(time.RFC3339, timeStampStr)
	if err != nil {
		log.Fatal(err)
	}
	s.statusMap[workerId] = parsedTime
}

func (s *WorkerStatus) checkTimeout() []int {
	s.mutex.Lock()
	defer s.mutex.Unlock()
	res := []int{}
	now := time.Now()
	for k, v := range s.statusMap {
		if now.Sub(v) > s.timeout {
			res = append(res, k)
		}
	}
	return res
}

type FileList struct {
	Files []string
	mutex sync.Mutex
}

func (f *FileList) Assign(file *[]string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.Files = *file
}

func (f *FileList) All() []string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.Files
}

func (f *FileList) Get(index int) string {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	return f.Files[index]
}

func (f *FileList) Append(file string) {
	f.mutex.Lock()
	defer f.mutex.Unlock()
	f.Files = append(f.Files, file)
}

func initFileList(files *[]string) *FileList {
	res := &FileList{}
	res.Files = *files
	return res
}

type Coordinator struct {
	mapFiles      FileList
	reduceFiles   []FileList
	mapTasks      TaskQueue
	reduceTasks   TaskQueue
	mapAlive      WorkerStatus
	reduceAlive   WorkerStatus
	mapStatus     []atomic.Bool
	reduceStatus  []atomic.Bool
	allMapDone    atomic.Bool
	allReduceDone atomic.Bool
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	if c.mapTasks.Len() > 0 {
		reply.WorkerId = c.mapTasks.Pop().(int)
		reply.FilePath = append(reply.FilePath, c.mapFiles.Get(reply.WorkerId))
		reply.TaskType = Map
		reply.NReduce = len(c.reduceStatus)
		c.mapAlive.updateTimeStamp(reply.WorkerId, time.Now())
	} else if c.allMapDone.Load() && c.reduceTasks.Len() > 0 {
		reply.WorkerId = c.reduceTasks.Pop().(int)
		reply.FilePath = c.reduceFiles[reply.WorkerId].All()
		reply.TaskType = Reduce
		c.reduceAlive.updateTimeStamp(reply.WorkerId, time.Now())
	} else if !c.doneImpl() {
		reply.TaskType = Wait
	} else {
		reply.TaskType = Terminate
	}
	return nil
}

func (c *Coordinator) Alive(args *AliveArgs, reply *AliveReply) error {
	if args.TaskType == Map {
		c.mapAlive.updateTimeStampStr(args.WorkerId, args.TimeStamp)
	} else if args.TaskType == Reduce {
		c.reduceAlive.updateTimeStampStr(args.WorkerId, args.TimeStamp)
	}
	reply.TimeStamp = time.Now().Format(time.RFC3339)
	return nil
}

func (c *Coordinator) Finish(args *FinishArgs, reply *FinishReply) error {
	if args.TaskType == Map {
		for _, file := range args.ResultFiles {
			index, err := strconv.Atoi(string(file[len(file)-1]))
			if err != nil {
				log.Fatal(err)
			} else {
				c.reduceFiles[index].Append(file)
			}
		}
		c.mapAlive.remove(args.WorkerId)
		c.mapStatus[args.WorkerId].Store(true)
		c.allMapDone.Store(allTasksDone(&c.mapStatus))
	} else if args.TaskType == Reduce {
		c.reduceAlive.remove(args.WorkerId)
		c.reduceStatus[args.WorkerId].Store(true)
		c.allReduceDone.Store(allTasksDone(&c.reduceStatus))
	} else {
		fmt.Println("Error: unknown work type: ", args.TaskType)
	}
	return nil
}

func (c *Coordinator) server() {
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
func (c *Coordinator) doneImpl() bool {
	return c.allMapDone.Load() && c.allReduceDone.Load()
}

func (c *Coordinator) Done() bool {
	time.Sleep(2 * time.Second) // wait for all workers to exit
	return c.doneImpl()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func initCoordinator(files *[]string, nReduce int) *Coordinator {
	c := &Coordinator{}
	c.mapFiles = *initFileList(files)
	c.reduceFiles = make([]FileList, nReduce)
	c.mapTasks = *InitTaskQueue(len(*files))
	c.reduceTasks = *InitTaskQueue(nReduce)
	c.mapAlive.statusMap = make(map[int]time.Time)
	c.reduceAlive.statusMap = make(map[int]time.Time)
	c.mapStatus = make([]atomic.Bool, len(*files))
	c.reduceStatus = make([]atomic.Bool, nReduce)
	c.mapAlive.timeout = 10 * time.Second
	c.reduceAlive.timeout = 10 * time.Second
	return c
}

func (c *Coordinator) checkAlive() {
	for !c.doneImpl() {
		mapAdd := c.mapAlive.checkTimeout()
		reduceAdd := c.reduceAlive.checkTimeout()
		for _, val := range mapAdd {
			c.mapTasks.Push(val)
		}
		for _, val := range reduceAdd {
			c.reduceTasks.Push(val)
		}
		time.Sleep(time.Second)
	}
}

func allTasksDone(record *[]atomic.Bool) bool {
	for i := range *record {
		if !(*record)[i].Load() {
			return false
		}
	}
	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := initCoordinator(&files, nReduce)
	go c.checkAlive()
	c.server()
	return c
}
