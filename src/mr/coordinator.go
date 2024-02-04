package mr

import (
	"errors"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"regexp"
	"strconv"
	"sync"
)

type Task struct {
	Status      int // 0:untouched  1:processing 2:done
	Files       []string
	ProcessTime int // 处于processing的时间
}

type Coordinator struct {
	// Your definitions here.
	Mtasks []Task
	Rtasks []Task

	Remain_mtasks int
	Remain_rtasks int

	lock sync.Mutex

	Nreduce int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func GetBucketFromFilename(file string) int {
	re := regexp.MustCompile(`(\d+)`)
	match := re.FindAllString(file, -1)
	number := match[len(match)-1]
	num, _ := strconv.Atoi(number)
	return int(num)
}

// 上锁
func (c *Coordinator) CompleteTask(args *Args, reply *Reply) error {
	index := args.Index
	tasktype := args.TaskType
	files := args.Files

	c.lock.Lock()
	defer c.lock.Unlock()

	// map任务完成
	if tasktype == 0 {
		log.Default().Printf("coordinator: map task done, taskid = %d", index)
		if c.Mtasks[index].Status == 2 {
			return nil
		}

		c.Remain_mtasks--
		c.Mtasks[index].Status = 2
		for _, file := range files {
			bucket := GetBucketFromFilename(file)
			c.Rtasks[bucket].Files = append(c.Rtasks[bucket].Files, file)
		}

		// reduce任务完成
	} else if tasktype == 1 {
		log.Default().Printf("coordinator: reduce task done, taskid = %d", index)

		if c.Rtasks[index].Status == 2 {
			return nil
		}
		c.Remain_rtasks--
		c.Rtasks[index].Status = 2
	}

	return nil
}

// return untouched task
func (c *Coordinator) GetFreeTask(tasks []Task) (int, error) {
	for i, t := range tasks {
		if t.Status == 0 {
			return i, nil
		}
	}
	return -1, errors.New("no free task")
}

// 上锁
func (c *Coordinator) AssignTask(args *Args, reply *Reply) error {
	// 先处理map任务,后处理reduce任务，如果map任务未完成且没有空闲，则返回等待

	c.lock.Lock()
	defer c.lock.Unlock()

	// map任务未处理完成
	if c.Remain_mtasks != 0 {
		i, err := c.GetFreeTask(c.Mtasks)

		log.Default().Printf("coordinator: map task, remain_matask = %d, free task id = %d", c.Remain_mtasks, i)

		if err != nil {
			// map任务已经全部分配，等待
			reply.Tasktype = 2
		} else {
			t := c.Mtasks[i]
			// 分配map任务
			reply.Tasktype = 0
			reply.Index = i
			reply.Files = t.Files
			reply.Nreduce = c.Nreduce
			c.Mtasks[i].Status = 1
		}
		// reduce任务未处理完成
	} else if c.Remain_rtasks != 0 {

		i, err := c.GetFreeTask(c.Rtasks)

		log.Default().Printf("coordinator: reduce task, remain_ratask = %d, free task id = %d", c.Remain_rtasks, i)

		if err != nil {
			// reduce任务已经全部分配，等待
			reply.Tasktype = 2
		} else {
			// 分配reduce任务
			reply.Tasktype = 1
			reply.Index = i
			reply.Files = c.Rtasks[i].Files
			reply.Nreduce = c.Nreduce

			c.Rtasks[i].Status = 1
		}
	} else {
		reply.Tasktype = 3
	}

	return nil
}

// start a thread that listens for RPCs from worker.go
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

func (c *Coordinator) UpdatePTime() {
	for i := 0; i < len(c.Mtasks); i++ {
		if c.Mtasks[i].Status != 1 {
			continue
		}
		c.Mtasks[i].ProcessTime++
		log.Default().Printf("coordinator: map task id = %d, processing time = %d", i, c.Mtasks[i].ProcessTime)
		if c.Mtasks[i].ProcessTime == 10 {
			c.Mtasks[i].Status = 0
			c.Mtasks[i].ProcessTime = 0
		}
	}

	for i := 0; i < len(c.Rtasks); i++ {
		if c.Rtasks[i].Status != 1 {
			continue
		}
		c.Rtasks[i].ProcessTime++
		log.Default().Printf("coordinator: reduce task id = %d, processing time = %d", i, c.Rtasks[i].ProcessTime)
		if c.Rtasks[i].ProcessTime == 10 {
			c.Rtasks[i].Status = 0
			c.Rtasks[i].ProcessTime = 0
		}
	}
}

// 上锁
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	c.lock.Lock()
	defer c.lock.Unlock()

	c.UpdatePTime()

	if c.Remain_mtasks+c.Remain_rtasks == 0 {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	log.SetOutput(ioutil.Discard)
	log.Default().Printf("coordinator: file number = %d", len(files))

	c.Remain_mtasks = len(files)
	c.Remain_rtasks = nReduce
	c.Nreduce = nReduce
	// print("files number: ", len(files), "\n")
	for _, file := range files {
		c.Mtasks = append(c.Mtasks, Task{0, []string{file}, 0})
	}

	i := 0
	for i < nReduce {
		c.Rtasks = append(c.Rtasks, Task{0, []string{}, 0})
		i++
	}
	c.server()
	return &c
}
