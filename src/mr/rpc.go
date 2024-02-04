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

// Add your RPC definitions here.

type Args struct {
	TaskType int      // 任务类型 0:map 1:reduce 2:wait 3:exit
	Index    int      // 任务ID
	Files    []string // 任务文件
}

type Reply struct {
	Tasktype int      // 任务类型 0:map 1:reduce 2:wait 3:exit
	Index    int      // 任务ID
	Files    []string // 任务文件
	Nreduce  int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
