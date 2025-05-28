package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

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

type Args struct {
	WorkerPID int
}

type Reply struct {
	TaskType  int
	TaskID    int
	FileName  string   // Map 任务需要的文件名
	Files     []string // Reduce 任务需要的文件列表
	NReduce   int
	AllFinish bool
}

type ReportArgs struct {
	TaskType int
	TaskID   int
	Done     bool
}

type ReportReply struct {
	Ack bool
}

// Add your RPC definitions here.
type RPC struct {
	WorkerID int
	TaskID   int
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
