package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"
)

const (
	WaitingMap int = iota
	WaitingReduce
	Finish
)

const (
	TaskStateIdle int = iota
	TaskStateRunning
	TaskStateDone
)

const TaskTimeout = 10 * time.Second

type Coordinator struct {
	// Your definitions here.
	state     int
	tasksChan chan Task
	nReduce   int
	files     []string
	completed int
	// The coordinator, as an RPC server, will be concurrent; don't forget to lock shared data
	mu sync.Mutex

	// Add task tracking
	mapTasks    map[int]Task
	reduceTasks map[int]Task
}

type Task interface { // interface类型已经是指针了
	GetTaskType() int
	GetID() int
	GetTaskState() int
	SetTaskState(state int)
	GetStartTime() time.Time
	SetStartTime(time.Time)
}

type MapTask struct {
	// ​​RPC 无法传递函数​​（gob 不能序列化函数）
	taskID    int
	fileName  string
	nReduce   int
	taskState int
	startTime time.Time
}

type ReduceTask struct {
	taskID    int
	files     []string
	nReduce   int
	taskState int
	startTime time.Time
}

func (m *MapTask) GetTaskType() int {
	return 0
}

func (m *MapTask) GetID() int {
	return m.taskID
}

func (m *MapTask) GetTaskState() int {
	return m.taskState
}

func (m *MapTask) SetTaskState(state int) {
	m.taskState = state
}

func (m *MapTask) GetStartTime() time.Time {
	return m.startTime
}

func (m *MapTask) SetStartTime(t time.Time) {
	m.startTime = t
}

func (r *ReduceTask) GetTaskType() int {
	return 1
}

func (r *ReduceTask) GetID() int {
	return r.taskID
}

func (r *ReduceTask) GetTaskState() int {
	return r.taskState
}

func (r *ReduceTask) SetTaskState(state int) {
	r.taskState = state
}

func (r *ReduceTask) GetStartTime() time.Time {
	return r.startTime
}

func (r *ReduceTask) SetStartTime(t time.Time) {
	r.startTime = t
}

// chan 的局限性​​
// chan 只能保证​​通道自身的发送/接收操作​​是线程安全的。
// 它无法保护通道​​外部的共享状态​​（如修改 State 或 completed）。

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) TaskHandler(args *Args, reply *Reply) error {
	// Don't lock before receiving from channel to allow parallel task assignment
	task, ok := <-c.tasksChan // 检查通道是否关闭
	if !ok {
		reply.AllFinish = true
		return nil
	}

	c.mu.Lock()
	defer c.mu.Unlock()

	// log.Printf("Worker (pid: %d) got task: %d (ok=%v)", args.WorkerPID, task.GetID(), ok)

	task.SetTaskState(TaskStateRunning)
	task.SetStartTime(time.Now())

	reply.TaskID = task.GetID()
	reply.TaskType = task.GetTaskType()
	reply.NReduce = c.nReduce
	if reply.TaskType == 0 {
		reply.FileName = task.(*MapTask).fileName
	} else {
		reply.Files = task.(*ReduceTask).files
	}
	reply.AllFinish = false

	return nil
}

func (c *Coordinator) ReportHandler(args *ReportArgs, reply *ReportReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	if args.Done {
		if args.TaskType == 0 {
			if c.mapTasks[args.TaskID].GetTaskState() == TaskStateRunning {
				c.mapTasks[args.TaskID].SetTaskState(TaskStateDone)
				c.completed++
			}
		} else {
			if c.reduceTasks[args.TaskID].GetTaskState() == TaskStateRunning {
				c.reduceTasks[args.TaskID].SetTaskState(TaskStateDone)
				c.completed++
			}
		}
	} else {
		// 失败的任务重新加入队列
		if args.TaskType == 0 {
			c.mapTasks[args.TaskID].SetTaskState(TaskStateIdle)
			c.tasksChan <- c.mapTasks[args.TaskID]
		} else {
			c.reduceTasks[args.TaskID].SetTaskState(TaskStateIdle)
			c.tasksChan <- c.reduceTasks[args.TaskID]
		}
	}

	// fmt.Println("Task completed:", args.Task.GetID(), "State:", args.Task.GetTaskState())
	// fmt.Println("Total completed tasks:", c.completed)

	if c.state == WaitingMap && c.completed == len(c.files) {
		c.state = WaitingReduce
		c.completed = 0

		// Dispatch Reduce tasks
		for i := 0; i < c.nReduce; i++ {
			// 在创建Reduce任务前收集文件
			files, err := filepath.Glob(fmt.Sprintf("mr-*-%d", i))
			if err != nil {
				return fmt.Errorf("glob failed: %v", err)
			}

			task := &ReduceTask{
				taskID:    i,
				files:     files,
				nReduce:   c.nReduce,
				taskState: TaskStateIdle,
			}
			c.tasksChan <- task
			c.reduceTasks[i] = task
			// fmt.Println("Reduce task created:", task.TaskID)
		}
	} else if c.state == WaitingReduce && c.completed == c.nReduce {
		c.state = Finish
		close(c.tasksChan) // 安全关闭通道
		// fmt.Println("All tasks completed.")
	}

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
	c.mu.Lock()
	defer c.mu.Unlock()
	ret := false

	if c.state == Finish {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	// Your code here.
	// Each mapper should create nReduce intermediate files for consumption by the reduce tasks.
	taskChanSize := 0
	if len(files) > nReduce {
		taskChanSize = len(files)
	} else {
		taskChanSize = nReduce
	}

	c := Coordinator{
		state:       WaitingMap,
		tasksChan:   make(chan Task, taskChanSize), // 无缓冲通道
		nReduce:     nReduce,
		files:       files,
		completed:   0,
		mapTasks:    make(map[int]Task),
		reduceTasks: make(map[int]Task),
	}

	// 将文件分配给 Map 任务
	for i, file := range files {
		task := &MapTask{
			taskID:    i,
			fileName:  file,
			nReduce:   c.nReduce,
			taskState: TaskStateIdle,
		}
		c.tasksChan <- task
		c.mapTasks[i] = task
	}

	c.server()
	go c.checkTimeouts()
	return &c
}

func (c *Coordinator) checkTimeouts() {
	for {
		time.Sleep(time.Second)
		c.mu.Lock()
		if c.state == Finish {
			c.mu.Unlock()
			return
		}

		now := time.Now()

		// 1. 检查 Map 任务超时
		if c.state == WaitingMap {
			for _, task := range c.mapTasks {
				if task.GetTaskState() == TaskStateRunning {
					if now.Sub(task.GetStartTime()) > TaskTimeout {
						task.SetTaskState(TaskStateIdle)
						c.tasksChan <- task
						// fmt.Printf("Map task %d timed out, re-queueing\n", task.GetID())
					}
				}
			}
		}

		// 2. 检查 Reduce 任务超时
		if c.state == WaitingReduce {
			for _, task := range c.reduceTasks {
				if task.GetTaskState() == TaskStateRunning {
					if now.Sub(task.GetStartTime()) > TaskTimeout {
						task.SetTaskState(TaskStateIdle)
						c.tasksChan <- task
						fmt.Printf("Reduce task %d timed out, re-queueing\n", task.GetID())
					}
				}
			}
		}
		c.mu.Unlock()
	}
}
