package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

var lock sync.Mutex

type TaskStatus int

const (
	Idle TaskStatus = iota
	InProgress
	Done
)

type State int

const (
	Map State = iota
	Reduce
	Wait
	Exit
)

type Task struct {
	TaskId            int
	MapFilePath       string
	ReduceFilePathArr []string
	OutputFileName    string
	CoordinatorPhase  State
	NReduce           int
}

type TaskMeta struct {
	TaskReference *Task
	TaskStatus    TaskStatus
	TaskStartTime time.Time
}

type Coordinator struct {
	Files            []string
	NReduce          int
	CoordinatorPhase State
	TaskQueue        chan *Task
	TaskMeta         map[int]*TaskMeta
	IntermediatePath [][]string
}

// AssignTask
// Your code here -- RPC handlers for the worker to call.
// 协调者的RPC handler，由worker向coordinator发送获取任务的请求。
func (c *Coordinator) AssignTask(args *Args, task *Task) error {
	if len(c.TaskQueue) > 0 {
		//fmt.Println("work请求任务，当前任务队列长度为：", len(c.TaskQueue))
		*task = *<-c.TaskQueue
		c.TaskMeta[task.TaskId].TaskStatus = InProgress
		//fmt.Printf("work请求任务,请求到任务：%+v", task)
	} else if c.CoordinatorPhase == Exit {
		*task = Task{CoordinatorPhase: Exit}
	} else {
		*task = Task{CoordinatorPhase: Wait}
	}
	return nil
}

// server
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	//fmt.Println("sockname is: ", sockname)
	os.Remove(sockname)
	// 基于文件的方式进行rpc调用
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// Done
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	return c.CoordinatorPhase == Exit
}

func (c *Coordinator) isAllTaskDone() bool {
	for _, kv := range c.TaskMeta {
		if kv.TaskStatus != Done {
			return false
		}
	}
	return true
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:            files,
		NReduce:          nReduce,
		CoordinatorPhase: Map,
		TaskQueue:        make(chan *Task, max(nReduce, len(files))),
		TaskMeta:         make(map[int]*TaskMeta),
		IntermediatePath: make([][]string, nReduce),
	}
	c.createMapTask()
	// 启动线程，监听worker的rpc请求
	c.server()
	return &c
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

// 创建map任务
func (c *Coordinator) createMapTask() {
	for idx, filename := range c.Files {
		task := Task{
			MapFilePath:      filename,
			TaskId:           idx,
			CoordinatorPhase: Map,
			NReduce:          c.NReduce,
		}
		c.TaskQueue <- &task
		c.TaskMeta[idx] = &TaskMeta{TaskReference: &task, TaskStatus: Idle, TaskStartTime: time.Now()}
	}
}

// complete task
// 完成任务
func (c *Coordinator) CompleteTask(task *Task, placeholder *PlaceHolder) error {
	// 加锁
	lock.Lock()
	defer lock.Unlock()
	// 如果task中标识的阶段状态和coordinator当前的阶段状态不一致 || 或者TaskMeta中标识的TaskStatus状态为Done，则需要进行重复处理
	if task.CoordinatorPhase != c.CoordinatorPhase || c.TaskMeta[task.TaskId].TaskStatus == Done {
		return nil
	}
	// 将任务状态设置为已完成
	c.TaskMeta[task.TaskId].TaskStatus = Done
	// 启动另外一个协程
	go c.processTaskResult(task)
	return nil
}

/**
  异步处理任务结束后应该做的事情
*/
func (c *Coordinator) processTaskResult(task *Task) {
	lock.Lock()
	defer lock.Unlock()
	switch task.CoordinatorPhase {
	case Map:
		for reduceTaskId, filePath := range task.ReduceFilePathArr {
			c.IntermediatePath[reduceTaskId] = append(c.IntermediatePath[reduceTaskId], filePath)
		}
		if c.isAllTaskDone() {
			c.createReduceTask()
			c.CoordinatorPhase = Reduce
		}
	case Reduce:
		// reduce任务完成之后，进入exit阶段
		if c.isAllTaskDone() {
			c.CoordinatorPhase = Exit
		}
	}
}

func (c *Coordinator) createReduceTask() {
	c.CoordinatorPhase = Reduce
	// 创建reduce任务，加入到任务队列中
	for idx, intermediatePath := range c.IntermediatePath {
		reduceTask := Task{
			TaskId:            idx,
			CoordinatorPhase:  Reduce,
			ReduceFilePathArr: intermediatePath, // 告知reducer 中间文件的存放的路径
		}
		c.TaskQueue <- &reduceTask
		c.TaskMeta[idx] = &TaskMeta{TaskReference: &reduceTask, TaskStatus: Idle, TaskStartTime: time.Now()}
	}
}
