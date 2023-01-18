package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

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
	Filename   string
	TaskId     int
	TaskStatus TaskStatus
	State      State
	StartTime  time.Time
	Result     string
}

type Coordinator struct {
	Files     []string
	NReduce   int
	State     State
	TaskQueue chan *Task
	TaskMap   map[int]*Task
}

// GetContents
// Your code here -- RPC handlers for the worker to call.
// 协调者的RPC handler，由worker向coordinator发送获取任务的请求。
func (c *Coordinator) AssignTask(args *Args, reply *Task) error {
	if len(c.TaskQueue) > 0 {
		reply = <-c.TaskQueue
	} else if c.State == Exit {
		reply = &Task{State: Exit}
	} else {
		reply = &Task{State: Wait}
	}
	return nil
}

func (c *Coordinator) getFileContent(filename string) string {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read content of %v", filename)
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("close file failed, filename is %v", filename)
	}

	c.changeFileStatus(filename)
	return string(content)
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	fmt.Println("sockname is: ", sockname)
	os.Remove(sockname)
	// 基于文件的方式进行rpc调用
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		Files:     files,
		NReduce:   nReduce,
		State:     Map,
		TaskQueue: make(chan *Task),
		TaskMap:   make(map[int]*Task),
	}
	c.initMapTask()
	// 启动线程，监听worker的rpc请求
	c.server()
	return &c
}

func (c *Coordinator) initMapTask() {
	for idx, filename := range c.Files {
		task := Task{
			Filename:   filename,
			TaskId:     idx,
			TaskStatus: Idle,
			State:      Map,
			StartTime:  time.Now(),
		}
		c.TaskQueue <- &task
		c.TaskMap[idx] = &task
	}
}
