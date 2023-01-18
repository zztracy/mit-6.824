package mr

import (
	"fmt"
)
import "log"
import "net/rpc"
import "hash/fnv"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// ihash 来确定要调用哪个reduceTask
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := getTask()
	switch reply.State
	if Map {
		
	}

	intermediate := []KeyValue{}
	kva := mapf(reply.Filename, reply.Contents)
	// 将map方法返回的结果加入到kva中
	intermediate = append(intermediate, kva...)

}

func getTask() Task {
	args := Args{}
	reply := Task{}
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok == false {
		fmt.Println("Rpc call error")
	}
	return reply
}

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
