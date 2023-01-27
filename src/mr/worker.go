package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"time"
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

// ByKey for sorting by key.
type ByKey []KeyValue

// Len for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	for {
		task := getTask()
		switch task.CoordinatorPhase {
		case Map:
			doMap(mapf, &task)
		case Wait:
			time.Sleep(5 * time.Second)
		case Reduce:
			doReduce(reducef, &task)
		case Exit:
			fmt.Println("所有任务执行完毕，退出worker")
			return
		}
	}

}

func doMap(mapf func(string, string) []KeyValue, task *Task) {
	// 打开文件，并且获取文件内容
	content := getFileContent(task)
	// 进行map处理，获取kv键值队数组
	kva := mapf(task.MapFilePath, string(content))

	buffer := make([][]KeyValue, task.NReduce)
	// 将kv键值队数组的内容写入到缓存中
	for _, kv := range kva {
		slot := ihash(kv.Key) % task.NReduce
		buffer[slot] = append(buffer[slot], kv)
	}
	reduceFilePathArr := make([]string, 0)
	for idx, kva := range buffer {
		reduceFilePathArr = append(reduceFilePathArr, writeToLocalDisk(&kva, task.TaskId, idx))
	}
	task.ReduceFilePathArr = reduceFilePathArr
	completeTask(task)
}

func completeTask(task *Task) {
	ok := call("Coordinator.CompleteTask", task, &PlaceHolder{})
	if ok == false {
		fmt.Println("Rpc call error")
	}
}

func doReduce(reducef func(string, []string) string, task *Task) {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("failed to create tempfile", err)
	}

	kva := *readFromLocalDisk(task)
	sort.Sort(ByKey(kva))

	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
		i = j
	}
	tempFile.Close()
	oname := fmt.Sprintf("mr-out-%d", task.TaskId)
	os.Rename(tempFile.Name(), oname)
	task.OutputFileName = oname
	completeTask(task)
}

func readFromLocalDisk(task *Task) *[]KeyValue {
	kva := []KeyValue{}
	for _, localFilePath := range task.ReduceFilePathArr {
		file, err := os.Open(localFilePath)
		if err != nil {
			log.Fatalf("cannot open %v", localFilePath)
		}
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	return &kva
}

// 将数据缓存到buffer中，并且写入到磁盘上，并且返回文件路径给到Master
func writeToLocalDisk(kva *[]KeyValue, x, y int) string {
	dir, _ := os.Getwd()
	tempFile, err := ioutil.TempFile(dir, "mr-tmp-*")
	if err != nil {
		log.Fatal("Failed to create temp file", err)
	}
	enc := json.NewEncoder(tempFile)
	for _, kv := range *kva {
		if err := enc.Encode(&kv); err != nil {
			log.Fatal("Failed to write kv pair", err)
		}
	}
	tempFile.Close()
	outputName := fmt.Sprintf("mr-%d-%d", x, y)
	os.Rename(tempFile.Name(), outputName)
	return filepath.Join(dir, outputName)
}

// 打开文件，并且获取文件内容
func getFileContent(task *Task) []byte {
	filename := task.MapFilePath
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	return content
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

	//fmt.Println(err)
	return false
}
