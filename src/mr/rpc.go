package mr

//
// RPC definitions.
//
// remember to capitalize all names.
// Go RPC sends only struct fields whose names start with capital letters. Sub-structures must also have capitalized field names.
// Go 的RPC只会发送结构体中的字母大写开头的字段，子结构体也是，其字段需要是大写开头的
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
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	os.Getwd()
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
