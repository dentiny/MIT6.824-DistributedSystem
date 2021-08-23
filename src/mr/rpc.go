package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

// Worker request tasks to finish
type RequestTaskRequest struct {
	WorkerId int
}

type RequestTaskReply struct {
	Task *Task
}

// Report successful/failed tasks to master
type ReportTaskRequest struct {
	TaskSeq 	int
	WorkerId 	int
	Phase 		TaskPhase
	Completed bool
}

type ReportTaskReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
