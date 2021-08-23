package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

const (
	MaxTaskRunTime   = time.Second * 10
	ScheduleInterval = time.Millisecond * 500
)

type TaskState int
const (
	// Initial state, should be enqueued
	TaskStateReady   TaskState = 0

	// Task in queue, will be assigned to next worker request
	TaskStateQueue   TaskState = 1

	// Task is being done by one worker
	TaskStateRunning TaskState = 2

	// Task has been finished successfully
	TaskStateFinish  TaskState = 3

	// Task has been reported execution error
	TaskStateErr     TaskState = 4
)

type TaskStatus struct {
	task_state TaskState
	start_time time.Time
}

type Master struct {
	mtx        sync.Mutex
	files			 []string  // filenames to execute at Map phase
	taskPhase  TaskPhase
	taskStatus []TaskStatus
	nReduce    int
	tasksDone  bool
	taskChan 	 chan Task
}

func (m *Master) getTask(taskSeq int) Task {
	task := Task{
		Filename: "",
		Phase:    m.taskPhase,
		Seq:      taskSeq,
		NMap:     len(m.files),
		NReduce:  m.nReduce,
	}
	if task.Phase == MapPhase {
		task.Filename = m.files[taskSeq]
	}
	return task
}

func (m *Master) checkTasksStatus() {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if m.tasksDone {
		return
	}

	// Update task status.
	all_task_finished := true
	for idx, task_status := range m.taskStatus {
		switch task_status.task_state {
		case TaskStateReady:
			all_task_finished = false
			m.taskChan <- m.getTask(idx)
			m.taskStatus[idx].task_state = TaskStateQueue
		case TaskStateQueue:
			all_task_finished = false
		case TaskStateRunning:
			all_task_finished = false
			// Set timeout on worker, re-issue task to another worker.
			if time.Now().Sub(task_status.start_time) > MaxTaskRunTime {
				m.taskStatus[idx].task_state = TaskStateQueue
				m.taskChan <- m.getTask(idx)
			}
		case TaskStateFinish:
		case TaskStateErr:
			all_task_finished = false
			m.taskStatus[idx].task_state = TaskStateQueue
			m.taskChan <- m.getTask(idx)
		default:
			panic("Internal error: task status has invalid status")
		}
	}

	if all_task_finished {
		if m.taskPhase == MapPhase {
			m.taskPhase = ReducePhase
			m.taskStatus = make([]TaskStatus, m.nReduce)
		} else {
			m.tasksDone = true
		}
	}
}

func (m *Master) registerTask(request *RequestTaskRequest, task *Task) {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	// Check Master internal status.
	if task.Phase != m.taskPhase {
		panic("Internal error: current task phase doesn't match master's phase")
	}

	// Update master's status.
	m.taskStatus[task.Seq].task_state = TaskStateRunning
	m.taskStatus[task.Seq].start_time = time.Now()
}

// Request task via RPC
func (m *Master) RequestTask(request *RequestTaskRequest, reply *RequestTaskReply) error {
	// Blocks to get a task from task channel.
	task := <-m.taskChan
	reply.Task = &task
	
	// Register the task within master.
	m.registerTask(request, &task)

	return nil
}

// Report task via RPC
func (m *Master) ReportTask(request *ReportTaskRequest, reply *ReportTaskReply) error {
	m.mtx.Lock()
	defer m.mtx.Unlock()

	if request.Completed {
		m.taskStatus[request.TaskSeq].task_state = TaskStateFinish
	} else {
		m.taskStatus[request.TaskSeq].task_state = TaskStateErr
	}
	go m.checkTasksStatus()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mtx.Lock()
	defer m.mtx.Unlock()
	return m.tasksDone
}

func (m *Master) tickSchedule() {
	for {
		// Check whether all tasks finished and exit program.
		m.mtx.Lock()
		allTasksDone := m.tasksDone
		m.mtx.Unlock()
		if allTasksDone {
			break
		}

		go m.checkTasksStatus()
		time.Sleep(ScheduleInterval)
	}
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mtx = sync.Mutex{}
	m.files = files
	m.nReduce = nReduce
	m.taskPhase = MapPhase
	m.taskStatus = make([]TaskStatus, len(m.files))
	m.tasksDone = false
	if (nReduce > len(files)) {
		m.taskChan = make(chan Task, nReduce)
	} else {
		m.taskChan = make(chan Task, len(files))
	}

	go m.tickSchedule()
	m.server()
	return &m
}
