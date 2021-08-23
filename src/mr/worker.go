package mr

import "encoding/json"
import "fmt"
import "hash/fnv"
import "io/ioutil"
import "log"
import "net/rpc"
import "os"
import "strings"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

type worker struct {
	mapf      func(string, string) []KeyValue
	reducef   func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// Request task via RPC
func (w *worker) requestTask() Task {
	request := RequestTaskRequest{}
	reply := RequestTaskReply{}
	if ok := call("Master.RequestTask", &request, &reply); !ok {
		log.Fatal("Request task failed")
	}
	return *reply.Task
}

// Report task via RPC
func (w *worker) reportTask(task Task, completed bool) {
	request := ReportTaskRequest{}
	request.TaskSeq = task.Seq
	request.Phase = task.Phase
	request.Completed = completed
	reply := ReportTaskReply{}
	if ok := call("Master.ReportTask", &request, &reply); !ok {
		log.Fatal("Report task failed")
	}
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	w := worker{}
	w.mapf = mapf
	w.reducef = reducef
	w.run()
}

func (w *worker) doMapTask(task Task) {
	content, err := ioutil.ReadFile(task.Filename)
	if err != nil {
		w.reportTask(task, false /* completed */)
		return
	}

	// Map file content into key-value pairs and shard.
	kvs := w.mapf(task.Filename, string(content))
	reduces := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}

	for idx, kvs_to_reduce := range reduces {
		// Create file for mapped key-value pairs.
		filename := fmt.Sprintf("mr-%d-%d", task.Seq /* map-index */, idx /* reduce-index */)
		f, err := os.Create(filename)
		defer f.Close()
		if err != nil {
			w.reportTask(task, false /* completed */)
			return
		}

		// Dump key-values pairs into file.
		enc := json.NewEncoder(f)
		for _, kv := range kvs_to_reduce {
			if err := enc.Encode(&kv); err != nil {
				w.reportTask(task, false /* completed */)
				return
			}
		}
	}

	// All key-value pairs successfully dumped into files.
	w.reportTask(task, true /* completed */)
}

func (w *worker) doReduceTask(task Task) {
	maps := make(map[string][]string)  // <KeyType, ValueType array>
	for idx := 0; idx < task.NMap; idx++ {
		// Open corresponding reduce file.
		filename := fmt.Sprintf("mr-%d-%d", idx /* map-index */, task.Seq /* reduce-index */)
		f, err := os.Open(filename)
		defer f.Close()
		if err != nil {
			w.reportTask(task, false /* completed */)
			return
		}

		// Read key-values pairs stored within file, and reduce to ValueArray
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break  // All key-value pairs have been read through
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0 /* len */, 50 /* cap */)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}

	// All key-value pairs have been stored within maps, apply reduce function.
	res := make([]string, 0 /* len */, 50 /* cap */)
	for k, v := range maps {
		cur_reduced_res := fmt.Sprintf("%v %v\n", k, w.reducef(k, v))
		res = append(res, cur_reduced_res)
	}

	// Concatenate reduced key-value pairs, and dump to out file.
	filename := fmt.Sprintf("mr-out-%d", task.Seq /* reduce-index */)
	content := []byte(strings.Join(res, ""))
	if err := ioutil.WriteFile(filename, content, 0600); err != nil {
		w.reportTask(task, false /* completed */)
	}
	w.reportTask(task, true /* completed */)
}

func (w *worker) doTask(task Task) {
	switch task.Phase {
	case MapPhase:
			w.doMapTask(task)
	case ReducePhase:
			w.doReduceTask(task)
	default:
			panic(fmt.Sprintf("Task in invalid phase: %v", task.Phase))
	}
}

func (w *worker) run() {
	for {
		cur_task := w.requestTask()
		w.doTask(cur_task)
	}
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
