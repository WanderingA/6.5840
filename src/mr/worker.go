package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"os"
	"strings"
	"log"
	"net/rpc"
	"hash/fnv"
)



//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type worker struct {
	workerId int 
	mapf func(string, string) []KeyValue
	reducef func(string, []string) string
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


//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	worker := worker{
		mapf: mapf,
		reducef: reducef,
	}
	worker.register()
	worker.run()
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

func (w *worker) run() {
	for {
		task, err := w.getTask()
		if err != nil {
			DPrintf(err.Error())
			continue
		}
		if !task.Alive {
			DPrintf("no task")
			return 
		}
		w.doTask(*task)
	}
}

// 开始执行任务
func (w *worker) doTask(task Task) {
	switch task.Phase {
		case TaskPhase_Map:
			w.doMap(task)
		case TaskPhase_Reduce:
			w.doReduce(task)
		default:
			DPrintf("unknown task phase:%+v", task.Phase)
	}
}
// Map任务时获取要输出的文件名
func (w *worker) getReduceName(mapId, intermediateId int) string {
	return fmt.Sprintf("mr-kv-%d-%d", mapId, intermediateId)
}
// reduce任务时获取要输出的文件名
func (w *worker) getOutputName(intermediateId int) string {
	return fmt.Sprintf("mr-out-%d", intermediateId)
}

// Map任务
func (w *worker) doMap(task Task) {
	DPrintf("%v start map task:%+v", w.workerId, task.FileName)
	// 读取文件
	cont, err := ioutil.ReadFile(task.FileName)
	if err != nil {
		DPrintf("read file err:%v", err)
		w.reportTask(&task, false)
	}
	// 执行map函数，获取中间结果
	kvs := w.mapf(task.FileName, string(cont))
	intermediate := make([][]KeyValue, task.NReduce)
	for _, kv := range kvs {
		pid := ihash(kv.Key) % task.NReduce
		intermediate[pid] = append(intermediate[pid], kv)
	}
	// 将中间结果写入文件
	for k, v := range intermediate {
		fileName := w.getReduceName(task.Seq, k)
		file, err := os.Create(fileName)
		if err != nil {
			DPrintf("create file-%v failed. %v", fileName, err)
			w.reportTask(&task, false)
			return 
		}
		encoder := json.NewEncoder(file)
		for _, kv := range v {
			if err := encoder.Encode(&kv); err != nil {
				DPrintf("encode kvs to file-%v failed. %v", fileName, err)
				w.reportTask(&task, false)  // 上报任务失败
			}
		}
		if err := file.Close(); err != nil {
			DPrintf("close file-%v failed. %v", fileName, err)
			w.reportTask(&task, false)    // 上报任务失败
		}
	}
	w.reportTask(&task, true)  		// 上报任务成功
}

// Reduce任务
func(w *worker) doReduce(task Task) {
	maps := make(map[string][]string)
	// 读取中间结果
	for i := 0; i < task.NMap; i++ {
		fileName := w.getReduceName(i, task.Seq)
		file, err := os.Open(fileName)
		if err != nil {
			DPrintf("open file-%v failed. %v", fileName, err)
			w.reportTask(&task, false)
			return
		}
		// 解析中间结果
		decoder := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := decoder.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	// 执行reduce函数
	res := make([]string, 0)
	for k, v := range maps {
		len := w.reducef(k, v)
		res = append(res, fmt.Sprintf("%s %s\n", k, len))
	}
	fileName := w.getOutputName(task.Seq)
	if err := ioutil.WriteFile(fileName, []byte(strings.Join(res, "")), 0600); err != nil {
		DPrintf("write file-%v in doReduceTask. %v", fileName, err)
		w.reportTask(&task, false)  // 上报任务失败
	}
	w.reportTask(&task, true) 		// 上报任务成功
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}


// 注册worker
func (w *worker) register() {
	args := &RegArgs{}
	reply := &RegReply{}

	if err := call("Coordinator.RegWorker", args, reply); !err {
		log.Fatal("register worker failed")
	}
	w.workerId = reply.WorkerId  // coordinator分配的workerId
}
// 获取任务
func (w *worker) getTask() (*Task, error) {
	args := &TaskArgs{
		WorkerId: w.workerId,
	}
	reply := &TaskReply{}

	if err := call("Coordinator.GetOneTask", args, reply); !err {
		return nil, errors.New("worker get task failed")
	}
	DPrintf("worker get task:%+v", reply.Task)
	return reply.Task, nil
}
// 上报任务状态
func (w *worker) reportTask(task *Task, done bool) {
	args := &ReportTaskArgs{
		WorkerId: w.workerId,
		Phase: task.Phase,
		Seq: task.Seq,
		Done: done,
	}
	reply := &ReportTaskReply{}

	if err := call("Coordinator.ReportTask", args, reply); !err {
		DPrintf("report task fail:%+v", args)
	}
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
