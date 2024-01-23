package mr

import (
	"errors"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)


type TaskPhase int   // Map or Reduce
type TaskStatus int  // NotStarted, Ready, InProgress, Completed, Failed

const (
	TaskPhase_Map TaskPhase = 0  // Map
	TaskPhase_Reduce TaskPhase = 1  // Reduce
)

const (
	TaskStatus_NotStarted TaskStatus = 0  // NotStarted
	TaskStatus_Ready TaskStatus = 1  // Ready
	TaskStatus_InProgress TaskStatus = 2  // InProgress
	TaskStatus_Completed TaskStatus = 3  // Completed
	TaskStatus_Failed TaskStatus = 4  // Failed
)

// 扫描任务执行时间
const (
	ScheduleInterval = time.Millisecond * 500  // 扫描任务执行时间
	MaxTaskDuration = time.Second * 10         // 任务最大执行时间，用于判断任务是否失败
)

type Task struct {
	// Your definitions here.
	FileName string  // 当前任务处理的文件名
	Phase TaskPhase  // 当前任务的阶段
	Seq int          // 当前任务的序号
	NMap int         // Map任务的数量
	NReduce int      // Reduce任务的数量
	Alive bool       // 当前任务是否存活
}
// 任务状态
type TaskState struct {
	Status TaskStatus  // 当前任务的状态
	WorkerId int       // 当前任务的执行者
	StartTime time.Time  // 当前任务的开始时间,用于在扫描任务状态时判断是否超时
}

type Coordinator struct {
	// Your definitions here.
	files []string  	// 要处理的文件列表
	nReduce int     // Reduce任务的数量
	taskPhase  TaskPhase   //任务阶段
	taskstatus []TaskState  // 任务状态
	taskChan chan Task  // 任务通道
	workerSeq int  // worker的序号
	done bool  // 是否完成
	muLock sync.Mutex  // 互斥锁
}

// 创建一个任务，根据当前任务阶段和序号
func (c *Coordinator) NewOneTask(seq int) Task {
	task := Task{
		FileName: "", 
		Phase: c.taskPhase,
		Seq: seq,            
		NMap: len(c.files),         
		NReduce: c.nReduce,      
		Alive: true,       
	}
	
	if task.Phase == TaskPhase_Map {
		task.FileName = c.files[seq]
	}
	return task  
}

//扫描任务状态并适当更新
func (c *Coordinator) scanTaskState() {
	c.muLock.Lock()
	defer c.muLock.Unlock()

	if c.done {
		return
	}
	allDone := true
	// 循环每个任务的状态
	for k, v := range c.taskstatus {
		switch v.Status {
		case TaskStatus_NotStarted:
			allDone = false
			c.taskstatus[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		case TaskStatus_Ready:
			allDone = false
		case TaskStatus_InProgress: 
			allDone = false
			// 任务超时，重新分配
			if time.Now().Sub(v.StartTime) > MaxTaskDuration {
				c.taskstatus[k].Status = TaskStatus_Ready
				c.taskChan <- c.NewOneTask(k)
			}
		case TaskStatus_Completed:
		case TaskStatus_Failed: // 任务失败，重新分配
			allDone = false
			c.taskstatus[k].Status = TaskStatus_Ready
			c.taskChan <- c.NewOneTask(k)
		default:
			panic("t. status err in schedule")
		}
	}

	// 如果所有任务都完成了，且当前阶段是Map，则开始Reduce阶段
	if allDone {
		if c.taskPhase == TaskPhase_Map {
			// MAP
			c.taskPhase = TaskPhase_Reduce
			c.taskstatus = make([]TaskState, c.nReduce)
		} else {
			// REDUCE
			c.done = true
		}
	}
}
// 定时更新
func (c *Coordinator) schedule() {
	for !c.done{
		c.scanTaskState()
		time.Sleep(ScheduleInterval)
	}
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}
// 获取任务
func (c *Coordinator) GetOneTask(args *TaskArgs, reply *TaskReply) error {
	task := <- c.taskChan
	reply.Task = &task

	if task.Alive {
		// 更新任务状态
		c.muLock.Lock()
		defer c.muLock.Unlock()
		if task.Phase != c.taskPhase {
			return errors.New("task phase not match")
		}
		c.taskstatus[task.Seq].WorkerId = args.WorkerId
		c.taskstatus[task.Seq].Status = TaskStatus_InProgress
		c.taskstatus[task.Seq].StartTime = time.Now()
	}
	return nil
}
// 注册worker
func (c *Coordinator) RegWorker(args *RegArgs, reply *RegReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	c.workerSeq++   // worker序号加1
	reply.WorkerId = c.workerSeq
	return nil
}
// worker上报任务状态
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.muLock.Lock()
	defer c.muLock.Unlock()
	//如果发现阶段不同或者当前任务已经分配给了其它worker就不修改当前任务状态
	if c.taskPhase != args.Phase || c.taskstatus[args.Seq].WorkerId != args.WorkerId {
		return nil
	}
	if args.Done {
		c.taskstatus[args.Seq].Status = TaskStatus_Completed
	} else {
		c.taskstatus[args.Seq].Status = TaskStatus_Failed
	}
	go c.scanTaskState()
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	//ret := false
	// Your code here.
	c.muLock.Lock()
	defer c.muLock.Unlock()
	return c.done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files: files,
		nReduce: nReduce,
		taskPhase: TaskPhase_Map,
		taskstatus: make([]TaskState, len(files)),
		workerSeq: 0,
		done: false,
	}

	// Your code here.
	if len(files)>nReduce {
		c.taskChan = make(chan Task, len(files))
	} else {
		c.taskChan = make(chan Task, nReduce)
	}
	go c.schedule()
	c.server()
	
	return &c
}
