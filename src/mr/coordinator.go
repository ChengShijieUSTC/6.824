package mr

import (
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	M              int
	R              int
	MapFinish      int
	ReduceFinish   int
	MapList        []int //0代表未分配，1代表已分配未完成，2代表已完成
	ReduceList     []int
	BackMapList    []int //-1
	BackReduceList []int
	FileName       []string
	Lock           sync.Mutex
	Flag           int
	Flag2          int
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) Allocate(args *RPCArgs, reply *RPCReply) error {
	c.Lock.Lock()
	if c.MapFinish < c.M {
		mapTaskIndex := -1
		backMapIndex := -1
		for i := 0; i < c.M; i++ { //找未分配的map任务
			if c.MapList[i] == 0 {
				mapTaskIndex = i
			}
		}
		for i := 0; i < c.M; i++ { //找未分配的map任务
			if c.BackMapList[i] == 0 {
				backMapIndex = i
			}
		}
		if mapTaskIndex == -1 && backMapIndex == -1 { //没有0状态的任务，有1状态的任务，通知worker等着
			reply.Type = 2
			c.Lock.Unlock()
		} else if mapTaskIndex != -1 {
			reply.Type = 0                            //map task
			reply.Filename = c.FileName[mapTaskIndex] //filename
			reply.MapTaskNumber = mapTaskIndex
			reply.M = c.M
			reply.R = c.R
			c.MapList[mapTaskIndex] = 1
			c.Lock.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.Lock.Lock()
				if c.MapList[mapTaskIndex] == 1 {
					if c.Flag == 0 && c.MapFinish >= c.M*3/4 {
						//初始化BackMapList数组
						for i := 0; i < c.M; i++ {
							if c.MapList[i] != 2 {
								c.BackMapList[i] = 0
							}
						}
						c.Flag = 1
					}
					c.MapList[mapTaskIndex] = 0
				}
				c.Lock.Unlock()
			}()
		} else { //map backup task
			reply.Type = 10
			reply.Filename = c.FileName[backMapIndex] //filename
			reply.MapBackNumber = backMapIndex
			reply.M = c.M
			reply.R = c.R
			c.BackMapList[backMapIndex] = 1
			c.Lock.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.Lock.Lock()
				if c.BackMapList[backMapIndex] == 1 {
					c.BackMapList[backMapIndex] = 0
				}
				c.Lock.Unlock()
			}()
		}

	} else if c.ReduceFinish < c.R { //reduce
		reduceTaskIndex := -1
		backReduceIndex := -1
		for i := 0; i < c.R; i++ {
			if c.ReduceList[i] == 0 {
				reduceTaskIndex = i
			}
		}
		for i := 0; i < c.R; i++ {
			if c.BackReduceList[i] == 0 {
				backReduceIndex = i
			}
		}
		if reduceTaskIndex == -1 && backReduceIndex == -1 {
			reply.Type = 2
			c.Lock.Unlock()
		} else if reduceTaskIndex != -1 {
			reply.Type = 1 //reduce task
			reply.ReduceTaskNumber = reduceTaskIndex
			reply.M = c.M
			reply.R = c.R
			c.ReduceList[reduceTaskIndex] = 1
			c.Lock.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.Lock.Lock()
				if c.ReduceList[reduceTaskIndex] == 1 {
					if c.Flag2 == 0 && c.ReduceFinish >= c.R*4/5 {
						//初始化BackReduceList数组
						for i := 0; i < c.R; i++ {
							if c.ReduceList[i] != 2 {
								c.BackReduceList[i] = 0
							}
						}
						c.Flag2 = 1
					}
					c.ReduceList[reduceTaskIndex] = 0
				}
				c.Lock.Unlock()
			}()
		} else {
			reply.Type = 11
			reply.ReduceBackNumber = backReduceIndex
			reply.M = c.M
			reply.R = c.R
			c.BackReduceList[backReduceIndex] = 1
			c.Lock.Unlock()
			go func() {
				time.Sleep(time.Duration(10) * time.Second)
				c.Lock.Lock()
				if c.BackReduceList[backReduceIndex] == 1 {
					c.BackReduceList[backReduceIndex] = 0
				}
				c.Lock.Unlock()
			}()
		}

	} else {
		reply.Type = 3
		c.Lock.Unlock()
	}

	return nil
}

func (c *Coordinator) Finish(args *RPCArgs, reply *RPCReply) error {
	c.Lock.Lock()
	if args.FinishM == 1 {
		if c.MapList[args.MapTaskNumber] != 2 {
			c.MapFinish++
		}
		//fmt.Println(c.MapList)
		//fmt.Println(c.BackMapList)
		//fmt.Println(c.MapFinish)
		c.MapList[args.MapTaskNumber] = 2 //标记为已完成
		c.BackMapList[args.MapTaskNumber] = 2
		//fmt.Println(c.MapList)
		//fmt.Println(c.BackMapList)
		//fmt.Println(c.MapFinish)
		//fmt.Println("  ")
		c.Lock.Unlock()
	} else if args.FinishR == 1 {
		if c.ReduceList[args.ReduceTaskNumber] != 2 {
			c.ReduceFinish++
		}
		c.ReduceList[args.ReduceTaskNumber] = 2
		c.BackReduceList[args.ReduceTaskNumber] = 2
		c.Lock.Unlock()
	} else if args.FinishM == 11 {
		if c.BackMapList[args.MapBackNumber] != 2 {
			c.MapFinish++
		}
		c.BackMapList[args.MapBackNumber] = 2 //标记为已完成
		c.MapList[args.MapBackNumber] = 2
		c.Lock.Unlock()
	} else if args.FinishR == 11 {
		if c.BackReduceList[args.ReduceBackNumber] != 2 {
			c.ReduceFinish++
		}
		c.BackReduceList[args.ReduceBackNumber] = 2
		c.ReduceList[args.ReduceBackNumber] = 2
		c.Lock.Unlock()
	}
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
	ret := false
	c.Lock.Lock()
	// Your code here.
	if c.ReduceFinish == c.R {
		ret = true
		//删除中间文件
		for i := 0; i < c.M; i++ {
			for j := 0; j < c.R; j++ {
				tname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(j)
				os.Remove(tname)
			}
		}
	}
	c.Lock.Unlock()
	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.M = len(files)
	c.R = nReduce
	c.FileName = files
	c.MapFinish = 0
	c.ReduceFinish = 0
	c.MapList = make([]int, c.M)
	c.ReduceList = make([]int, c.R)
	c.BackMapList = make([]int, c.M)
	c.BackReduceList = make([]int, c.R)
	for i := 0; i < c.M; i++ {
		c.BackMapList[i] = -1
	}
	for i := 0; i < c.R; i++ {
		c.BackReduceList[i] = -1
	}
	c.Flag = 0
	c.Flag2 = 0
	c.server()
	return &c
}
