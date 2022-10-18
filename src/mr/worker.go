package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		time.Sleep(time.Second)
		args := RPCArgs{}
		reply := RPCReply{}
		ok := call("Coordinator.Allocate", &args, &reply)
		if ok {
			switch reply.Type {
			case 0:
				file, errOpen := os.Open(reply.Filename)
				if errOpen != nil {
					log.Fatalf("cannot open %v", reply.Filename)
				}
				content, errRead := ioutil.ReadAll(file)
				if errRead != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				errClose := file.Close()
				if errClose != nil {
					log.Fatalf("cannot close %v", reply.Filename)
				}
				kva := mapf(reply.Filename, string(content))
				//二维
				intermediate := make([][]KeyValue, reply.R)
				for i := range intermediate {
					intermediate[i] = make([]KeyValue, 0)
				}
				for i := 0; i < len(kva); i++ {
					chooseReduce := ihash(kva[i].Key) % reply.R
					intermediate[chooseReduce] = append(intermediate[chooseReduce], kva[i])
				}
				//排序
				for i := 0; i < reply.R; i++ {
					sort.Sort(ByKey(intermediate[i]))
				}
				for i := 0; i < reply.R; i++ {
					oname := "mr-" + strconv.Itoa(reply.MapTaskNumber) + "-" + strconv.Itoa(i)
					ofile, _ := ioutil.TempFile("", oname+"*")
					enc := json.NewEncoder(ofile)
					for _, kv := range intermediate[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatal("intermediate err")
						}
					}
					_, err := os.Stat(oname)
					if err == nil { //文件存在
						break
					}
					errRename := os.Rename(ofile.Name(), oname)
					if errRename != nil {
						log.Fatal("rename err")
					}
					errTmpClose := ofile.Close()
					if errTmpClose != nil {
						log.Fatal("intermediate file close err")
					}
				}

				endArgs := RPCArgs{}
				endArgs.FinishM = 1
				endArgs.FinishR = 0
				endArgs.MapTaskNumber = reply.MapTaskNumber
				endReply := RPCReply{}
				call("Coordinator.Finish", &endArgs, &endReply)

				break
			case 1:
				var kva []KeyValue
				for i := 0; i < reply.M; i++ {
					tname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceTaskNumber)
					file, errOpen := os.Open(tname)
					if errOpen != nil {
						log.Fatalf("cannot open %v", tname)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				//排序
				sort.Sort(ByKey(kva))

				oname := "mr-out-" + strconv.Itoa(reply.ReduceTaskNumber)
				ofile, _ := ioutil.TempFile("", oname+"*")
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
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				errRename := os.Rename(ofile.Name(), oname)
				if errRename != nil {
					log.Fatal("rename err")
				}
				errOutClose := ofile.Close()
				if errOutClose != nil {
					log.Fatal("Output file close err")
				}

				endArgs := RPCArgs{}
				endArgs.FinishM = 0
				endArgs.FinishR = 1
				endArgs.ReduceTaskNumber = reply.ReduceTaskNumber
				endReply := RPCReply{}
				call("Coordinator.Finish", &endArgs, &endReply)

				break
			case 2:
				time.Sleep(time.Second)
				break
			case 10:
				file, errOpen := os.Open(reply.Filename)
				if errOpen != nil {
					log.Fatalf("cannot open %v", reply.Filename)
				}
				content, errRead := ioutil.ReadAll(file)
				if errRead != nil {
					log.Fatalf("cannot read %v", reply.Filename)
				}
				errClose := file.Close()
				if errClose != nil {
					log.Fatalf("cannot close %v", reply.Filename)
				}
				kva := mapf(reply.Filename, string(content))
				//二维
				intermediate := make([][]KeyValue, reply.R)
				for i := range intermediate {
					intermediate[i] = make([]KeyValue, 0)
				}
				for i := 0; i < len(kva); i++ {
					chooseReduce := ihash(kva[i].Key) % reply.R
					intermediate[chooseReduce] = append(intermediate[chooseReduce], kva[i])
				}
				//排序
				for i := 0; i < reply.R; i++ {
					sort.Sort(ByKey(intermediate[i]))
				}
				for i := 0; i < reply.R; i++ {
					oname := "mr-" + strconv.Itoa(reply.MapBackNumber) + "-" + strconv.Itoa(i)
					ofile, _ := ioutil.TempFile("", oname+"*")
					enc := json.NewEncoder(ofile)
					for _, kv := range intermediate[i] {
						err := enc.Encode(&kv)
						if err != nil {
							log.Fatal("intermediate err")
						}
					}
					_, err := os.Stat(oname)
					if err == nil { //文件存在
						break
					}
					errRename := os.Rename(ofile.Name(), oname)
					if errRename != nil {
						log.Fatal("rename err")
					}
					errTmpClose := ofile.Close()
					if errTmpClose != nil {
						log.Fatal("intermediate file close err")
					}
				}

				endArgs := RPCArgs{}
				endArgs.FinishM = 11
				endArgs.FinishR = 0
				endArgs.MapBackNumber = reply.MapBackNumber
				endReply := RPCReply{}
				call("Coordinator.Finish", &endArgs, &endReply)

				break
			case 11:
				var kva []KeyValue
				for i := 0; i < reply.M; i++ {
					tname := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.ReduceBackNumber)
					file, errOpen := os.Open(tname)
					if errOpen != nil {
						log.Fatalf("cannot open %v", tname)
					}
					dec := json.NewDecoder(file)
					for {
						var kv KeyValue
						if err := dec.Decode(&kv); err != nil {
							break
						}
						kva = append(kva, kv)
					}
				}

				//排序
				sort.Sort(ByKey(kva))

				oname := "mr-out-" + strconv.Itoa(reply.ReduceBackNumber)
				ofile, _ := ioutil.TempFile("", oname+"*")
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
					fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)
					i = j
				}
				_, err := os.Stat(oname)
				if err != nil { //文件不存在
					errRename := os.Rename(ofile.Name(), oname)
					if errRename != nil {
						log.Fatal("rename err")
					}
					errOutClose := ofile.Close()
					if errOutClose != nil {
						log.Fatal("Output file close err")
					}
				}

				endArgs := RPCArgs{}
				endArgs.FinishM = 0
				endArgs.FinishR = 11
				endArgs.ReduceBackNumber = reply.ReduceBackNumber
				endReply := RPCReply{}
				call("Coordinator.Finish", &endArgs, &endReply)

				break
			default:
				break
			}
		} else {
			fmt.Printf("call failed!\n")
		}
	}
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
