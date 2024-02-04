package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type KeyValues struct {
	Key    string
	Values []string
}

// 执行map函数，返回所有输出的临时文件名
func exec_map(task Reply, mapf func(string, string) []KeyValue) []string {
	intermediate := []KeyValue{}

	// 遍历文件列表，将文件分割为单词存入intermediate，其中key为单词，value为1
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v", filename)
		}
		file.Close()

		kva := mapf(filename, string(content))
		intermediate = append(intermediate, kva...)
	}

	// 将所有key-value根据key哈希值分配到指定bucket
	tmp := [][]KeyValue{}
	i := 0
	for i < task.Nreduce {
		tmp = append(tmp, []KeyValue{})
		i++
	}

	for _, kv := range intermediate {
		index := ihash(kv.Key) % task.Nreduce
		tmp[index] = append(tmp[index], kv)
	}

	ofiles := []string{}
	i = 0
	for i := 0; i < task.Nreduce; i++ {
		if len(tmp[i]) == 0 {
			continue
		}

		// 临时文件名需要加上index区分不同的map任务，若不添加，当同一个进程多次执行任务，会覆盖先前生成临时文件导致单词丢失
		oname := fmt.Sprintf("/var/tmp/mr-tmp-%d-%d-%d", os.Getpid(), task.Index, i)
		ofile, _ := os.Create(oname)

		// 以json格式存储kv
		enc := json.NewEncoder(ofile)
		for _, kv := range tmp[i] {
			enc.Encode(&kv)
		}

		ofiles = append(ofiles, oname)
		ofile.Close()
	}

	return ofiles
}

func reduce_kv(kv []KeyValue) []KeyValues {
	keyvalues := []KeyValues{}

	sort.Sort(ByKey(kv))

	key := ""
	values := []string{}
	for _, i := range kv {
		if (key != i.Key) && (key != "") && (len(values) > 0) {
			keyvalues = append(keyvalues, KeyValues{key, values})
			values = []string{}
		}
		if key != i.Key {
			key = i.Key
		}
		values = append(values, i.Value)
	}

	if len(values) > 0 {
		keyvalues = append(keyvalues, KeyValues{key, values})
	}
	return keyvalues
}

func exec_reduce(task Reply, reducef func(string, []string) string) {
	intermediate := []KeyValue{}

	// loop buffer file and put kv into var intermediate
	for _, filename := range task.Files {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}

		// get kv and put into intermediate
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			intermediate = append(intermediate, kv)
		}
	}

	// put all equal key values into same array
	kvalues := reduce_kv(intermediate)

	oname := fmt.Sprintf("mr-out-%d", task.Index)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	for _, kv := range kvalues {
		value := reducef(kv.Key, kv.Values)
		fmt.Fprintf(ofile, "%v %v\n", kv.Key, value)
	}
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		reply, err := GetTask()
		if err != nil {
			fmt.Print("gettask error\n")
			time.Sleep(time.Second)
		} else if reply.Tasktype == 0 { // map task
			ofiles := exec_map(reply, mapf)
			ComplelteTask(ofiles, reply)
		} else if reply.Tasktype == 1 { // reduce task
			exec_reduce(reply, reducef)
			ComplelteTask([]string{}, reply)
		} else if reply.Tasktype == 2 { // wait task
			time.Sleep(time.Second)
		} else if reply.Tasktype == 3 { // exit task
			log.Default().Printf("all task done!")
			os.Exit(0)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
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

// ask task from coordinator
func GetTask() (Reply, error) {
	args := Args{}
	reply := Reply{}

	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		return reply, nil
	} else {
		return reply, errors.New("gettask rpc fault!\n")
	}
}

// notify task complete to coordinator
func ComplelteTask(files []string, task Reply) {
	args := Args{}
	reply := Reply{}

	args.TaskType = task.Tasktype
	args.Index = task.Index
	args.Files = files

	ok := call("Coordinator.CompleteTask", &args, &reply)
	if ok == false {
		fmt.Printf("complete task call fail!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
