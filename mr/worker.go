package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"strings"
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

// for sorting by key. 实现了这三个方法相当于实现了Interface接口，才能将其传入Sort()
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
	for {
		args := Args{os.Getpid()}
		reply := Reply{}

		if ok := GetTask(&args, &reply); !ok {
			continue
		}

		if reply.AllFinish {
			// fmt.Println("All tasks completed. Worker exiting.")
			break
		}

		var err error

		// if no assigned task in `reply`, it wouldn't reach here
		if reply.TaskType == 0 {
			err = ExecMapTask(&reply, mapf)
		} else {
			err = ExecReduceTask(&reply, reducef)
		}

		if err != nil {
			// fmt.Printf("Task %d failed: %v\n", reply.TaskID, err)
			reportArgs := ReportArgs{
				TaskType: reply.TaskType,
				TaskID:   reply.TaskID,
				Done:     false,
			}
			ReportTask(&reportArgs)
		}

		// 任务完成后，向协调器发送完成信号
		reportArgs := ReportArgs{
			TaskType: reply.TaskType,
			TaskID:   reply.TaskID,
			Done:     true,
		}
		ReportTask(&reportArgs)
	}
}

func GetTask(args *Args, reply *Reply) bool {
	return call("Coordinator.TaskHandler", args, reply)
}

func ReportTask(args *ReportArgs) bool {
	return call("Coordinator.ReportHandler", args, &ReportReply{})
}

func ExecMapTask(reply *Reply, mapf func(string, string) []KeyValue) error {
	file, err := os.Open(reply.FileName)
	if err != nil {
		return fmt.Errorf("cannot open %v: %v", reply.FileName, err)
	}

	content, err := io.ReadAll(file)
	if err != nil {
		return fmt.Errorf("cannot read file: %v", err)
	}
	file.Close()

	kva := mapf(reply.FileName, string(content))

	// 按Reduce分区写入文件
	partitions := make(map[int][]KeyValue)
	for _, kv := range kva {
		Y := ihash(kv.Key) % reply.NReduce // 计算Reduce分区号
		partitions[Y] = append(partitions[Y], kv)
	}

	for Y, kvs := range partitions {
		// 创建临时文件（避免并发冲突）
		// os.CreateTemp 的设计是为了：
		// 避免文件名冲突（自动添加随机后缀）
		// 保证原子性（创建和写入完成后才重命名）
		// 自动选择系统临时目录（更安全）
		tempFile, err := os.CreateTemp(".", fmt.Sprintf("mr-temp-%d-*", reply.TaskID))
		if err != nil {
			tempFile.Close()
			return fmt.Errorf("cannot create temp file: %v", err)
		}
		defer os.Remove(tempFile.Name()) // 确保临时文件被清理

		// 需要确保在任何错误路径下都关闭文件
		// 将键值对编码为JSON写入文件
		enc := json.NewEncoder(tempFile)
		for _, kv := range kvs {
			if err := enc.Encode(&kv); err != nil {
				tempFile.Close()
				return fmt.Errorf("cannot encode kv: %v", err)
			}
		}
		tempFile.Close()

		// 原子重命名为最终文件 mr-X-Y
		finalName := fmt.Sprintf("mr-%d-%d", reply.TaskID, Y)
		if err := os.Rename(tempFile.Name(), finalName); err != nil {
			return fmt.Errorf("cannot rename temp file: %v", err)
		}
	}

	return nil
}

func ExecReduceTask(reply *Reply, reducef func(string, []string) string) error {
	intermediate := []KeyValue{}
	for _, filename := range reply.Files {
		// 检查文件名是否符合当前 Reduce 任务的分区（mr-X-Y，Y == r.TaskID）
		parts := strings.Split(filename, "-")

		if len(parts) != 3 || parts[0] != "mr" {
			continue
		}

		Y, err := strconv.Atoi(parts[2])
		if err != nil || Y != reply.TaskID {
			continue
		}

		// 打开并解码 JSON 格式的中间文件
		file, err := os.Open(filename)
		if err != nil {
			return fmt.Errorf("cannot open %v: %v", filename, err)
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				if err != io.EOF {
					file.Close()
					return fmt.Errorf("decode error: %v", err)
				}
				break // 文件结束或解码错误
			}
			// 按key分组
			intermediate = append(intermediate, kv)
		}
		file.Close()
	}

	// 按key排序 (保证顺序一致性)
	sort.Sort(ByKey(intermediate))

	tempOut, err := os.CreateTemp(".", fmt.Sprintf("mr-out-temp-%d-*", reply.TaskID))
	if err != nil {
		return fmt.Errorf("cannot create temp output: %v", err)
	}
	defer os.Remove(tempOut.Name())

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)
		fmt.Fprintf(tempOut, "%v %v\n", intermediate[i].Key, output)
		i = j
	}

	tempOut.Close()
	if err := os.Rename(tempOut.Name(), fmt.Sprintf("mr-out-%d", reply.TaskID)); err != nil {
		return fmt.Errorf("cannot rename temp output: %v", err)
	}

	return nil
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
