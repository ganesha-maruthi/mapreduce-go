package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	UNALLOCATED = iota
	ALLOCATED
	FINISHED
)

var mapTasks chan string
var reduceTasks chan int

type Master struct {
	InputFilenames        map[string]int
	MapTaskCount          int
	NReduce               int
	IntermediateFilenames [][]string
	FinalFilenames        []string
	MapDone               bool
	ReduceTaskStatus      map[int]int
	ReduceDone            bool
	Lock                  *sync.RWMutex
}

func (m *Master) RPCHandler(arguments *Arguments, reply *Reply) error {
	messageType := arguments.MessageType
	switch messageType {
	case TASK:
		select {
		case filename := <-mapTasks:
			m.Lock.Lock()
			reply.Filename = filename
			reply.MapNumberAllocated = m.MapTaskCount
			reply.NReduce = m.NReduce
			reply.TaskType = "map"
			m.InputFilenames[filename] = ALLOCATED
			m.MapTaskCount++
			m.Lock.Unlock()
			go m.monitorWorker("map", filename)
			return nil

		case reduceNum := <-reduceTasks:
			m.Lock.Lock()
			reply.ReduceFileList = m.IntermediateFilenames[reduceNum]
			reply.ReduceNumberAllocated = reduceNum
			reply.NReduce = m.NReduce
			reply.TaskType = "reduce"

			m.ReduceTaskStatus[reduceNum] = ALLOCATED
			m.Lock.Unlock()
			go m.monitorWorker("reduce", strconv.Itoa(reduceNum))
			return nil
		}
	case FINISH_MAP:
		m.Lock.Lock()
		defer m.Lock.Unlock()
		m.InputFilenames[arguments.MessageContent] = FINISHED
	case FINISH_REDUCE:
		index, _ := strconv.Atoi(arguments.MessageContent)
		m.Lock.Lock()
		defer m.Lock.Unlock()
		m.ReduceTaskStatus[index] = FINISHED
	}
	return nil
}

func (m *Master) monitorWorker(taskType, argument string) {
	ticker := time.NewTicker(10 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			if taskType == "map" {
				m.Lock.Lock()
				m.InputFilenames[argument] = UNALLOCATED
				m.Lock.Unlock()
				mapTasks <- argument
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(argument)
				m.Lock.Lock()
				m.ReduceTaskStatus[index] = UNALLOCATED
				m.Lock.Unlock()
				reduceTasks <- index
			}
			return
		default:
			if taskType == "map" {
				m.Lock.RLock()
				if m.InputFilenames[argument] == FINISHED {
					m.Lock.RUnlock()
					return
				} else {
					m.Lock.RUnlock()
				}
			} else if taskType == "reduce" {
				index, _ := strconv.Atoi(argument)
				m.Lock.RLock()
				if m.ReduceTaskStatus[index] == FINISHED {
					m.Lock.RUnlock()
					return
				} else {
					m.Lock.RUnlock()
				}
			}
		}
	}
}

func (m *Master) IntermediateFilesHandler(arguments *IntermediateFile, reply *Reply) error {
	nReduceNumber := arguments.NReduceNumber
	filename := arguments.MessageContent

	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.IntermediateFilenames[nReduceNumber] = append(m.IntermediateFilenames[nReduceNumber], filename)
	return nil
}

func (m *Master) FinalFilesHandler(arguments *FinalFile, reply *Reply) error {
	filename := arguments.MessageContent

	m.Lock.Lock()
	defer m.Lock.Unlock()
	m.FinalFilenames = append(m.FinalFilenames, filename)
	return nil
}

func (m *Master) startRPCServer() {
	mapTasks = make(chan string, 10)
	reduceTasks = make(chan int, 10)

	rpc.Register(m)
	rpc.HandleHTTP()
	go m.generateTask()

	socketAddress := getSocketAddress()
	os.Remove(socketAddress)
	listener, err := net.Listen("unix", socketAddress)
	if err != nil {
		log.Fatal("Error listening:", err)
	}
	go http.Serve(listener, nil)
}

func (m *Master) generateTask() {
	m.Lock.Lock()
	for key, value := range m.InputFilenames {
		if value == UNALLOCATED {
			mapTasks <- key
		}
	}
	m.Lock.Unlock()
	done := false
	for !done {
		done = checkAllMapTask(m)
	}

	m.MapDone = true

	m.Lock.Lock()
	for key, value := range m.ReduceTaskStatus {
		if value == UNALLOCATED {
			reduceTasks <- key
		}
	}
	m.Lock.Unlock()

	done = false
	for !done {
		done = checkAllReduceTask(m)
	}

	m.ReduceDone = true
	m.generateResult()
}

func (m *Master) generateResult() {
	intermediates := []KeyValue{}
	for _, filename := range m.FinalFilenames {
		file, err := os.Open("../test/" + filename)
		if err != nil {
			fmt.Println("Error file open:", err)
			log.Fatalf("Error opening %v", filename)
		}
		defer file.Close()
		decoder := json.NewDecoder(file)
		for {
			var keyValue KeyValue
			if err := decoder.Decode(&keyValue); err != nil {
				break
			}
			intermediates = append(intermediates, keyValue)
		}
	}
	sort.Sort(KeyValueArray(intermediates))
	outputFilename := "../test/mapreduce-out-final.txt"
	outputFile, _ := os.Create(outputFilename)

	i := 0
	for i < len(intermediates) {
		j := i + 1
		for j < len(intermediates) && intermediates[j].Key == intermediates[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediates[k].Value)
		}
		output := 0
		for _, v := range values {
			temp, _ := strconv.Atoi(v)
			output += temp
		}
		fmt.Fprintf(outputFile, "%v %v\n", intermediates[i].Key, output)
		i = j
	}
	fmt.Println("Output of MapReduce written to:", outputFilename)
}

func checkAllMapTask(m *Master) bool {
	m.Lock.RLock()
	defer m.Lock.RUnlock()
	for _, value := range m.InputFilenames {
		if value != FINISHED {
			return false
		}
	}
	return true
}

func checkAllReduceTask(m *Master) bool {
	m.Lock.RLock()
	defer m.Lock.RUnlock()
	for _, value := range m.ReduceTaskStatus {
		if value != FINISHED {
			return false
		}
	}
	return true
}

func (m *Master) Done() bool {
	return false || m.ReduceDone
}

func MakeMaster(filenames []string, nReduce int) *Master {
	master := Master{}
	master.InputFilenames = make(map[string]int)
	master.MapTaskCount = 0
	master.NReduce = nReduce
	master.MapDone = false
	master.ReduceDone = false
	master.ReduceTaskStatus = make(map[int]int)
	master.IntermediateFilenames = make([][]string, master.NReduce)
	master.FinalFilenames = make([]string, master.NReduce)
	master.Lock = new(sync.RWMutex)

	for _, value := range filenames {
		master.InputFilenames[value] = UNALLOCATED
	}

	for i := 0; i < nReduce; i++ {
		master.ReduceTaskStatus[i] = UNALLOCATED
	}

	master.startRPCServer()
	return &master
}
