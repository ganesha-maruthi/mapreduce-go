package mapreduce

import (
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
)

type KeyValueArray []KeyValue

func (a KeyValueArray) Len() int {
	return len(a)
}
func (a KeyValueArray) Swap(i, j int) {
	a[i], a[j] = a[j], a[i]
}
func (a KeyValueArray) Less(i, j int) bool {
	return a[i].Key < a[j].Key
}

type KeyValue struct {
	Key   string
	Value string
}

func MakeWorker(mapFunction func(string, string) []KeyValue, reduceFunction func(string, []string) string) {
	for {
		reply := call(TASK, "")
		if reply.TaskType == "" {
			break
		}
		switch reply.TaskType {
		case "map":
			mapPhase(&reply, mapFunction)
		case "reduce":
			reducePhase(&reply, reduceFunction)
		}
	}
}

func mapPhase(reply *Reply, mapFunction func(string, string) []KeyValue) {
	file, err := os.Open(reply.Filename)
	if err != nil {
		log.Fatalf("Error opening %v", reply.Filename)
	}
	defer file.Close()
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("Error reading %v", reply.Filename)
	}

	keyValues := mapFunction(reply.Filename, string(content))
	partitions := PartitionData(keyValues, reply.NReduce)
	for i := 0; i < reply.NReduce; i++ {
		filename := WriteIntermediateData(partitions[i], reply.MapNumberAllocated, i)
		_ = SendIntermediateFilenames(INTERMEDIATE_FILE_LOCATION, filename, i)
	}
	_ = call(FINISH_MAP, reply.Filename)
}

func reducePhase(reply *Reply, reduceFunction func(string, []string) string) {
	intermediates := []KeyValue{}
	for _, filename := range reply.ReduceFileList {
		file, err := os.Open(filename)
		if err != nil {
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

	result := []KeyValue{}

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
		output := reduceFunction(intermediates[i].Key, values)
		result = append(result, KeyValue{intermediates[i].Key, output})
		i = j
	}
	outputFilename := WriteFinalData(result, reply.ReduceNumberAllocated)
	_ = SendFinalFilenames(FINAL_FILE_LOCATION, outputFilename, i)
	_ = call(FINISH_REDUCE, strconv.Itoa(reply.ReduceNumberAllocated))
}

func call(messageType int, messageContent string) Reply {
	arguments := Arguments{}
	arguments.MessageType = messageType
	arguments.MessageContent = messageContent

	reply := Reply{}

	// call
	res := sendRPC("Master.RPCHandler", &arguments, &reply)
	if !res {
		return Reply{TaskType: ""}
	}
	return reply
}

func SendIntermediateFilenames(messageType int, messageContent string, nReduceType int) Reply {
	arguments := IntermediateFile{}
	arguments.MessageType = messageType
	arguments.MessageContent = messageContent
	arguments.NReduceNumber = nReduceType

	reply := Reply{}

	res := sendRPC("Master.IntermediateFilesHandler", &arguments, &reply)
	if !res {
		fmt.Println("Error sending location of intermediate files")
	}
	return reply
}

func SendFinalFilenames(messageType int, messageContent string, nReduceType int) Reply {
	arguments := FinalFile{}
	arguments.MessageType = messageType
	arguments.MessageContent = messageContent
	arguments.NReduceNumber = nReduceType

	reply := Reply{}

	res := sendRPC("Master.FinalFilesHandler", &arguments, &reply)
	if !res {
		fmt.Println("Error sending location of final files")
	}
	return reply
}

func sendRPC(rpcName string, arguments interface{}, reply interface{}) bool {
	socketAddress := getSocketAddress()
	client, err := rpc.DialHTTP("unix", socketAddress)
	if err != nil {
		log.Fatal("Error while dialing: ", err)
	}
	defer client.Close()

	err = client.Call(rpcName, arguments, reply)
	return err == nil
}

func WriteIntermediateData(intermediates []KeyValue, mapTaskNumber, reduceTaskNumber int) string {
	filename := "mapreduce-" + strconv.Itoa(mapTaskNumber) + "-" + strconv.Itoa(reduceTaskNumber) + ".txt"
	file, _ := os.Create(filename)

	enc := json.NewEncoder(file)
	for _, keyValue := range intermediates {
		err := enc.Encode(&keyValue)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

func WriteFinalData(intermediates []KeyValue, reduceTaskNumber int) string {
	filename := "mapreduce-out-" + strconv.Itoa(reduceTaskNumber) + ".txt"
	file, _ := os.Create(filename)

	enc := json.NewEncoder(file)
	for _, keyValue := range intermediates {
		err := enc.Encode(&keyValue)
		if err != nil {
			log.Fatal("error: ", err)
		}
	}
	return filename
}

func PartitionData(keyValues []KeyValue, nReduce int) [][]KeyValue {
	partitions := make([][]KeyValue, nReduce)

	for i := 0; i < len(keyValues); i++ {
		temp := i % nReduce
		partitions[temp] = append(partitions[temp], keyValues[i])
	}
	return partitions
}
