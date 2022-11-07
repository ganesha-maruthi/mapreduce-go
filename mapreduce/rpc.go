package mapreduce

const (
	TASK = iota
	INTERMEDIATE_FILE_LOCATION
	FINAL_FILE_LOCATION
	FINISH_MAP
	FINISH_REDUCE
)

type Arguments struct {
	MessageType    int
	MessageContent string
}

type IntermediateFile struct {
	MessageType    int
	MessageContent string
	NReduceNumber  int
}

type FinalFile struct {
	MessageType    int
	MessageContent string
	NReduceNumber  int
}

type Reply struct {
	Filename              string
	MapNumberAllocated    int
	NReduce               int
	ReduceNumberAllocated int
	TaskType              string
	ReduceFileList        []string
}

func getSocketAddress() string {
	return "/tmp/randomname.sock"
}
