package main

import (
	"fmt"
	"mapreduce-go/mapreduce"
	"os"
	"strconv"
	"time"
)

func main() {
	if len(os.Args) < 3 {
		fmt.Fprintf(os.Stderr, "Usage: go run main/main-master.go <n-reduce> <input-files>\n")
		os.Exit(1)
	}

	nReduce, _ := strconv.Atoi(os.Args[1])
	m := mapreduce.MakeMaster(os.Args[2:], nReduce)
	for !m.Done() {
		time.Sleep(time.Second)
	}

	time.Sleep(time.Second)
}
