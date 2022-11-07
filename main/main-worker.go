package main

import (
	"mapreduce-go/def"
	"mapreduce-go/mapreduce"
)

func main() {
	mapreduce.MakeWorker(def.Map, def.Reduce)
}
