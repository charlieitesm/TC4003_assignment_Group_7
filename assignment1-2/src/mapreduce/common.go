package mapreduce

import (
	"encoding/json"
	"fmt"
	"log"
	"os"
	"strconv"
)

// Debugging enabled?
const debugEnabled = false

// DPrintf will only print if the debugEnabled const has been set to true
func debug(format string, a ...interface{}) (n int, err error) {
	if debugEnabled {
		n, err = fmt.Printf(format, a...)
	}
	return
}

// Propagate error if it exists
func checkError(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// jobPhase indicates whether a task is scheduled as a map or reduce task.
type jobPhase string

const (
	mapPhase    jobPhase = "Map"
	reducePhase          = "Reduce"
)

// KeyValue is a type used to hold the key/value pairs passed to the map and
// reduce functions.
type KeyValue struct {
	Key   string
	Value string
}

// reduceName constructs the name of the intermediate file which map task
// <mapTask> produces for reduce task <reduceTask>.
func reduceName(jobName string, mapTask int, reduceTask int) string {
	return "mrtmp." + jobName + "-" + strconv.Itoa(mapTask) + "-" + strconv.Itoa(reduceTask)
}

// mergeName constructs the name of the output file of reduce task <reduceTask>
func mergeName(jobName string, reduceTask int) string {
	return "mrtmp." + jobName + "-res-" + strconv.Itoa(reduceTask)
}

func writeIntermediateOutputFile(filename string, output interface{}) {
	outputFile, err := os.Create(filename)
	checkError(err)
	defer func() {
		err := outputFile.Close()
		checkError(err)
	}()

	enc := json.NewEncoder(outputFile)
	err = enc.Encode(&output)
	checkError(err)
}

func readIntermediateOutputFile(filename string) map[uint32][]KeyValue {
	inputFile, err := os.Open(filename)
	checkError(err)
	defer func() {
		err := inputFile.Close()
		checkError(err)
	}()

	var result map[uint32][]KeyValue

	dec := json.NewDecoder(inputFile)

	err = dec.Decode(&result)
	checkError(err)
	return result
}

func writeMergedFile(filename string, output []KeyValue) {
	outputFile, err := os.Create(filename)
	checkError(err)
	defer func() {
		err := outputFile.Close()
		checkError(err)
	}()
	enc := json.NewEncoder(outputFile)
	for _, v := range output {
		err = enc.Encode(v)
		checkError(err)
	}
}
