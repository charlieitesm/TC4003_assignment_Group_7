package mapreduce

import (
	"hash/fnv"
	"io/ioutil"
)

// doMap does the job of a map worker: it reads one of the input files
// (inFile), calls the user-defined map function (mapF) for that file's
// contents, and partitions the output into nReduce intermediate files.
func doMap(
	jobName string, // the name of the MapReduce job
	mapTaskNumber int, // which map task this is
	inFile string,
	nReduce int, // the number of reduce task that will be run ("R" in the paper)
	mapF func(file string, contents string) []KeyValue,
) {
	// You will need to write this function.
	// You can find the filename for this map task's input to reduce task number
	// r using reduceName(jobName, mapTaskNumber, r). The ihash function (given
	// below doMap) should be used to decide which file a given key belongs into.
	//
	// The intermediate output of a map task is stored in the file
	// system as multiple files whose name indicates which map task produced
	// them, as well as which reduce task they are for. Coming up with a
	// scheme for how to store the key/value pairs on disk can be tricky,
	// especially when taking into account that both keys and values could
	// contain newlines, quotes, and any other character you can think of.
	//
	// One format often used for serializing data to a byte stream that the
	// other end can correctly reconstruct is JSON. You are not required to
	// use JSON, but as the output of the reduce tasks *must* be JSON,
	// familiarizing yourself with it here may prove useful. You can write
	// out a data structure as a JSON string to a file using the commented
	// code below. The corresponding decoding functions can be found in
	// common_reduce.go.
	//
	//   enc := json.NewEncoder(file)
	//   for _, kv := ... {
	//     err := enc.Encode(&kv)
	//
	// Remember to close the file after you have written all the values!
	// Use checkError to handle errors.

	content, err := ioutil.ReadFile(inFile)
	checkError(err)

	wordCountsKV := mapF(inFile, string(content))

	// We want the same words to end up in the same reduceTask file, regardless of mapTask ID (input file) so that
	//  the reduce worker will quickly sum up all occurrences found of the word. We can use lists and the
	//  ihash to determine what file a word be put into

	var wordBuckets [][]KeyValue = make([][]KeyValue, nReduce)

	// Put all words in the appropriate word bucket according to its ihash
	for _, wordKV := range wordCountsKV {
		bucketNum := ihash(wordKV.Key) % nReduce // This will return a value 0 <= bucket <= nReduce
		wordBuckets[bucketNum] = append(wordBuckets[bucketNum], wordKV)
	}

	for reduceChunk, outputValues := range wordBuckets {
		// The reduceChunk for the name should be 0-based
		outputFileName := reduceName(jobName, mapTaskNumber, reduceChunk)

		writeKeyValuesToFile(outputFileName, outputValues)
	}
}

func ihash(s string) int {
	h := fnv.New32a()
	_, err := h.Write([]byte(s))
	checkError(err)
	return int(h.Sum32())
}
