package mapreduce

import "sort"

// doReduce does the job of a reduce worker: it reads the intermediate
// key/value pairs (produced by the map phase) for this task, sorts the
// intermediate key/value pairs by key, calls the user-defined reduce function
// (reduceF) for each key, and writes the output to disk.
func doReduce(
	jobName string, // the name of the whole MapReduce job
	reduceTaskNumber int, // which reduce task this is
	nMap int, // the number of map tasks that were run ("M" in the paper)
	reduceF func(key string, values []string) string,
) {
	// You will need to write this function.
	// You can find the intermediate file for this reduce task from map task number
	// m using reduceName(jobName, m, reduceTaskNumber).
	// Remember that you've encoded the values in the intermediate files, so you
	// will need to decode them. If you chose to use JSON, you can read out
	// multiple decoded values by creating a decoder, and then repeatedly calling
	// .Decode() on it until Decode() returns an error.
	//
	// You should write the reduced output in as JSON encoded KeyValue
	// objects to a file named mergeName(jobName, reduceTaskNumber). We require
	// you to use JSON here because that is what the merger than combines the
	// output from all the reduce tasks expects. There is nothing "special" about
	// JSON -- it is just the marshalling format we chose to use. It will look
	// something like this:
	//
	// enc := json.NewEncoder(mergeFile)
	// for key in ... {
	// 	enc.Encode(KeyValue{key, reduceF(...)})
	// }
	// file.Close()
	//
	// Use checkError to handle errors.
	var outputKV []KeyValue

	// We'll compile the values for the same keys across different files and put them in a []string
	//  After we're done reading from all files, we'll apply the reduce function and put the result
	//  in KeyValue structs for output
	var keys []string
	valueCache := make(map[string][]string)

	for mapTaskNum := 0; mapTaskNum < nMap; mapTaskNum++ {
		mapInputFileName := reduceName(jobName, mapTaskNum, reduceTaskNumber)
		intermediateOutput := readIntermediateOutputFile(mapInputFileName)

		reduceFileHash := ihash(mapInputFileName)
		intermediateValue := intermediateOutput[reduceFileHash]

		for _, kv := range intermediateValue {
			key := kv.Key

			if _, keyAlreadyExists := valueCache[key]; !keyAlreadyExists {
				keys = append(keys, key)
			}
			valueCache[key] = append(valueCache[key], kv.Value)
		}
	}

	sort.Strings(keys)

	// Apply the reduce function for all values for the same key across all map files
	for _, k := range keys {
		key := k
		outputKV = append(outputKV, KeyValue{Key: key, Value: reduceF(key, valueCache[k])})
	}

	outputMergeFileName := mergeName(jobName, reduceTaskNumber)
	writeMergedFile(outputMergeFileName, outputKV)
}
