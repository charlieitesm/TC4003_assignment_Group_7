package cos418_hw1_1

import (
	"bufio"
	"io"
	"log"
	"os"
	"strconv"
)

// Sum numbers from channel `nums` and output sum to `out`.
// You should only output to `out` once.
// Do NOT modify function signature.
func sumWorker(nums chan int, out chan int) {
	// HINT: use for loop over `nums`
	result := 0
	for i := range nums {
		result += i
	}
	out <- result
}

// Read integers from the file `fileName` and return sum of all values.
// This function must launch `num` go routines running
// `sumWorker` to find the sum of the values concurrently.
// You should use `checkError` to handle potential errors.
// Do NOT modify function signature.
func sum(num int, fileName string) int {
	// HINT: use `readInts` and `sumWorkers`
	// HINT: used buffered channels for splitting numbers between workers
	file, err := os.Open(fileName)
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	if err != nil {
		log.Fatal(err)
	}
	numbers, err := readInts(file)

	// We'll set the buffer of the input channel to 1 in order to better split work across the available workers
	inputWorkerChan := make(chan int, 1)
	outputWorkerChan := make(chan int)

	// Instantiate the specified number of workers
	for i := 0; i < num; i++ {
		go sumWorker(inputWorkerChan, outputWorkerChan)
	}

	// Select will block until we have a slot available in the input channel, once there is, we pass on work to the map
	//  workers
	for _, n := range numbers {
		select {
		case inputWorkerChan <- n:
			continue
		}
	}
	// Let the workers know there are no more numbers coming
	close(inputWorkerChan)

	// Process the partial results into the final result (reduce operation)
	result := 0
	for i := 0; i < num; i++ {
		preliminaryResult := <-outputWorkerChan
		log.Printf("Processing partial result %d from worker...", preliminaryResult)
		result += preliminaryResult
	}

	return result
}

// Read a list of integers separated by whitespace from `r`.
// Return the integers successfully read with no error, or
// an empty slice of integers and the error that occurred.
// Do NOT modify this function.
func readInts(r io.Reader) ([]int, error) {
	scanner := bufio.NewScanner(r)
	scanner.Split(bufio.ScanWords)
	var elems []int
	for scanner.Scan() {
		val, err := strconv.Atoi(scanner.Text())
		if err != nil {
			return elems, err
		}
		elems = append(elems, val)
	}
	return elems, nil
}
