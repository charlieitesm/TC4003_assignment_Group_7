package cos418_hw1_1

import (
	"bufio"
	"fmt"
	"log"
	"os"
	"regexp"
	"sort"
	"strings"
)

// Find the top K most common words in a text document.
// 	path: location of the document
//	numWords: number of words to return (i.e. k)
//	charThreshold: character threshold for whether a token qualifies as a word,
//		e.g. charThreshold = 5 means "apple" is a word but "pear" is not.
// Matching is case insensitive, e.g. "Orange" and "orange" is considered the same word.
// A word comprises alphanumeric characters only. All punctuations and other characters
// are removed, e.g. "don't" becomes "dont".
// You should use `checkError` to handle potential errors.
func topWords(path string, numWords int, charThreshold int) []WordCount {
	// HINT: You may find the `strings.Fields` and `strings.ToLower` functions helpful
	// HINT: To keep only alphanumeric characters, use the regex "[^0-9a-zA-Z]+"

	var result []WordCount
	// We'll use this cache to quickly access previous encounters of a word
	var cache = make(map[string]int)

	file, err := os.Open(path)

	if err != nil {
		log.Fatal(err)
	}

	// We defer the closing of the file to the end of the execution of topWords in order to prevent a resource leak
	defer func() {
		err = file.Close()
		if err != nil {
			log.Fatal(err)
		}
	}()

	// We can take advantage of the ScanWords scanner to retrieve tokens separated by spaces
	scanner := bufio.NewScanner(file)
	scanner.Split(bufio.ScanWords)

	// This for will retrieve tokens until it fails or returns false, signaling the EOF
	for scanner.Scan() {
		// Get the word and remove any non-alphanumeric characters, transform it to lower case
		word := regexp.MustCompile("[^0-9a-zA-Z]+").ReplaceAllString(scanner.Text(), "")
		word = strings.ToLower(word)

		if len(word) < charThreshold {
			continue
		}
		cache[word] = cache[word] + 1
	}

	// Transform the cache map into WordCount structs
	for word, count := range cache {
		result = append(result, WordCount{word, count})
	}

	sortWordCounts(result)
	// Create a slice with only the requested number of words
	return result[:numWords]
}

// A struct that represents how many times a word is observed in a document
type WordCount struct {
	Word  string
	Count int
}

func (wc WordCount) String() string {
	return fmt.Sprintf("%v: %v", wc.Word, wc.Count)
}

// Helper function to sort a list of word counts in place.
// This sorts by the count in decreasing order, breaking ties using the word.
// DO NOT MODIFY THIS FUNCTION!
func sortWordCounts(wordCounts []WordCount) {
	sort.Slice(wordCounts, func(i, j int) bool {
		wc1 := wordCounts[i]
		wc2 := wordCounts[j]
		if wc1.Count == wc2.Count {
			return wc1.Word < wc2.Word
		}
		return wc1.Count > wc2.Count
	})
}
