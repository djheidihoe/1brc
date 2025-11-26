package main

import (
	"bufio"
	"fmt"
	"os"
	"strconv"
	"strings"
)

// Stats holds min, max, sum, and count for each city
type Stats struct {
	min   float64
	max   float64
	sum   float64
	count int64
}

func main() {
	// Adjust path if needed
	file, err := os.Open("../data/measurements.txt")
	if err != nil {
		panic(err)
	}
	defer file.Close()

	stats := make(map[string]*Stats, 1<<16) // preallocate some space

	scanner := bufio.NewScanner(file)
	// Increase buffer size for long lines
	const maxCapacity = 1024 * 1024
	buf := make([]byte, 0, 64*1024)
	scanner.Buffer(buf, maxCapacity)

	for scanner.Scan() {
		line := scanner.Text()
		// Split once on ';'
		sep := strings.IndexByte(line, ';')
		if sep < 0 {
			continue
		}
		city := line[:sep]
		valStr := line[sep+1:]
		val, err := strconv.ParseFloat(valStr, 64)
		if err != nil {
			continue
		}

		s, ok := stats[city]
		if !ok {
			stats[city] = &Stats{min: val, max: val, sum: val, count: 1}
		} else {
			if val < s.min {
				s.min = val
			}
			if val > s.max {
				s.max = val
			}
			s.sum += val
			s.count++
		}
	}

	if err := scanner.Err(); err != nil {
		panic(err)
	}

	// Print results
	for city, s := range stats {
		avg := s.sum / float64(s.count)
		fmt.Printf("%s => min: %.2f, max: %.2f, avg: %.2f\n", city, s.min, s.max, avg)
	}
}
