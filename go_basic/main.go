package main

import (
	"bufio"
	"fmt"
	"os"
	"runtime"
	"strconv"
	"sync"
)

// Stats holds min, max, sum, count
type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

func main() {
	runtime.GOMAXPROCS(runtime.NumCPU())

	filename := "../data/measurements.txt" // change if needed
	f, err := os.Open(filename)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	reader := bufio.NewReaderSize(f, 16<<20) // 16MB read buffer

	lineChan := make(chan []byte, 100000)
	resultChan := make(chan map[string]Stats, runtime.NumCPU())

	workerCount := runtime.NumCPU()
	var wg sync.WaitGroup

	// ---------------- WORKERS ----------------
	for i := 0; i < workerCount; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()

			local := make(map[string]Stats)

			for line := range lineChan {
				if len(line) == 0 {
					continue
				}

				// find ';'
				sep := -1
				for i := 0; i < len(line); i++ {
					if line[i] == ';' {
						sep = i
						break
					}
				}
				if sep == -1 {
					continue
				}

				station := string(line[:sep])
				valBytes := line[sep+1:]

				v, err := strconv.ParseFloat(string(valBytes), 64)
				if err != nil {
					continue
				}

				s, ok := local[station]
				if !ok {
					local[station] = Stats{
						Min:   v,
						Max:   v,
						Sum:   v,
						Count: 1,
					}
					continue
				}

				if v < s.Min {
					s.Min = v
				}
				if v > s.Max {
					s.Max = v
				}
				s.Sum += v
				s.Count++

				local[station] = s
			}

			resultChan <- local
		}()
	}

	// ---------------- READER ----------------
	go func() {
		for {
			line, err := reader.ReadBytes('\n')
			if len(line) > 0 {
				// strip newline
				if line[len(line)-1] == '\n' {
					line = line[:len(line)-1]
				}
				b := make([]byte, len(line))
				copy(b, line)
				lineChan <- b
			}
			if err != nil {
				break
			}
		}
		close(lineChan)
	}()

	// ---------------- CLOSE RESULT CHAN WHEN DONE ----------------
	go func() {
		wg.Wait()
		close(resultChan)
	}()

	// ---------------- MERGE RESULTS ----------------
	final := make(map[string]Stats)

	for partial := range resultChan {
		for station, p := range partial {
			s, ok := final[station]
			if !ok {
				final[station] = p
				continue
			}

			if p.Min < s.Min {
				s.Min = p.Min
			}
			if p.Max > s.Max {
				s.Max = p.Max
			}
			s.Sum += p.Sum
			s.Count += p.Count

			final[station] = s
		}
	}

	// ---------------- OUTPUT ----------------
	for station, s := range final {
		avg := s.Sum / float64(s.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", station, s.Min, avg, s.Max)
	}
}
