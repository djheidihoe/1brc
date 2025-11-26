package main

import (
	"errors"
	"fmt"
	"hash/fnv"
	"os"
	"path/filepath"
	"runtime"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const (
	inputFile     = "../data/measurements.txt"
	tmpDir        = "shards_tmp"
	shardCount    = 32
	maxLineLength = 128
)

type Stats struct {
	Min   float64
	Max   float64
	Sum   float64
	Count int64
}

// mmap the entire file into memory
func mmapFile(path string) ([]byte, error) {
	f, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	stat, err := f.Stat()
	if err != nil {
		return nil, err
	}

	size := stat.Size()
	if size == 0 {
		return nil, errors.New("file empty")
	}

	data, err := syscall.Mmap(
		int(f.Fd()),
		0,
		int(size),
		syscall.PROT_READ,
		syscall.MAP_PRIVATE,
	)
	if err != nil {
		return nil, err
	}

	return data, nil
}

// much faster value parser for formats like -12.3, 4.3, -0.1
func fastParseFloat(b []byte) (float64, error) {
	return strconv.ParseFloat(string(b), 64)
}

// find ';' without bounds checks and without allocations
func findSep(b []byte) int {
	for i := 0; i < len(b); i++ {
		if b[i] == ';' {
			return i
		}
	}
	return -1
}

func shardIndex(b []byte) int {
	h := fnv.New32a()
	_, _ = h.Write(b)
	return int(h.Sum32()) % shardCount
}

func main() {
	start := time.Now()

	runtime.GOMAXPROCS(runtime.NumCPU())

	fmt.Println("M2 Max optimized version running...")

	// mmap file
	data, err := mmapFile(inputFile)
	if err != nil {
		panic(err)
	}

	// prepare shard directory
	_ = os.RemoveAll(tmpDir)
	_ = os.Mkdir(tmpDir, 0o755)

	// create shard writers
	shardFiles := make([]*os.File, shardCount)
	shardBuf := make([][]byte, shardCount)

	for i := range shardFiles {
		path := filepath.Join(tmpDir, fmt.Sprintf("shard_%02d", i))
		f, err := os.Create(path)
		if err != nil {
			panic(err)
		}
		shardFiles[i] = f
		shardBuf[i] = make([]byte, 0, 8<<20) // 8MB buffers
	}

	//////////////////////////////
	// PHASE 1: SHARD (mmap scan)
	//////////////////////////////
	lineStart := 0

	for i := 0; i < len(data); i++ {
		if data[i] != '\n' {
			continue
		}

		line := data[lineStart:i] // no allocations
		lineStart = i + 1

		if len(line) == 0 {
			continue
		}

		// find station end
		sep := findSep(line)
		if sep <= 0 {
			continue
		}

		station := line[:sep] // raw bytes
		sh := shardIndex(station)

		// append to shard buffer
		shardBuf[sh] = append(shardBuf[sh], line...)
		shardBuf[sh] = append(shardBuf[sh], '\n')
	}

	// write shard buffers
	for i := range shardFiles {
		_, _ = shardFiles[i].Write(shardBuf[i])
		shardFiles[i].Close()
	}

	//////////////////////////////
	// PHASE 2: PARALLEL AGGREGATE
	//////////////////////////////

	type ShardOut struct {
		m map[string]Stats
	}

	out := make(chan ShardOut, shardCount)
	var wg sync.WaitGroup

	sem := make(chan struct{}, runtime.NumCPU())

	for s := 0; s < shardCount; s++ {
		wg.Add(1)
		sem <- struct{}{}

		go func(idx int) {
			defer wg.Done()
			defer func() { <-sem }()

			path := filepath.Join(tmpDir, fmt.Sprintf("shard_%02d", idx))
			raw, err := os.ReadFile(path)
			if err != nil {
				out <- ShardOut{m: make(map[string]Stats)}
				return
			}

			m := make(map[string]Stats, 512)
			start := 0

			for i := 0; i < len(raw); i++ {
				if raw[i] != '\n' {
					continue
				}

				line := raw[start:i]
				start = i + 1

				sep := findSep(line)
				if sep <= 0 {
					continue
				}

				station := string(line[:sep])
				valBytes := line[sep+1:]

				v, err := fastParseFloat(valBytes)
				if err != nil {
					continue
				}

				if st, ok := m[station]; ok {
					if v < st.Min {
						st.Min = v
					}
					if v > st.Max {
						st.Max = v
					}
					st.Sum += v
					st.Count++
					m[station] = st
				} else {
					m[station] = Stats{Min: v, Max: v, Sum: v, Count: 1}
				}
			}

			out <- ShardOut{m: m}
		}(s)
	}

	// waiter closes out channel
	go func() {
		wg.Wait()
		close(out)
	}()

	//////////////////////////////
	// FINAL MERGE
	//////////////////////////////

	final := make(map[string]Stats)

	for sh := range out {
		for station, s := range sh.m {
			if ex, ok := final[station]; ok {
				if s.Min < ex.Min {
					ex.Min = s.Min
				}
				if s.Max > ex.Max {
					ex.Max = s.Max
				}
				ex.Sum += s.Sum
				ex.Count += s.Count
				final[station] = ex
			} else {
				final[station] = s
			}
		}
	}

	//////////////////////////////
	// PRINT
	//////////////////////////////

	for st, s := range final {
		avg := s.Sum / float64(s.Count)
		fmt.Printf("%s=%.1f/%.1f/%.1f\n", st, s.Min, avg, s.Max)
	}

	end := time.Since(start)
	fmt.Printf("\nCompleted in %v (M2 Max optimized)\n", end)
}
