package main

import (
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
)

// Stat holds metrics in integer tenths for speed and precision
type Stat struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

func main() {
	// --- CPU profiling setup ---
	cpuFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// --- Memory profiling setup ---
	defer func() {
		memFile, err := os.Create("mem.prof")
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(memFile)
		memFile.Close()
	}()

	path := "../data/measurements.txt"

	f, err := os.Open(path)
	if err != nil {
		panic(err)
	}
	defer f.Close()

	info, err := f.Stat()
	if err != nil {
		panic(err)
	}
	size := info.Size()
	if size == 0 {
		return
	}

	// Use all cores
	nCPU := runtime.NumCPU()
	runtime.GOMAXPROCS(nCPU)
	workers := nCPU

	// Chunking with overlap to align at newline boundaries
	// Each worker reads [start, end+overlap] and then trims to full lines
	chunk := size / int64(workers)
	const overlap = int64(1 << 20) // 1MB overlap for boundary search

	type Work struct {
		start int64
		end   int64
	}
	wks := make([]Work, 0, workers)
	var s int64 = 0
	for i := 0; i < workers; i++ {
		e := s + chunk - 1
		if i == workers-1 {
			e = size - 1
		}
		wks = append(wks, Work{start: s, end: e})
		s = e + 1
	}

	// Per-worker local maps to avoid contention.
	// We use map[string]Stat; keys are city names as strings (allocation unavoidable).
	locals := make([]map[string]Stat, workers)
	// Heuristic: estimate average line length ~ 20-40 bytes; pre-size maps to reduce rehash.
	estPerWorker := int(size/int64(workers)) / 32
	if estPerWorker < 2048 {
		estPerWorker = 2048
	}

	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		i := i
		go func() {
			defer wg.Done()

			// Read with overlap
			start := wks[i].start
			end := wks[i].end
			readStart := start
			readEnd := end + overlap
			if readEnd >= size {
				readEnd = size - 1
			}
			readLen := readEnd - readStart + 1
			buf := make([]byte, readLen)
			_, err := f.ReadAt(buf, readStart)
			if err != nil {
				// For big files, partial read errors are possible; keep simple: panic
				panic(err)
			}

			// Adjust to start at the first newline after 'start' (unless first chunk)
			offset := int64(0)
			if start != 0 {
				for offset < readLen {
					if buf[offset] == '\n' {
						offset++
						break
					}
					offset++
				}
				if offset >= readLen {
					// No newline found in overlap; nothing to process
					return
				}
			}

			// Trim end to the last newline before end+overlap
			limit := readLen
			if end < size-1 {
				// scan backward to newline
				j := readLen - 1
				for j >= 0 && buf[j] != '\n' {
					j--
				}
				if j >= 0 {
					limit = j + 1 // include newline
				}
			}

			m := make(map[string]Stat, estPerWorker)
			parseChunk(buf[offset:limit], m)
			locals[i] = m
		}()
	}

	wg.Wait()

	// Merge local maps
	global := make(map[string]Stat, workers*estPerWorker)
	for _, m := range locals {
		for city, st := range m {
			if g, ok := global[city]; !ok {
				global[city] = st
			} else {
				// merge
				if st.min < g.min {
					g.min = st.min
				}
				if st.max > g.max {
					g.max = st.max
				}
				g.sum += st.sum
				g.count += st.count
				global[city] = g
			}
		}
	}

	// Output: min, max, avg with two decimals
	// for city, s := range global {
	// 	avg := float64(s.sum) / float64(s.count) / 10.0
	// 	fmt.Printf("%s => min: %.1f, max: %.1f, avg: %.2f\n",
	// 		city, float64(s.min)/10.0, float64(s.max)/10.0, avg)
	// }
}

// parseChunk scans the buffer line-by-line using byte ops,
// lines are "City;[-]dd.d\n"
func parseChunk(buf []byte, m map[string]Stat) {
	n := len(buf)
	i := 0
	for i < n {
		// line start
		lineStart := i

		// find ';'
		semi := -1
		for i < n {
			b := buf[i]
			if b == ';' {
				semi = i
				i++
				break
			}
			if b == '\n' {
				// malformed line; skip
				i++
				lineStart = i
				semi = -1
				continue
			}
			i++
		}
		if semi < 0 {
			// no semicolon found until end; stop
			break
		}

		// parse temperature after ';' until newline
		// format: [+-]?digits '.' digit
		sign := int32(1)
		if i < n {
			if buf[i] == '-' {
				sign = -1
				i++
			} else if buf[i] == '+' {
				i++
			}
		}

		// integer part
		var intPart int32 = 0
		for i < n {
			c := buf[i]
			if c >= '0' && c <= '9' {
				intPart = intPart*10 + int32(c-'0')
				i++
			} else {
				break
			}
		}

		// expect '.' then one decimal digit
		if i < n && buf[i] == '.' {
			i++
		}
		var decDigit int32 = 0
		if i < n {
			c := buf[i]
			if c >= '0' && c <= '9' {
				decDigit = int32(c - '0')
				i++
			}
		}

		// move to end of line (newline)
		for i < n && buf[i] != '\n' {
			i++
		}
		// end of line index
		lineEnd := i
		if i < n && buf[i] == '\n' {
			i++ // advance to next line
		}

		// city key as string
		city := string(buf[lineStart:semi]) // allocates once per city occurrence

		// temperature in tenths
		tenth := sign * (intPart*10 + decDigit)

		// aggregate
		if st, ok := m[city]; ok {
			if tenth < st.min {
				st.min = tenth
			}
			if tenth > st.max {
				st.max = tenth
			}
			st.sum += int64(tenth)
			st.count++
			m[city] = st
		} else {
			m[city] = Stat{
				min:   tenth,
				max:   tenth,
				sum:   int64(tenth),
				count: 1,
			}
		}
		_ = lineEnd // kept for clarity; not needed after city extraction
	}
}
