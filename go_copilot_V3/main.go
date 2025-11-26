package main

import (
	"fmt"
	"os"
	"runtime"
	"runtime/pprof"
	"sync"
	"syscall"
)

// Stat holds metrics in integer tenths
type Stat struct {
	min   int32
	max   int32
	sum   int64
	count int64
}

// Intern is a sharded interner that assigns a compact int32 ID for each unique city.
// Lookups are by 64-bit FNV-1a hash; collisions are resolved by byte-wise compare
// against the stored string without allocating temporary strings.
type Intern struct {
	shards  [256]internShard
	names   []string
	namesMu sync.Mutex
}

type internShard struct {
	mu sync.RWMutex
	m  map[uint64][]int32 // hash -> list of IDs to resolve collisions
}

func newIntern() *Intern {
	in := &Intern{}
	for i := range in.shards {
		in.shards[i].m = make(map[uint64][]int32, 4096)
	}
	return in
}

func (in *Intern) GetOrAdd(b []byte) int32 {
	h := fnv1a64(b)
	sh := &in.shards[h&255]

	// fast read path
	sh.mu.RLock()
	ids := sh.m[h]
	sh.mu.RUnlock()
	for _, id := range ids {
		if equalSB(in.names[id], b) {
			return id
		}
	}

	// not found: allocate once and register
	s := string(b)
	in.namesMu.Lock()
	id := int32(len(in.names))
	in.names = append(in.names, s)
	in.namesMu.Unlock()

	// write into shard
	sh.mu.Lock()
	sh.m[h] = append(ids, id)
	sh.mu.Unlock()

	return id
}

func (in *Intern) Name(id int32) string {
	return in.names[id]
}

func equalSB(s string, b []byte) bool {
	if len(s) != len(b) {
		return false
	}
	for i := 0; i < len(b); i++ {
		if s[i] != b[i] {
			return false
		}
	}
	return true
}

func fnv1a64(b []byte) uint64 {
	const (
		off   = 1469598103934665603
		prime = 1099511628211
	)
	h := uint64(off)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime
	}
	return h
}

func main() {
	// --- CPU profiling ---
	cpuFile, err := os.Create("cpu.prof")
	if err != nil {
		panic(err)
	}
	pprof.StartCPUProfile(cpuFile)
	defer pprof.StopCPUProfile()

	// --- Memory profiling ---
	defer func() {
		memFile, err := os.Create("mem.prof")
		if err != nil {
			panic(err)
		}
		pprof.WriteHeapProfile(memFile)
		memFile.Close()
	}()

	// --- mmap file ---
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

	data, err := syscall.Mmap(int(f.Fd()), 0, int(size), syscall.PROT_READ, syscall.MAP_SHARED)
	if err != nil {
		panic(err)
	}
	defer syscall.Munmap(data)

	// --- parallel parsing ---
	nCPU := runtime.NumCPU()
	// Empirically, 6–10 workers can be optimal on M2 Max due to memory bandwidth vs. GC;
	// use min(nCPU, 8) as a starting point.
	workers := nCPU
	if workers > 8 {
		workers = 8
	}
	runtime.GOMAXPROCS(workers)

	chunk := len(data) / workers
	intern := newIntern()

	locals := make([]map[int32]Stat, workers)
	var wg sync.WaitGroup
	wg.Add(workers)

	for i := 0; i < workers; i++ {
		start := i * chunk
		end := start + chunk
		if i == workers-1 {
			end = len(data)
		}
		// align start to line boundary (skip partial line)
		if i > 0 {
			for start < end && data[start] != '\n' {
				start++
			}
			if start < end {
				start++
			}
		}
		// align end to include up to next newline (so last line is complete)
		if i < workers-1 {
			for end < len(data) && data[end] != '\n' {
				end++
			}
		}

		go func(idx, s, e int) {
			defer wg.Done()
			// heuristic pre-size: unique cities per worker are usually small relative to rows
			m := make(map[int32]Stat, 8192)
			parseChunkIDs(data[s:e], m, intern)
			locals[idx] = m
		}(i, start, end)
	}

	wg.Wait()

	// --- merge results ---
	global := make(map[int32]Stat, 1<<16)
	for _, m := range locals {
		for id, st := range m {
			if g, ok := global[id]; !ok {
				global[id] = st
			} else {
				if st.min < g.min {
					g.min = st.min
				}
				if st.max > g.max {
					g.max = st.max
				}
				g.sum += st.sum
				g.count += st.count
				global[id] = g
			}
		}
	}

	// --- output ---
	for id, s := range global {
		avg := float64(s.sum) / float64(s.count) / 10.0
		fmt.Printf("%s => min: %.1f, max: %.1f, avg: %.2f\n",
			intern.Name(id), float64(s.min)/10.0, float64(s.max)/10.0, avg)
	}
}

// parseChunkIDs scans buffer line-by-line, aggregates by city ID (int32).
// Format: City;[-]dd.d\n
func parseChunkIDs(buf []byte, m map[int32]Stat, intern *Intern) {
	n := len(buf)
	i := 0
	for i < n {
		lineStart := i

		// find semicolon
		semi := -1
		for i < n {
			b := buf[i]
			if b == ';' {
				semi = i
				i++
				break
			}
			if b == '\n' {
				// empty/malformed line
				i++
				lineStart = i
				continue
			}
			i++
		}
		if semi < 0 {
			break
		}

		// parse temperature
		sign := int32(1)
		if i < n {
			switch buf[i] {
			case '-':
				sign = -1
				i++
			case '+':
				i++
			}
		}
		var intPart int32
		for i < n {
			c := buf[i]
			if c >= '0' && c <= '9' {
				intPart = intPart*10 + int32(c-'0')
				i++
			} else {
				break
			}
		}
		if i < n && buf[i] == '.' {
			i++
		}
		var decDigit int32
		if i < n {
			c := buf[i]
			if c >= '0' && c <= '9' {
				decDigit = int32(c - '0')
				i++
			}
		}
		// consume rest until newline
		for i < n && buf[i] != '\n' {
			i++
		}
		if i < n && buf[i] == '\n' {
			i++
		}

		// get city ID via interner, avoiding temp string allocations
		cityID := intern.GetOrAdd(buf[lineStart:semi])
		tenth := sign * (intPart*10 + decDigit)

		if st, ok := m[cityID]; ok {
			if tenth < st.min {
				st.min = tenth
			}
			if tenth > st.max {
				st.max = tenth
			}
			st.sum += int64(tenth)
			st.count++
			m[cityID] = st
		} else {
			m[cityID] = Stat{
				min:   tenth,
				max:   tenth,
				sum:   int64(tenth),
				count: 1,
			}
		}
	}
}
