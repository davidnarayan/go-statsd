// statspipe is a metrics pipeline
package main

import (
	"bufio"
	"bytes"
	"flag"
	"fmt"
	"io"
	"log"
	"math"
	"net"
	"regexp"
	"sort"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//"github.com/davecgh/go-spew/spew"
	"github.com/davecheney/profile"
)

const FlushInterval = time.Duration(10 * time.Second)
const BufSize = 8192

//-----------------------------------------------------------------------------

// Command line flags
var (
	listen       = flag.String("listen", ":1514", "UDP listener address")
	graphite     = flag.String("graphite", "localhost:2003", "Graphite server address")
	cpuprofile   = flag.Bool("cpuprofile", false, "Enable CPU profiling")
	memprofile   = flag.Bool("memprofile", false, "Enable memory profiling")
	blockprofile = flag.Bool("blockprofile", false, "Enable memory profiling")
)

// Metric Types
var In = make(chan *Metric)

var counters = struct {
	sync.RWMutex
	m map[string]int64
}{m: make(map[string]int64)}

var gauges = struct {
	sync.RWMutex
	m map[string]uint64
}{m: make(map[string]uint64)}

type Timers []uint64

func (t Timers) Len() int           { return len(t) }
func (t Timers) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Timers) Less(i, j int) bool { return t[i] < t[j] }

var timers = struct {
	sync.RWMutex
	m map[string]Timers
}{m: make(map[string]Timers)}

var Percentiles = []int{5, 95}

// Internal metrics
type Stats struct {
	IngressRate     int64
	IngressMetrics  int64
	IngressCounters int64
	IngressGauges   int64
	IngressTimers   int64
}

var stats = &Stats{}

//-----------------------------------------------------------------------------
// Read syslog stream

func ListenUDP(addr string) error {
	var buf = make([]byte, 1024)
	ln, err := net.ResolveUDPAddr("udp", addr)

	if err != nil {
		return err
	}

	sock, err := net.ListenUDP("udp", ln)

	if err != nil {
		return err
	}

	log.Printf("Listening on UDP %s\n", ln)

	for {
		n, raddr, err := sock.ReadFromUDP(buf[:])

		if err != nil {
			// TODO: handle error
			continue
		}

		log.Printf("Read %d bytes from %s\n", n, raddr)
		go handleMessage(buf)
	}
}

func ListenTCP(addr string) error {
	l, err := net.Listen("tcp", addr)

	if err != nil {
		return err
	}

	defer l.Close()
	log.Printf("Listening on TCP %s\n", l.Addr())

	for {
		conn, err := l.Accept()

		if err != nil {
			// TODO: handle error
			continue
		}

		go handleConnection(conn)
	}
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	for {
		line, err := r.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				break
			} else {
				// TODO: handle error
			}
		}

		handleMessage(line)
	}
}

// Metrics should be in statsd format
// <metric_name>:<metric_value>|<metric_type>
var statsPattern = regexp.MustCompile(`[\w\.]+:-?\d+\|(?:c|ms|g)(?:\|\@[\d\.]+)?`)

// Handle an event message
func handleMessage(buf []byte) {
	//log.Printf("DEBUG: buf is %d bytes\n", len(buf))
	// Parse metrics from the message
	m := statsPattern.FindAll(buf, -1)
	//spew.Dump(m)

	if m != nil {
		for _, metric := range m {
			err := handleMetric(metric)

			if err != nil {
				log.Printf("ERROR: Unable to process metric %s: %s",
					metric, err)
			}
		}
	} else {
		log.Println("No metrics found in message")
	}
}

type Metric struct {
	Name  string
	Value interface{}
	Type  string
}

// handle a single metric
func handleMetric(b []byte) error {
	i := bytes.Index(b, []byte(":"))
	j := bytes.Index(b, []byte("|"))
	k := bytes.Index(b, []byte("@"))
	v := b[i+1 : j]

	// End position of the metric type is the end of the byte slice
	// if no sample was sent.
	tEnd := len(b)
	var sampleRate float64 = 1

	if k > -1 {
		tEnd = k - 1 // Use -1 because of the | before the @
		sr := b[(k + 1):len(b)]
		var err error
		sampleRate, err = strconv.ParseFloat(string(sr), 64)

		if err != nil {
			return err
		}
	}

	m := &Metric{
		Name: string(b[0:i]),
		Type: string(b[j+1 : tEnd]),
	}

	switch m.Type {
	case "c":
		val, err := strconv.ParseInt(string(v), 10, 64)

		if err != nil {
			return err
		}

		m.Value = int64(float64(val) / sampleRate)
	default:
		val, err := strconv.ParseUint(string(v), 10, 64)

		if err != nil {
			return err
		}

		m.Value = val
	}

	In <- m
	return nil
}

func processMetrics() {
	ticker := time.NewTicker(FlushInterval)

	for {
		select {
		case <-ticker.C:
			flushMetrics()
		case m := <-In:
			atomic.AddInt64(&stats.IngressMetrics, 1)

			switch m.Type {
			case "c":
				counters.Lock()
				counters.m[m.Name] += m.Value.(int64)
				counters.Unlock()
				atomic.AddInt64(&stats.IngressCounters, 1)

			case "g":
				gauges.Lock()
				gauges.m[m.Name] = m.Value.(uint64)
				gauges.Unlock()
				atomic.AddInt64(&stats.IngressGauges, 1)

			case "ms":
				timers.Lock()
				_, ok := timers.m[m.Name]

				if !ok {
					var t Timers
					timers.m[m.Name] = t
				}

				timers.m[m.Name] = append(timers.m[m.Name], m.Value.(uint64))
				timers.Unlock()
				atomic.AddInt64(&stats.IngressTimers, 1)

			}
		}
	}
}

func flushMetrics() {
	var buf bytes.Buffer
	now := time.Now().Unix()

	log.Printf("%+v", stats)

	// Build buffer of stats
	flushCounters(&buf, now)
	flushGauges(&buf, now)
	flushTimers(&buf, now)
	flushInternalStats(&buf, now)

	// Send metrics to Graphite
	sendGraphite(&buf)
}

func flushInternalStats(buf *bytes.Buffer, now int64) {
	//fmt.Fprintf(buf, "statsd.metrics.per_second %d %d\n", v, now)
	fmt.Fprintf(buf, "statsd.metrics.count %d %d\n",
		atomic.LoadInt64(&stats.IngressMetrics), now)
	fmt.Fprintf(buf, "statsd.counters.count %d %d\n",
		atomic.LoadInt64(&stats.IngressCounters), now)
	fmt.Fprintf(buf, "statsd.gauges.count %d %d\n",
		atomic.LoadInt64(&stats.IngressGauges), now)
	fmt.Fprintf(buf, "statsd.timers.count %d %d\n",
		atomic.LoadInt64(&stats.IngressTimers), now)

	// Clear internal metrics
	atomic.StoreInt64(&stats.IngressMetrics, 0)
	atomic.StoreInt64(&stats.IngressCounters, 0)
	atomic.StoreInt64(&stats.IngressGauges, 0)
	atomic.StoreInt64(&stats.IngressTimers, 0)
}

func flushCounters(buf *bytes.Buffer, now int64) {
	counters.Lock()
	defer counters.Unlock()

	for k, v := range counters.m {
		fmt.Fprintf(buf, "%s %d %d\n", k, v, now)
		delete(counters.m, k)
	}
}

func flushGauges(buf *bytes.Buffer, now int64) {
	gauges.Lock()
	defer gauges.Unlock()

	for k, v := range gauges.m {
		fmt.Fprintf(buf, "%s %d %d\n", k, v, now)
		delete(gauges.m, k)
	}
}

func flushTimers(buf *bytes.Buffer, now int64) {
	timers.RLock()
	defer timers.RUnlock()
	var n int64

	for k, t := range timers.m {
		count := len(t)

		// Skip processing if there are no timer values
		if count < 1 {
			break
		}

		var sum uint64

		for _, v := range t {
			sum += v
			n++
		}

		// Linear average (mean)
		mean := float64(sum) / float64(count)

		// Min and Max
		sort.Sort(t)
		min := t[0]
		max := t[len(t)-1]

		// Write out all derived stats
		fmt.Fprintf(buf, "%s.count %d %d\n", k, count, now)
		fmt.Fprintf(buf, "%s.mean %f %d\n", k, mean, now)
		fmt.Fprintf(buf, "%s.lower %d %d\n", k, min, now)
		fmt.Fprintf(buf, "%s.upper %d %d\n", k, max, now)

		// Calculate and write out percentiles
		for _, pct := range Percentiles {
			p := perc(t, pct)
			fmt.Fprintf(buf, "%s.perc%d %f %d\n", k, pct, p, now)
		}

		delete(timers.m, k)
	}
}

// percentile calculates Nth percentile of a list of values
func perc(values []uint64, pct int) float64 {
	p := float64(pct) / float64(100)
	n := float64(len(values))
	i := math.Ceil(p*n) - 1

	return float64(values[int(i)])
}

// sendGraphite sends metrics to graphite
func sendGraphite(buf *bytes.Buffer) {
	conn, err := net.Dial("tcp", *graphite)

	if err != nil {
		log.Fatal("ERROR: Unable to connect to graphite")
	}

	w := bufio.NewWriter(conn)
	n, err := buf.WriteTo(w)

	if err != nil {
		log.Fatal("ERROR: Unable to write to graphite")
	}

	w.Flush()
	conn.Close()

	log.Printf("Wrote %d bytes to Graphite", n)
}

//-----------------------------------------------------------------------------

func main() {
	flag.Parse()

	// Profiling
	cfg := profile.Config{
		CPUProfile:   *cpuprofile,
		MemProfile:   *memprofile,
		BlockProfile: *blockprofile,
		ProfilePath:  ".",
	}

	p := profile.Start(&cfg)
	defer p.Stop()

	// Process metrics as they arrive
	go processMetrics()

	// Setup listeners
	go log.Fatal(ListenTCP(*listen))
}
