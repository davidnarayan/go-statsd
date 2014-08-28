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
	//"regexp"
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
	listen   = flag.String("listen", ":8125", "Listener address")
	graphite = flag.String("graphite", "localhost:2003", "Graphite server address")

	// Profiling
	cpuprofile   = flag.Bool("cpuprofile", false, "Enable CPU profiling")
	memprofile   = flag.Bool("memprofile", false, "Enable memory profiling")
	blockprofile = flag.Bool("blockprofile", false, "Enable block profiling")

	debug = flag.Bool("debug", false, "Enable debug mode")
)

//-----------------------------------------------------------------------------
// Data structures

// Metric is a numeric data point
type Metric struct {
	Bucket string
	Value  interface{}
	Type   string
}

// Metrics should be in statsd format. Metric names may not have spaces.
//
//     <metric_name>:<metric_value>|<metric_type>|@<sample_rate>
//
// Note: The sample rate is optional
// var statsPattern = regexp.MustCompile(`[\w\.]+:-?\d+\|(?:c|ms|g)(?:\|\@[\d\.]+)?`)

// In is a channel for processing metrics
var In = make(chan *Metric)

// counters holds all of the counter metrics
var counters = struct {
	sync.RWMutex
	m map[string]int64
}{m: make(map[string]int64)}

// gauges holds all of the gauge metrics
var gauges = struct {
	sync.RWMutex
	m map[string]uint64
}{m: make(map[string]uint64)}

// Timers is a list of uints
type Timers []uint64

// timers holds all of the timer metrics
var timers = struct {
	sync.RWMutex
	m map[string]Timers
}{m: make(map[string]Timers)}

// Internal metrics
type Stats struct {
	IngressRate     int64
	IngressMetrics  int64
	IngressCounters int64
	IngressGauges   int64
	IngressTimers   int64
}

var stats = &Stats{}

// TODO: move this to command line option
var Percentiles = []int{5, 95}

//-----------------------------------------------------------------------------

// Implement the sort interface for Timers
func (t Timers) Len() int           { return len(t) }
func (t Timers) Swap(i, j int)      { t[i], t[j] = t[j], t[i] }
func (t Timers) Less(i, j int) bool { return t[i] < t[j] }

//-----------------------------------------------------------------------------

// ListenUDP creates a UDP listener
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

		if *debug {
			log.Printf("DEBUG: Received UDP message: bytes=%d client=%s",
				n, raddr)
		}

		go handleUdpMessage(buf)
	}
}

func handleUdpMessage(buf []byte) {
	tokens := bytes.Split(buf, []byte("\n"))

	for _, token := range tokens {
		handleMessage(token)
	}
}

// ListenTCP creates a TCP listener
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

// handleConnection handles a single client connection
func handleConnection(conn net.Conn) {
	defer conn.Close()
	r := bufio.NewReader(conn)

	// Incoming metrics should be separated by a newline
	for {
		line, err := r.ReadBytes('\n')

		if err != nil {
			if err == io.EOF {
				break
			} else {
				// TODO: handle error
			}
		}

		if *debug {
			log.Printf("DEBUG: Received TCP message: bytes=%d client=%s",
				len(line), conn.RemoteAddr())
		}

		handleMessage(line)
	}
}

// Handle an event message
func handleMessage(buf []byte) {
	// According to the statsd protocol, metrics should be separated by a
	// newline. This parser isn't quite as strict since it may be receiving
	// metrics from clients that aren't proper statsd clients (e.g. syslog).
	// Instead, find metrics by looking for tokens between spaces and then
	// use parseMetric to create an actual metric that can be aggregated.
	tokens := bytes.Split(buf, []byte(" "))

	for _, token := range tokens {
		// metrics must have a : and | at a minimum
		if bytes.Index(token, []byte(":")) == -1 ||
			bytes.Index(token, []byte("|")) == -1 {
			continue
		}

		metric, err := parseMetric(token)

		if err != nil {
			log.Printf("ERROR: Unable to parse metric %s: %s",
				token, err)
			continue
		}

		// Send metric off for processing
		In <- metric
	}
}

// parseMetric parses a raw metric into a Metric struct
func parseMetric(b []byte) (*Metric, error) {
	i := bytes.Index(b, []byte(":"))
	j := bytes.Index(b, []byte("|"))
	k := bytes.Index(b, []byte("@"))
	v := b[i+1 : j]

	// End position of the metric type is the end of the byte slice
	// if no sample was sent.
	tEnd := len(b)
	var sampleRate float64 = 1

	// Indicates that a sample rate was sent as part of the metric
	if k > -1 {
		tEnd = k - 1 // Use -1 because of the | before the @
		sr := b[(k + 1):len(b)]
		var err error
		sampleRate, err = strconv.ParseFloat(string(sr), 64)

		if err != nil {
			return nil, err
		}
	}

	m := &Metric{
		Bucket: string(b[0:i]),
		Type:   string(b[j+1 : tEnd]),
	}

	switch m.Type {
	case "c":
		val, err := strconv.ParseInt(string(v), 10, 64)

		if err != nil {
			return nil, err
		}

		m.Value = int64(float64(val) / sampleRate)
	default:
		val, err := strconv.ParseUint(string(v), 10, 64)

		if err != nil {
			return nil, err
		}

		m.Value = val

	default:
		err := fmt.Errorf("unable to create metric for type %q", m.Type)

		return nil, err
	}

	return m, nil
}

// processMetrics updates new metrics and flushes aggregates to Graphite
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
				counters.m[m.Bucket] += m.Value.(int64)
				counters.Unlock()
				atomic.AddInt64(&stats.IngressCounters, 1)

			case "g":
				gauges.Lock()
				gauges.m[m.Bucket] = m.Value.(uint64)
				gauges.Unlock()
				atomic.AddInt64(&stats.IngressGauges, 1)

			case "ms":
				timers.Lock()
				_, ok := timers.m[m.Bucket]

				if !ok {
					var t Timers
					timers.m[m.Bucket] = t
				}

				timers.m[m.Bucket] = append(timers.m[m.Bucket], m.Value.(uint64))
				timers.Unlock()
				atomic.AddInt64(&stats.IngressTimers, 1)

			default:
				if *debug {
					log.Printf("DEBUG: Unable to process unknown metric type %q", m.Type)
				}

			}

			if *debug {
				log.Printf("DEBUG: Finished processing metric: %+v", m)
			}
		}
	}
}

// flushMetrics sends metrics to Graphite
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

// flushInternalStats writes the internal stats to the buffer
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

// flushCounters writes the counters to the buffer
func flushCounters(buf *bytes.Buffer, now int64) {
	counters.Lock()
	defer counters.Unlock()

	for k, v := range counters.m {
		fmt.Fprintf(buf, "%s %d %d\n", k, v, now)
		delete(counters.m, k)
	}
}

// flushGauges writes the gauges to the buffer
func flushGauges(buf *bytes.Buffer, now int64) {
	gauges.Lock()
	defer gauges.Unlock()

	for k, v := range gauges.m {
		fmt.Fprintf(buf, "%s %d %d\n", k, v, now)
		delete(gauges.m, k)
	}
}

// flushTimers writes the timers and aggregate statistics to the buffer
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
	log.Printf("Sending metrics to Graphite: bytes=%d host=%s",
		buf.Len(), *graphite)
	t0 := time.Now()

	conn, err := net.Dial("tcp", *graphite)

	if err != nil {
		log.Printf("ERROR: Unable to connect to graphite: %s", err)
		return
	}

	w := bufio.NewWriter(conn)
	n, err := buf.WriteTo(w)

	if err != nil {
		log.Printf("ERROR: Unable to write to graphite: %s", err)
	}

	w.Flush()
	conn.Close()

	log.Printf("Finished sending metrics to Graphite: bytes=%d host=%s duration=%s",
		n, conn.RemoteAddr(), time.Now().Sub(t0))
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
	var wg sync.WaitGroup
	wg.Add(2)

	go func() {
		defer wg.Done()
		log.Fatal(ListenUDP(*listen))
	}()

	go func() {
		defer wg.Done()
		log.Fatal(ListenTCP(*listen))
	}()

	wg.Wait()
}
