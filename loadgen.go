package main

import (
	"bufio"
	"flag"
	"fmt"
	"log"
	"math/rand"
	"net"
	"sync"
	"time"
)

var clients = flag.Int("c", 10, "Concurrent clients")
var metrics = flag.Int("n", 1000, "Metrics per client")

func client(addr string, maxMetrics int) {
	//log.Printf("Sending %d metrics to %s", maxMetrics, addr)
	t0 := time.Now()

	conn, err := net.Dial("tcp", addr)

	if err != nil {
		log.Fatal(err)
	}

	n := maxMetrics / 3
	w := bufio.NewWriter(conn)

	// counters
	for i := 0; i < (n + maxMetrics%3); i++ {
		w.WriteString("mycounter:1|c\n")
	}

	// gauges
	rand.Seed(time.Now().UnixNano())
	min := 100
	max := 1000

	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "mygauge:%d|g\n", rand.Intn(max-min)+max)
	}

	// timers
	rand.Seed(time.Now().UnixNano())

	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "mytimer:%d|ms\n", rand.Intn(100))
	}

	log.Printf("Finished sending %d metrics to %s in %s", maxMetrics, addr,
		time.Now().Sub(t0))

	w.Flush()
	conn.Close()
}

func main() {
	flag.Parse()
	host := flag.Arg(0)

	if host == "" {
		host = "localhost:1514"
	}

	var wg sync.WaitGroup

	c := *clients
	n := *metrics

	log.Printf("Sending %d metrics to %s", c*n, host)
	t0 := time.Now()

	for i := 0; i < c; i++ {
		wg.Add(1)

		go func(addr string, max int) {
			defer wg.Done()
			client(addr, max)
		}(host, n)
	}

	wg.Wait()
	log.Printf("Finished sending %d metrics to %s in %s", c*n, host,
		time.Now().Sub(t0))

}
