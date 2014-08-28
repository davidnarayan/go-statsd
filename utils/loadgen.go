// Load generator for statspipe
package main

import (
	"bufio"
	"crypto/sha1"
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
	s := sha1.Sum([]byte(t0.String()))
	clientId := fmt.Sprintf("%x", s[0:3])

	conn, err := net.Dial("tcp", addr)

	if err != nil {
		log.Fatal(err)
	}

	w := bufio.NewWriter(conn)
	n := maxMetrics / 3
	n2 := n + maxMetrics%3

	var wg sync.WaitGroup
	wg.Add(3)

	// Send metrics
	go sendCounters(clientId, n2, w, &wg)
	go sendGauges(clientId, n, w, &wg)
	go sendTimers(clientId, n, w, &wg)

	wg.Wait()
	w.Flush()
	conn.Close()

	log.Printf("Client %s finished sending %d metrics to %s in %s",
		clientId, maxMetrics, addr, time.Now().Sub(t0))
}

func sendCounters(id string, n int, w *bufio.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "mycounter.%s.count:1|c\n", id)
	}
}

func sendGauges(id string, n int, w *bufio.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	rand.Seed(time.Now().UnixNano())
	min := 100
	max := 1000

	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "mygauge%d.%s.latest:%d|g\n", i+1, id, rand.Intn(max-min)+max)
	}
}

func sendTimers(id string, n int, w *bufio.Writer, wg *sync.WaitGroup) {
	defer wg.Done()

	rand.Seed(time.Now().UnixNano())

	for i := 0; i < n; i++ {
		fmt.Fprintf(w, "mytimer.%s:%d|ms\n", id, rand.Intn(100))
	}
}

func main() {
	flag.Parse()
	host := flag.Arg(0)

	if host == "" {
		host = "localhost:8125"
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
