package main

import (
	"bytes"
	//"fmt"
	"reflect"
	"regexp"
	//"sync"
	"testing"
)

type metricTest struct {
	input    string
	expected *Metric
}

var metricTests = []metricTest{
	{"mycounter:1|c", &Metric{Bucket: "mycounter", Value: int64(1), Type: Counter}},
	{"mycounter:1|c\n", &Metric{Bucket: "mycounter", Value: int64(1), Type: Counter}},
	{"  mycounter:1|c ", &Metric{Bucket: "mycounter", Value: int64(1), Type: Counter}},

	{"mygauge:78|g", &Metric{Bucket: "mygauge", Value: float64(78), Type: Gauge}},
	{"mygauge:8.9|g", &Metric{Bucket: "mygauge", Value: float64(8.9), Type: Gauge}},

	{"mytimer:123|ms", &Metric{Bucket: "mytimer", Value: float64(123), Type: Timer}},
	{"mytimer:0.789|ms", &Metric{Bucket: "mytimer", Value: float64(0.789), Type: Timer}},
}

// TestParseMetric tests all of the parsing
func TestParseMetric(t *testing.T) {

	for _, tt := range metricTests {
		want := tt.expected
		got, err := parseMetric([]byte(tt.input))

		if err != nil {
			t.Fatal(err)
		}

		if got.Bucket != want.Bucket {
			t.Errorf("parseMetric(%q): got: %s, want %s",
				tt.input, got.Bucket, want.Bucket)
		}

		if got.Value != want.Value {
			t.Errorf("parseMetric(%q): got: %v (%s), want %v (%s)",
				tt.input, got.Value, reflect.TypeOf(got.Value), want.Value,
				reflect.TypeOf(want.Value))
		}

		if got.Type != want.Type {
			t.Errorf("parseMetric(%q): got: %q, want %q",
				tt.input, got.Type, want.Type)
		}
	}
}

func TestHandleMessage(t *testing.T) {
	done := make(chan bool)
	testTable := make(chan metricTest, len(metricTests))

	go func() {
		for {
			select {
			case got := <-In:
				tt := <-testTable
				want := tt.expected

				if got.Bucket != want.Bucket {
					t.Errorf("handleMessage(%q): got: %q, want %q",
						tt.input, got.Bucket, want.Bucket)
				}

				if got.Value != want.Value {
					t.Errorf("handleMessage(%q): got: %v (%s), want %v (%s)",
						tt.input, got.Value, reflect.TypeOf(got.Value), want.Value,
						reflect.TypeOf(want.Value))
				}

				if got.Type != want.Type {
					t.Errorf("handleMessage(%q): got: %q, want %q",
						tt.input, got.Type, want.Type)
				}
			case <-done:
				break
			}
		}
	}()

	for _, tt := range metricTests {
		testTable <- tt
		handleMessage([]byte(tt.input))
	}

	done <- true
}

// TODO: doesn't always work...
/*
func TestHandleMessageMultiple(t *testing.T) {
	//done := make(chan bool)
	num := 0
	input := []byte("foo:1|c\nbar:1|c\nbaz:1|c")
	metrics := bytes.Split(input, []byte("\n"))
	var wg sync.WaitGroup
	wg.Add(len(metrics))

	go handleMessage(input)

	go func() {
		for got := range In {
			fmt.Println(num)
			fmt.Println(got)
			num++
			wg.Done()
		}
	}()

	wg.Wait()

	if num != len(metrics) {
		t.Errorf("Error parsing multiple metrics: got %d, want %d", num,
			len(metrics))
	}
}
*/

//-----------------------------------------------------------------------------
// Benchmarks

func getBuf(size int) []byte {
	var buf bytes.Buffer

	for {
		if buf.Len() >= size {
			break
		}

		buf.Write([]byte("a:1|c\n"))
	}

	return buf.Bytes()
}

func benchmarkHandleMessage(size int, b *testing.B) {
	done := make(chan bool)
	//num := 0

	go func() {
		for {
			select {
			case <-In:
				//num++
			case <-done:
				break
			}
		}
	}()

	buf := getBuf(size)

	b.ResetTimer()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		handleMessage(buf)
	}

	b.StopTimer()

	done <- true
	//b.Logf("Handled %d metrics", num)
}

func BenchmarkHandleMessage64(b *testing.B)   { benchmarkHandleMessage(64, b) }
func BenchmarkHandleMessage128(b *testing.B)  { benchmarkHandleMessage(128, b) }
func BenchmarkHandleMessage256(b *testing.B)  { benchmarkHandleMessage(256, b) }
func BenchmarkHandleMessage512(b *testing.B)  { benchmarkHandleMessage(512, b) }
func BenchmarkHandleMessage1024(b *testing.B) { benchmarkHandleMessage(1024, b) }
func BenchmarkHandleMessage2048(b *testing.B) { benchmarkHandleMessage(2048, b) }
func BenchmarkHandleMessage4096(b *testing.B) { benchmarkHandleMessage(4096, b) }
func BenchmarkHandleMessage8192(b *testing.B) { benchmarkHandleMessage(8192, b) }

// Benchmark metric parsing using different types
func benchmarkParseMetric(s string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		parseMetric([]byte(s))
	}
}

func BenchmarkParseMetricCounter(b *testing.B) { benchmarkParseMetric("mycounter:1|c", b) }
func BenchmarkParseMetricGauge(b *testing.B)   { benchmarkParseMetric("mygauge:78|g", b) }
func BenchmarkParseMetricTimer(b *testing.B)   { benchmarkParseMetric("mytimer:123|ms", b) }

// Benchmark metric extraction using regular expressions
var statsPattern = regexp.MustCompile(`[\w\.]+:-?\d+\|(?:c|ms|g)(?:\|\@[\d\.]+)?`)

func benchmarkRegexFindAll(size int, b *testing.B) {
	buf := getBuf(size)

	b.ResetTimer()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		statsPattern.FindAll(buf, -1)
	}

	b.StopTimer()
}

func BenchmarkRegexFindAll512(b *testing.B)  { benchmarkRegexFindAll(512, b) }
func BenchmarkRegexFindAll1024(b *testing.B) { benchmarkRegexFindAll(1024, b) }
func BenchmarkRegexFindAll2048(b *testing.B) { benchmarkRegexFindAll(2048, b) }
func BenchmarkRegexFindAll4096(b *testing.B) { benchmarkRegexFindAll(4096, b) }
func BenchmarkRegexFindAll8192(b *testing.B) { benchmarkRegexFindAll(8192, b) }

// Benchmark metric extraction using bytes.Split()
func benchmarkBytesSplit(size int, b *testing.B) {
	buf := getBuf(size)

	b.ResetTimer()
	b.StartTimer()

	for n := 0; n < b.N; n++ {
		bytes.Split(buf, []byte("\n"))
	}

	b.StopTimer()
}

func BenchmarkBytesSplit512(b *testing.B)  { benchmarkBytesSplit(512, b) }
func BenchmarkBytesSplit1024(b *testing.B) { benchmarkBytesSplit(1024, b) }
func BenchmarkBytesSplit2048(b *testing.B) { benchmarkBytesSplit(2048, b) }
func BenchmarkBytesSplit4096(b *testing.B) { benchmarkBytesSplit(4096, b) }
func BenchmarkBytesSplit8192(b *testing.B) { benchmarkBytesSplit(8192, b) }
