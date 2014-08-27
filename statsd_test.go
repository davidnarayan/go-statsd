package main

import (
	"bytes"
	//"fmt"
	"reflect"
	"testing"
)

type metricTest struct {
	input    string
	expected *Metric
}

var metricTests = []metricTest{
	// Counters
	{"mycounter:1|c", &Metric{Bucket: "mycounter", Value: int64(1), Type: "c"}},
	//{"mycounter:1.1|c", &Metric{Bucket: "mycounter", Value: float64(1.1), Type: "c"}},

	// Gauges
	{"mygauge:78|g", &Metric{Bucket: "mygauge", Value: uint64(78), Type: "g"}},
	//{"mygauge:15.6|g", &Metric{Bucket: "mygauge", Value: uint64(78), Type: "g"}},

	// Timers
	{"mytimer:123|ms", &Metric{Bucket: "mytimer", Value: uint64(123), Type: "ms"}},
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
			t.Errorf("parseMetric(%s): got: %s, want %s",
				tt.input, got.Bucket, want.Bucket)
		}

		if got.Value != want.Value {
			t.Errorf("parseMetric(%s): got: %d (%s), want %d (%s)",
				tt.input, got.Value, reflect.TypeOf(got.Value), want.Value,
				reflect.TypeOf(want.Value))
		}

		if got.Type != want.Type {
			t.Errorf("parseMetric(%s): got: %s, want %s",
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
					t.Errorf("handleMessage(%s): got: %s, want %s",
						tt.input, got.Bucket, want.Bucket)
				}

				if got.Value != want.Value {
					t.Errorf("handleMessage(%s): got: %d (%s), want %d (%s)",
						tt.input, got.Value, reflect.TypeOf(got.Value), want.Value,
						reflect.TypeOf(want.Value))
				}

				if got.Type != want.Type {
					t.Errorf("handleMessage(%s): got: %s, want %s",
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

//-----------------------------------------------------------------------------
// Benchmarks

func getBuf(size int) []byte {
	var buf bytes.Buffer

	for {
		if buf.Len() >= size {
			break
		}

		buf.Write([]byte("mycounter:1|c "))
	}

	return buf.Bytes()
}

func benchmarkHandleMessage(size int, b *testing.B) {
	done := make(chan bool)

	go func() {
		for {
			select {
			case <-In:
			case <-done:
				break
			}
		}
	}()

	buf := getBuf(size)

	for n := 0; n < b.N; n++ {
		handleMessage(buf)
	}

	done <- true
}

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
		bytes.Split(buf, []byte(" "))
	}

	b.StopTimer()
}

func BenchmarkBytesSplit512(b *testing.B)  { benchmarkBytesSplit(512, b) }
func BenchmarkBytesSplit1024(b *testing.B) { benchmarkBytesSplit(1024, b) }
func BenchmarkBytesSplit2048(b *testing.B) { benchmarkBytesSplit(2048, b) }
func BenchmarkBytesSplit4096(b *testing.B) { benchmarkBytesSplit(4096, b) }
func BenchmarkBytesSplit8192(b *testing.B) { benchmarkBytesSplit(8192, b) }
