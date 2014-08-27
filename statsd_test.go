package main

import (
	//"fmt"
	"bytes"
	"reflect"
	"testing"
)

var metricTests = []struct {
	input    string
	expected *Metric
}{
	// Counters
	{"mycounter:1|c", &Metric{Bucket: "mycounter", Value: int64(1), Type: "c"}},
	//{"mycounter:1.1|c", &Metric{Bucket: "mycounter", Value: float64(1.1), Type: "c"}},

	// Gauges
	{"mygauge:78|g", &Metric{Bucket: "mygauge", Value: uint64(78), Type: "g"}},
	//{"mygauge:15.6|g", &Metric{Bucket: "mygauge", Value: uint64(78), Type: "g"}},

	// Timers
	{"mytimer:123|ms", &Metric{Bucket: "mytimer", Value: uint64(123), Type: "ms"}},
}

// TestHandleMetric tests all of the parsing
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

//-----------------------------------------------------------------------------
// Benchmarks

func benchmarkRegexFindAll(size int, b *testing.B) {
	var buf bytes.Buffer

	for {
		if buf.Len() >= size {
			break
		}

		buf.Write([]byte("mycounter:1|c "))
	}

	for n := 0; n < b.N; n++ {
		statsPattern.FindAll(buf.Bytes(), -1)
	}
}

func BenchmarkRegexFindAll512(b *testing.B) {
	benchmarkRegexFindAll(512, b)
}

func BenchmarkRegexFindAll1024(b *testing.B) {
	benchmarkRegexFindAll(1024, b)
}

func BenchmarkRegexFindAll2048(b *testing.B) {
	benchmarkRegexFindAll(2048, b)
}

func BenchmarkRegexFindAll4096(b *testing.B) {
	benchmarkRegexFindAll(4096, b)
}

func BenchmarkRegexFindAll8192(b *testing.B) {
	benchmarkRegexFindAll(8192, b)
}

func benchmarkParseMetric(s string, b *testing.B) {
	for n := 0; n < b.N; n++ {
		parseMetric([]byte(s))
	}
}

func BenchmarkParseMetricCounter(b *testing.B) {
	benchmarkParseMetric("mycounter:1|c", b)
}

func BenchmarkParseMetricGauge(b *testing.B) {
	benchmarkParseMetric("mygauge:78|g", b)
}

func BenchmarkParseMetricTimer(b *testing.B) {
	benchmarkParseMetric("mytimer:123|ms", b)
}
