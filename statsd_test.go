package main

import (
	//"fmt"
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
