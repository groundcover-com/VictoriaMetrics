package streamaggr

import (
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func TestTotalPrometheusLongLabelBufferReuse(t *testing.T) {
	var output []prompb.TimeSeries
	a, err := LoadFromData([]byte(`
- interval: 24h
  by: [job]
  outputs: [total_prometheus]
  flush_on_shutdown: true
`), func(tss []prompb.TimeSeries) {
		output = appendClonedTimeseries(output, tss)
	}, nil, "long_label_buffer_reuse")
	if err != nil {
		t.Fatal(err)
	}
	buf := []byte(strings.Repeat("x", 600))
	ts := []prompb.TimeSeries{{
		Labels: []prompb.Label{
			{Name: "__name__", Value: "counter"},
			{Name: "job", Value: "fixed"},
			{Name: "long", Value: bytesutil.ToUnsafeString(buf)},
		},
		Samples: []prompb.Sample{{Timestamp: time.Now().UnixMilli()}},
	}}
	for _, sample := range []struct {
		id    byte
		value float64
	}{{'A', 10}, {'A', 15}, {'B', 100}, {'A', 20}, {'B', 110}} {
		buf[0] = sample.id
		ts[0].Samples[0].Value = sample.value
		if got := a.Push(ts, nil); len(got) != 1 || got[0] != 1 {
			t.Fatalf("input did not match: %v", got)
		}
	}
	clear(buf)
	a.MustStop()
	got := timeSeriessToString(output)
	const want = "counter:24h_by_job_total_prometheus{job=\"fixed\"} 20\n"
	if got != want {
		t.Fatalf("counter history changed after buffer reuse; got\n%s\nwant\n%s", got, want)
	}
}
