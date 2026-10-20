package streamaggr

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/promrelabel"
)

func TestAggregatorsFailure(t *testing.T) {
	f := func(config string) {
		t.Helper()
		pushFunc := func(_ []prompb.TimeSeries) {
			panic(fmt.Errorf("pushFunc shouldn't be called"))
		}
		a, err := LoadFromData([]byte(config), pushFunc, nil, "some_alias")
		if err == nil {
			t.Fatalf("expecting non-nil error")
		}
		if a != nil {
			t.Fatalf("expecting nil a")
		}
	}

	// Invalid config
	f(`foobar`)

	// Unknown option
	f(`
- interval: 1m
  outputs: [total]
  foobar: baz
`)

	// missing interval
	f(`
- outputs: [total]
`)

	// missing outputs
	f(`
- interval: 1m
`)

	// Bad interval
	f(`
- interval: 1foo
  outputs: [total]
`)

	// Invalid output
	f(`
- interval: 1m
  outputs: [foobar]
`)

	// Negative interval
	f(`
- outputs: [total]
  interval: -5m
`)
	// Too small interval
	f(`
- outputs: [total]
  interval: 10ms
`)

	// bad dedup_interval
	f(`
- interval: 1m
  dedup_interval: 1foo
  outputs: ["quantiles"]
`)

	// interval isn't multiple of dedup_interval
	f(`
- interval: 1m
  dedup_interval: 35s
  outputs: ["quantiles"]
`)

	// dedup_interval is bigger than dedup_interval
	f(`
- interval: 1m
  dedup_interval: 1h
  outputs: ["quantiles"]
`)

	// bad staleness_interval
	f(`
- interval: 1m
  staleness_interval: 1foo
  outputs: ["quantiles"]
`)

	// staleness_interval should be > interval
	f(`
- interval: 1m
  staleness_interval: 30s
  outputs: ["quantiles"]
`)

	// staleness_interval should be multiple of interval
	f(`
- interval: 1m
  staleness_interval: 100s
  outputs: ["quantiles"]
`)

	// keep_metric_names is set for multiple inputs
	f(`
- interval: 1m
  keep_metric_names: true
  outputs: ["total", "increase"]
`)

	// keep_metric_names is set for unsupported input
	f(`
- interval: 1m
  keep_metric_names: true
  outputs: ["histogram_bucket"]
`)

	// Invalid input_relabel_configs
	f(`
- interval: 1m
  outputs: [total]
  input_relabel_configs:
  - foo: bar
`)
	f(`
- interval: 1m
  outputs: [total]
  input_relabel_configs:
  - action: replace
`)

	// Invalid output_relabel_configs
	f(`
- interval: 1m
  outputs: [total]
  output_relabel_configs:
  - foo: bar
`)
	f(`
- interval: 1m
  outputs: [total]
  output_relabel_configs:
  - action: replace
`)

	// Both by and without are non-empty
	f(`
- interval: 1m
  outputs: [total]
  by: [foo]
  without: [bar]
`)

	// Invalid quantiles()
	f(`
- interval: 1m
  outputs: ["quantiles("]
`)
	f(`
- interval: 1m
  outputs: ["quantiles()"]
`)
	f(`
- interval: 1m
  outputs: ["quantiles(foo)"]
`)
	f(`
- interval: 1m
  outputs: ["quantiles(-0.5)"]
`)
	f(`
- interval: 1m
  outputs: ["quantiles(1.5)"]
`)
	f(`
- interval: 1m
  outputs: [total, total]
`)
	// "quantiles(0.5)", "quantiles(0.9)" should be set as "quantiles(0.5, 0.9)"
	f(`
- interval: 1m
  outputs: ["quantiles(0.5)", "quantiles(0.9)"]
`)
}

func TestAggregatorsEqual(t *testing.T) {
	f := func(a, b string, expectedResult bool) {
		t.Helper()

		pushFunc := func(_ []prompb.TimeSeries) {}
		opts := Options{
			EnableWindows: true,
		}
		aa, err := LoadFromData([]byte(a), pushFunc, &opts, "some_alias")
		if err != nil {
			t.Fatalf("cannot initialize aggregators: %s", err)
		}
		ab, err := LoadFromData([]byte(b), pushFunc, &opts, "some_alias")
		if err != nil {
			t.Fatalf("cannot initialize aggregators: %s", err)
		}
		defer aa.MustStop()
		defer ab.MustStop()
		result := aa.Equal(ab)
		if result != expectedResult {
			t.Fatalf("unexpected result; got %v; want %v", result, expectedResult)
		}
	}
	f("", "", true)
	f(`
- outputs: [total]
  interval: 5m
`, ``, false)
	f(`
- outputs: [total]
  interval: 5m
`, `
- outputs: [total]
  interval: 5m
`, true)
	f(`
- outputs: [total]
  interval: 3m
`, `
- outputs: [total]
  interval: 5m
`, false)
	f(`
- outputs: [total]
  interval: 5m
  flush_on_shutdown: true  
`, `
- outputs: [total]
  interval: 5m
  flush_on_shutdown: false
`, false)
	f(`
- outputs: [total]
  interval: 5m
  ignore_first_intervals: 2
`, `
- outputs: [total]
  interval: 5m
  ignore_first_intervals: 4`, false)
}

func timeSeriessToString(tss []prompb.TimeSeries) string {
	sorted := make([]prompb.TimeSeries, len(tss))
	copy(sorted, tss)
	sort.SliceStable(sorted, func(i, j int) bool {
		return promrelabel.LabelsToString(sorted[i].Labels) < promrelabel.LabelsToString(sorted[j].Labels)
	})
	a := make([]string, len(sorted))
	for i, ts := range sorted {
		a[i] = timeSeriesToString(ts)
	}
	return strings.Join(a, "")
}

func timeSeriesToString(ts prompb.TimeSeries) string {
	labelsString := promrelabel.LabelsToString(ts.Labels)
	if len(ts.Samples) != 1 {
		panic(fmt.Errorf("unexpected number of samples for %s: %d; want 1", labelsString, len(ts.Samples)))
	}
	return fmt.Sprintf("%s %v\n", labelsString, ts.Samples[0].Value)
}

func appendClonedTimeseries(dst, src []prompb.TimeSeries) []prompb.TimeSeries {
	for _, ts := range src {
		dst = append(dst, prompb.TimeSeries{
			Labels:  append(ts.Labels[:0:0], ts.Labels...),
			Samples: append(ts.Samples[:0:0], ts.Samples...),
		})
	}
	return dst
}

func TestAggregatorsOutputUnchangedByLabelsCompressorCleanup(t *testing.T) {
	const config = `
- match: '{__name__=~".+_total"}'
  without: [node]
  interval: 30s
  outputs: [total_prometheus, increase, rate_sum, count_series]
- match: '{__name__=~".+_seconds"}'
  without: [node]
  interval: 30s
  outputs: [avg, max, last]
`
	run := func(cleanupEvery int) (string, int) {
		var output string
		freed := 0
		synctest.Test(t, func(t *testing.T) {
			var tssOutput []prompb.TimeSeries
			var mu sync.Mutex
			a, err := LoadFromData([]byte(config), func(tss []prompb.TimeSeries) {
				mu.Lock()
				tssOutput = appendClonedTimeseries(tssOutput, tss)
				mu.Unlock()
			}, nil, "cleanup")
			if err != nil {
				t.Fatalf("cannot initialize aggregators: %s", err)
			}
			// Keep pushes off the flush instants, so their order is deterministic.
			time.Sleep(time.Second)
			for step := 0; step < 120; step++ {
				var tss []prompb.TimeSeries
				ts := time.Now().UnixMilli()
				for i := 0; i < 5; i++ {
					node := prompb.Label{Name: "node", Value: fmt.Sprintf("n%d", i%2)}
					tss = append(tss,
						prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: "requests_total"}, {Name: "svc", Value: fmt.Sprintf("stable-%d", i)}, node},
							Samples: []prompb.Sample{{Value: float64(step * (i + 1)), Timestamp: ts}}},
						prompb.TimeSeries{Labels: []prompb.Label{{Name: "__name__", Value: "latency_seconds"}, {Name: "svc", Value: fmt.Sprintf("stable-%d", i)}, node},
							Samples: []prompb.Sample{{Value: float64(step%7) / 10, Timestamp: ts}}},
					)
				}
				// A churning resource lives for four steps; one comes back after an hour of absence.
				for _, id := range []int{step / 4, (step / 4) % 3} {
					tss = append(tss, prompb.TimeSeries{
						Labels:  []prompb.Label{{Name: "__name__", Value: "requests_total"}, {Name: "resource_id", Value: fmt.Sprintf("r-%d", id)}, {Name: "node", Value: "n0"}},
						Samples: []prompb.Sample{{Value: float64(step), Timestamp: ts}},
					})
				}
				a.Push(tss, nil)
				if cleanupEvery > 0 && step%cleanupEvery == 0 {
					freed += CleanupLabelsCompressor()
				}
				time.Sleep(10 * time.Second)
			}
			a.MustStop()
			output = timeSeriessToString(tssOutput)
		})
		return output, freed
	}

	want, _ := run(0)
	if again, _ := run(0); again != want {
		t.Fatalf("two runs without Cleanup differ; the comparison is not deterministic:\n%s", diffLines(again, want))
	}
	// Cleanup on every push, far more often than staleness_interval: it must still free only
	// labels no aggregator state can reference.
	got, freed := run(1)
	if freed == 0 {
		t.Fatalf("Cleanup freed no labels; the test did not exercise eviction")
	}
	if got != want {
		t.Fatalf("aggregation output changed by Cleanup:\n%s", diffLines(got, want))
	}
}

func diffLines(got, want string) string {
	gotLines := map[string]bool{}
	for _, l := range strings.Split(got, "\n") {
		gotLines[l] = true
	}
	wantLines := map[string]bool{}
	for _, l := range strings.Split(want, "\n") {
		wantLines[l] = true
	}
	var b strings.Builder
	for _, l := range strings.Split(want, "\n") {
		if !gotLines[l] {
			fmt.Fprintf(&b, "- %s\n", l)
		}
	}
	for _, l := range strings.Split(got, "\n") {
		if !wantLines[l] {
			fmt.Fprintf(&b, "+ %s\n", l)
		}
	}
	return b.String()
}

func TestLabelsCompressorCleanupWaitsForAggregatorFlush(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		// An aggregator that has not flushed yet still holds every key it compressed, however
		// long ago, so no label may be freed until it does.
		a, err := LoadFromData([]byte(`
- match: '{__name__="requests_total"}'
  without: [node]
  interval: 1h
  outputs: [total]
`), func([]prompb.TimeSeries) {}, nil, "slow")
		if err != nil {
			t.Fatalf("cannot initialize aggregators: %s", err)
		}
		time.Sleep(time.Second)
		for i := 0; i < 50; i++ {
			a.Push([]prompb.TimeSeries{{
				Labels:  []prompb.Label{{Name: "__name__", Value: "requests_total"}, {Name: "id", Value: fmt.Sprintf("slow-%d", i)}},
				Samples: []prompb.Sample{{Value: 1, Timestamp: time.Now().UnixMilli()}},
			}}, nil)
			time.Sleep(time.Minute)
			if freed := CleanupLabelsCompressor(); freed != 0 {
				t.Fatalf("Cleanup freed %d labels while an aggregator holding them had not flushed", freed)
			}
		}
		a.MustStop()
	})
}

func TestLabelsCompressorCleanupWaitsForPushInFlight(t *testing.T) {
	// A Push that compressed its keys but has not stored them yet: a flush running meanwhile
	// does not see them, so no flush may report them gone until the Push completes.
	waitsForPush := func(t *testing.T, pushes *pushesInFlight, keysGoneBefore func() time.Time) {
		t.Helper()
		started := time.Now()
		id := pushes.start(started.UnixMilli())
		time.Sleep(10 * time.Minute)
		if got := keysGoneBefore(); got.After(started) {
			t.Fatalf("keys reported gone before %s while a Push started at %s is in flight", got, started)
		}
		pushes.done(id)
		time.Sleep(time.Minute)
		if got := keysGoneBefore(); !got.After(started) {
			t.Fatalf("keys still reported gone only before %s after the Push completed", got)
		}
	}
	newAggregator := func(t *testing.T, dedupInterval string) *aggregator {
		a, err := LoadFromData([]byte(`
- match: '{__name__="requests_total"}'
  interval: 10s
  dedup_interval: `+dedupInterval+`
  outputs: [total]
`), func([]prompb.TimeSeries) {}, nil, "in_flight")
		if err != nil {
			t.Fatalf("cannot initialize aggregators: %s", err)
		}
		t.Cleanup(a.MustStop)
		return a.as[0]
	}
	t.Run("aggregator", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a := newAggregator(t, "0s")
			time.Sleep(time.Second)
			waitsForPush(t, &a.pushes, a.keysGoneBefore)
		})
	})
	t.Run("aggregator dedup", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			a := newAggregator(t, "5s")
			time.Sleep(time.Second)
			waitsForPush(t, &a.pushes, func() time.Time { return time.UnixMilli(a.prevDedupStartMsec.Load()) })
		})
	})
	t.Run("deduplicator", func(t *testing.T) {
		synctest.Test(t, func(t *testing.T) {
			d := NewDeduplicator(func([]prompb.TimeSeries) {}, false, 10*time.Second, nil, "in_flight")
			defer d.MustStop()
			time.Sleep(time.Second)
			waitsForPush(t, &d.pushes, d.keysGoneBefore)
		})
	})
}
