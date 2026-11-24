package streamaggr

import (
	"fmt"
	"slices"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
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
		defer aa.MustStop()
		ab, err := LoadFromData([]byte(b), pushFunc, &opts, "some_alias")
		if err != nil {
			t.Fatalf("cannot initialize aggregators: %s", err)
		}
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

func TestFlushLag(t *testing.T) {
	type push struct {
		hasAccepted     bool
		acceptedLagMsec int64
		droppedLagMsec  int64
	}
	const interval = 10 * time.Second
	f := func(pushes []push, delayExpected time.Duration) {
		t.Helper()
		var fl flushLag
		for _, p := range pushes {
			fl.update(p.hasAccepted, p.acceptedLagMsec, p.droppedLagMsec)
		}
		if delay := fl.takeFlushDelay(interval); delay != delayExpected {
			t.Fatalf("unexpected delay; got %s; want %s", delay, delayExpected)
		}
		// The lags must be reset after takeFlushDelay.
		if delay := fl.takeFlushDelay(interval); delay != 0 {
			t.Fatalf("unexpected delay after reset; got %s; want 0s", delay)
		}
	}

	// no pushes
	f(nil, 0)

	// nothing accepted or dropped
	f([]push{{}}, 0)

	// the max accepted lag is used
	f([]push{{true, 1000, 0}, {true, 3000, 0}, {true, 2000, 0}}, 3*time.Second)

	// accepted lag wins over dropped lag
	f([]push{{true, 1000, 0}, {false, 0, 5000}}, time.Second)
	f([]push{{false, 0, 5000}, {true, 0, 0}}, 0)

	// dropped lag is used if no samples were accepted
	f([]push{{false, 0, 4000}, {false, 0, 6000}}, 6*time.Second)

	// the delay is capped at 90% of the interval
	f([]push{{true, 15000, 0}}, 9*time.Second)
	f([]push{{false, 0, 15000}}, 9*time.Second)
}

// checkNoLabelsUsers checks that no aggregator or deduplicator of a previous test is still running.
func checkNoLabelsUsers(t *testing.T) {
	t.Helper()
	n := 0
	labelsUsers.Range(func(_, _ any) bool {
		n++
		return true
	})
	if n > 0 {
		t.Fatalf("%d aggregators or deduplicators of previous tests are still running", n)
	}
}

// checkKeys decompresses every key that a holds. Decompress panics if a label of a key was freed.
func checkKeys(a *aggregator) {
	// Collect the keys first and decompress them without holding locks, so a panic doesn't hang the test.
	var keys, labelsKeys []string
	if a.da != nil {
		for i := range a.da.shards {
			shard := &a.da.shards[i]
			for _, state := range []*dedupAggrState{&shard.blue, &shard.green} {
				state.mu.Lock()
				for key := range state.m {
					keys = append(keys, strings.Clone(key))
				}
				state.mu.Unlock()
			}
		}
	}
	a.aggrOutputs.m.Range(func(k, v any) bool {
		labelsKeys = append(labelsKeys, strings.Clone(k.(string)))
		av := v.(*aggrValues)
		av.mu.Lock()
		for _, o := range av.blue {
			if tv, ok := o.(*totalAggrValue); ok {
				for key := range tv.shared.lastValues {
					if a.aggrOutputs.useInputKey {
						labelsKeys = append(labelsKeys, strings.Clone(key))
					} else {
						keys = append(keys, strings.Clone(key))
					}
				}
			}
		}
		av.mu.Unlock()
		return true
	})
	for _, key := range keys {
		src := bytesutil.ToUnsafeBytes(key)
		outputKeyLen, nSize := encoding.UnmarshalVarUint64(src)
		src = src[nSize:]
		lc.Decompress(nil, src[:outputKeyLen])
		lc.Decompress(nil, src[outputKeyLen:])
	}
	for _, key := range labelsKeys {
		decompressLabels(nil, key)
	}
}

func TestLabelsCompressorCleanupWaitsForPushInFlight(t *testing.T) {
	checkNoLabelsUsers(t)
	labels := []prompb.Label{{Name: "job", Value: "inflight-job"}, {Name: "pod", Value: "inflight-pod"}}

	// A Push that compressed a key but didn't store it yet, for example because it waits for a slow flush.
	inFlight := startPush()
	key := lc.Compress(nil, labels)
	for range 3 {
		cleanupLabels()
	}
	// New labels take the freed idxs, so a freed label would decode to another one.
	for i := range 100 {
		lc.Compress(nil, []prompb.Label{{Name: "other", Value: fmt.Sprintf("inflight-%d", i)}})
	}
	if got := lc.Decompress(nil, key); !slices.Equal(got, labels) {
		t.Fatalf("labels of a key compressed by a Push in flight changed; got %s; want %s", got, labels)
	}
	inFlight.done()

	// Once the Push is done and no state holds its key, its labels are freed.
	freed := 0
	for range 3 {
		freed += cleanupLabels()
	}
	if freed < len(labels) {
		t.Fatalf("Cleanup freed %d labels after the Push was done; want at least %d", freed, len(labels))
	}
}

func TestPushIsCountedUntilItStoresKeys(t *testing.T) {
	checkNoLabelsUsers(t)
	inFlight := func() int64 {
		return pushesInFlight[0].Load() + pushesInFlight[1].Load()
	}
	series := []prompb.TimeSeries{{
		Labels:  []prompb.Label{{Name: "__name__", Value: "counted_total"}, {Name: "job", Value: "j"}},
		Samples: []prompb.Sample{{Value: 1, Timestamp: time.Now().UnixMilli()}},
	}}
	// waitBlocked runs push while the state it stores into is locked, and checks that it is counted until it is done.
	waitBlocked := func(push func(), unlock func()) {
		t.Helper()
		done := make(chan struct{})
		go func() {
			push()
			close(done)
		}()
		// Wait until the Push is counted. It can't finish while the state is locked.
		deadline := time.Now().Add(10 * time.Second)
		for inFlight() != 1 && time.Now().Before(deadline) {
			time.Sleep(time.Millisecond)
		}
		if n := inFlight(); n != 1 {
			t.Errorf("got %d Push calls in flight while one waits to store its keys; want 1", n)
		}
		unlock()
		<-done
		if n := inFlight(); n != 0 {
			t.Fatalf("got %d Push calls in flight after the Push was done; want 0", n)
		}
	}

	a, err := LoadFromData([]byte(`
- match: '{__name__="counted_total"}'
  interval: 1h
  outputs: [total]
`), func([]prompb.TimeSeries) {}, nil, "counted")
	if err != nil {
		t.Fatalf("cannot initialize aggregators: %s", err)
	}
	a.Push(series, nil)
	var av *aggrValues
	a.as[0].aggrOutputs.m.Range(func(_, v any) bool {
		av = v.(*aggrValues)
		return false
	})
	av.mu.Lock()
	waitBlocked(func() { a.Push(series, nil) }, av.mu.Unlock)
	a.MustStop()

	d := NewDeduplicator(func([]prompb.TimeSeries) {}, false, time.Hour, nil, "counted")
	var states []*dedupAggrState
	for i := range d.da.shards {
		states = append(states, &d.da.shards[i].blue, &d.da.shards[i].green)
	}
	for _, s := range states {
		s.mu.Lock()
	}
	waitBlocked(func() { d.Push(series) }, func() {
		for _, s := range states {
			s.mu.Unlock()
		}
	})
	d.MustStop()
}
