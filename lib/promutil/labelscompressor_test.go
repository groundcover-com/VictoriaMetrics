package promutil

import (
	"bytes"
	"fmt"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func TestLabelsCompressorSerial(t *testing.T) {
	var lc LabelsCompressor

	f := func(labels []prompb.Label) {
		t.Helper()

		sExpected := labelsToString(labels)

		data := lc.Compress(nil, labels)
		labelsResult := lc.Decompress(nil, data)

		sResult := labelsToString(labelsResult)
		if sExpected != sResult {
			t.Fatalf("unexpected result; got %s; want %s", sResult, sExpected)
		}

		if len(labels) > 0 {
			if n := lc.SizeBytes(); n == 0 {
				t.Fatalf("Unexpected zero SizeBytes()")
			}
			if n := lc.ItemsCount(); n == 0 {
				t.Fatalf("Unexpected zero ItemsCount()")
			}
		}
	}

	// empty labels
	f(nil)
	f([]prompb.Label{})

	// non-empty labels
	f([]prompb.Label{
		{
			Name:  "instance",
			Value: "12345.4342.342.3",
		},
		{
			Name:  "job",
			Value: "kube-pod-12323",
		},
	})
	f([]prompb.Label{
		{
			Name:  "instance",
			Value: "12345.4342.342.3",
		},
		{
			Name:  "job",
			Value: "kube-pod-12323",
		},
		{
			Name:  "pod",
			Value: "foo-bar-baz",
		},
	})
}

func TestLabelsCompressorConcurrent(t *testing.T) {
	const concurrency = 5
	var lc LabelsCompressor
	var expectCompressedKeys sync.Map

	var wg sync.WaitGroup
	for i := range concurrency {
		wg.Go(func() {
			series := newTestSeries(100, 20)
			for n, labels := range series {
				sExpected := labelsToString(labels)
				data := lc.Compress(nil, labels)
				if expectData, ok := expectCompressedKeys.LoadOrStore(n, data); ok {
					if string(data) != string(expectData.([]byte)) {
						panic(fmt.Errorf("unexpected compress result at series/%d in iteration %d ", n, i))
					}
				}
				labelsResult := lc.Decompress(nil, data)
				sResult := labelsToString(labelsResult)
				if sExpected != sResult {
					panic(fmt.Errorf("unexpected result on iteration %d; got %s; want %s", i, sResult, sExpected))
				}
			}
		})
	}
	wg.Wait()

	if n := lc.SizeBytes(); n == 0 {
		t.Fatalf("Unexpected zero SizeBytes()")
	}
	if n := lc.ItemsCount(); n == 0 {
		t.Fatalf("Unexpected zero ItemsCount()")
	}
}

func labelsToString(labels []prompb.Label) string {
	l := Labels{
		Labels: labels,
	}
	return l.String()
}

func newTestSeries(seriesCount, labelsPerSeries int) [][]prompb.Label {
	series := make([][]prompb.Label, seriesCount)
	for i := range seriesCount {
		labels := make([]prompb.Label, labelsPerSeries)
		for j := range labelsPerSeries {
			labels[j] = prompb.Label{
				Name:  fmt.Sprintf("label_%d", j),
				Value: fmt.Sprintf("value_%d_%d", i, j),
			}
		}
		series[i] = labels
	}
	return series
}

func TestLabelsCompressorCleanupFreesUnreferencedLabels(t *testing.T) {
	var lc LabelsCompressor
	for _, labels := range newTestSeries(1000, 10) {
		lc.Compress(nil, labels)
	}
	itemsBefore, sizeBefore := lc.ItemsCount(), lc.SizeBytes()
	if itemsBefore == 0 {
		t.Fatalf("expected items after compressing")
	}

	// Labels compressed during a period are kept by its Cleanup, moved to standby by the next one
	// and freed by the one after.
	for i := 1; i <= 2; i++ {
		if freed := lc.Cleanup(time.Now()); freed != 0 {
			t.Fatalf("Cleanup #%d freed %d labels; want 0", i, freed)
		}
	}
	if freed := lc.Cleanup(time.Now()); freed != int(itemsBefore) {
		t.Fatalf("third Cleanup freed %d labels; want %d", freed, itemsBefore)
	}
	if n := lc.ItemsCount(); n != 0 {
		t.Fatalf("ItemsCount after freeing everything = %d; want 0", n)
	}
	if lc.SizeBytes() >= sizeBefore {
		t.Fatalf("SizeBytes did not shrink: before=%d after=%d", sizeBefore, lc.SizeBytes())
	}

	// Freed idxs are reused for new labels, which must still round-trip.
	nextIdxBefore := lc.nextIdx.Load()
	for _, labels := range newTestSeries(100, 10) {
		labels = append(labels, prompb.Label{Name: "generation", Value: "2"})
		got := lc.Decompress(nil, lc.Compress(nil, labels))
		if labelsToString(got) != labelsToString(labels) {
			t.Fatalf("unexpected labels after reuse; got %s; want %s", labelsToString(got), labelsToString(labels))
		}
	}
	if lc.nextIdx.Load() != nextIdxBefore {
		t.Fatalf("new labels allocated fresh idxs (%d -> %d) instead of reusing freed ones", nextIdxBefore, lc.nextIdx.Load())
	}
}

func TestLabelsCompressorCleanupKeepsKeysOfReferencedLabels(t *testing.T) {
	var lc LabelsCompressor
	labels := []prompb.Label{{Name: "job", Value: "api"}, {Name: "instance", Value: "10.0.0.1:9100"}}
	key := lc.Compress(nil, labels)
	for i := 0; i < 10; i++ {
		lc.Cleanup(time.Now())
		if got := lc.Compress(nil, labels); !bytes.Equal(got, key) {
			t.Fatalf("key of a label used every period changed after %d Cleanups", i+1)
		}
	}
}

func TestLabelsCompressorCleanupRestoresIdxOfStandbyLabel(t *testing.T) {
	var lc LabelsCompressor
	labels := []prompb.Label{{Name: "pod", Value: "api-7d9f-x2"}}
	key := lc.Compress(nil, labels)

	// Unreferenced for one period: on standby, yet keys referencing it must keep their meaning.
	lc.Cleanup(time.Now())
	lc.Cleanup(time.Now())
	if got := lc.Compress(nil, labels); !bytes.Equal(got, key) {
		t.Fatalf("label compressed again after one unreferenced period got a different key")
	}
	if got := lc.Decompress(nil, key); labelsToString(got) != labelsToString(labels) {
		t.Fatalf("unexpected labels; got %s; want %s", labelsToString(got), labelsToString(labels))
	}
}

func TestLabelsCompressorCleanupConcurrent(t *testing.T) {
	var lc LabelsCompressor
	var generation atomic.Int64
	stop := make(chan struct{})
	var cleanups sync.WaitGroup
	cleanups.Go(func() {
		for {
			select {
			case <-stop:
				return
			case <-time.After(5 * time.Millisecond):
				lc.Cleanup(time.Now())
				generation.Add(1)
			}
		}
	})

	hot := newTestSeries(50, 8)
	var wg sync.WaitGroup
	for g := 0; g < 8; g++ {
		wg.Go(func() {
			prevKeys := make([][]byte, len(hot))
			prevGen := make([]int64, len(hot))
			deadline := time.Now().Add(2 * time.Second)
			for iter := 0; time.Now().Before(deadline); iter++ {
				i := iter % len(hot)
				gen := generation.Load()
				key := lc.Compress(nil, hot[i])
				if got := lc.Decompress(nil, key); labelsToString(got) != labelsToString(hot[i]) {
					t.Errorf("hot labels: got %s; want %s", labelsToString(got), labelsToString(hot[i]))
					return
				}
				// A key compressed within the same or the next Cleanup period must be identical.
				if prevKeys[i] != nil && gen-prevGen[i] <= 1 && !bytes.Equal(prevKeys[i], key) {
					t.Errorf("key of hot labels changed while in use (generations %d -> %d)", prevGen[i], gen)
					return
				}
				prevKeys[i], prevGen[i] = key, generation.Load()

				churn := []prompb.Label{{Name: "id", Value: fmt.Sprintf("g%d-%d", g, iter)}, {Name: "job", Value: "churn"}}
				if got := lc.Decompress(nil, lc.Compress(nil, churn)); labelsToString(got) != labelsToString(churn) {
					t.Errorf("churn labels: got %s; want %s", labelsToString(got), labelsToString(churn))
					return
				}
			}
		})
	}
	wg.Wait()
	close(stop)
	cleanups.Wait()

	if generation.Load() < 10 {
		t.Fatalf("only %d Cleanups ran; the test did not exercise eviction", generation.Load())
	}
	if n := lc.ItemsCount(); n > 100_000 {
		t.Fatalf("ItemsCount=%d: churned labels were not freed", n)
	}
}
