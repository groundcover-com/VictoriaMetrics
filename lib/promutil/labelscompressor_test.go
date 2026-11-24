package promutil

import (
	"bytes"
	"fmt"
	"sync"
	"testing"

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

func noLiveKeys(func([]byte)) {}

func TestLabelsCompressorCleanupFreesUnusedLabels(t *testing.T) {
	var lc LabelsCompressor
	for _, labels := range newTestSeries(1000, 10) {
		lc.Compress(nil, labels)
	}
	itemsBefore, sizeBefore := lc.ItemsCount(), lc.SizeBytes()
	if itemsBefore == 0 {
		t.Fatalf("expected items after compressing")
	}

	// The first Cleanup sees the labels used since the previous one, so it keeps them.
	if freed := lc.Cleanup(noLiveKeys); freed != 0 {
		t.Fatalf("first Cleanup freed %d labels; want 0", freed)
	}
	if freed := lc.Cleanup(noLiveKeys); freed != int(itemsBefore) {
		t.Fatalf("second Cleanup freed %d labels; want %d", freed, itemsBefore)
	}
	if n := lc.ItemsCount(); n != 0 {
		t.Fatalf("ItemsCount after freeing everything = %d; want 0", n)
	}
	if lc.SizeBytes() >= sizeBefore {
		t.Fatalf("SizeBytes did not shrink: before=%d after=%d", sizeBefore, lc.SizeBytes())
	}

	// New labels reuse the freed idxs and must decode correctly.
	nextIdxBefore := lc.idxToLabel.nextIdx
	for _, labels := range newTestSeries(100, 10) {
		labels = append(labels, prompb.Label{Name: "generation", Value: "2"})
		got := lc.Decompress(nil, lc.Compress(nil, labels))
		if labelsToString(got) != labelsToString(labels) {
			t.Fatalf("unexpected labels after reuse; got %s; want %s", labelsToString(got), labelsToString(labels))
		}
	}
	if lc.idxToLabel.nextIdx != nextIdxBefore {
		t.Fatalf("new labels allocated fresh idxs (%d -> %d) instead of reusing freed ones", nextIdxBefore, lc.idxToLabel.nextIdx)
	}
}

func TestLabelsCompressorCleanupKeepsLabelsOfLiveKeys(t *testing.T) {
	var lc LabelsCompressor
	labels := []prompb.Label{{Name: "job", Value: "api"}, {Name: "pod", Value: "api-7d9f-x2"}}
	key := lc.Compress(nil, labels)
	markKey := func(mark func([]byte)) {
		mark(key)
	}
	for i := range 10 {
		if freed := lc.Cleanup(markKey); freed != 0 {
			t.Fatalf("Cleanup #%d freed %d labels of a live key", i+1, freed)
		}
	}
	if got := lc.Decompress(nil, key); labelsToString(got) != labelsToString(labels) {
		t.Fatalf("unexpected labels; got %s; want %s", labelsToString(got), labelsToString(labels))
	}
	// The same labels get the same key while they are kept.
	if got := lc.Compress(nil, labels); !bytes.Equal(got, key) {
		t.Fatalf("key of kept labels changed")
	}
	// Once the key is gone, the labels are freed.
	lc.Cleanup(noLiveKeys)
	if freed := lc.Cleanup(noLiveKeys); freed != len(labels) {
		t.Fatalf("Cleanup freed %d labels after the key was gone; want %d", freed, len(labels))
	}
}

func TestLabelsCompressorCleanupKeepsKeysOfUsedLabels(t *testing.T) {
	var lc LabelsCompressor
	labels := []prompb.Label{{Name: "job", Value: "api"}, {Name: "instance", Value: "10.0.0.1:9100"}}
	key := lc.Compress(nil, labels)
	for i := range 10 {
		lc.Cleanup(noLiveKeys)
		if got := lc.Compress(nil, labels); !bytes.Equal(got, key) {
			t.Fatalf("key of a label used every period changed after %d Cleanups", i+1)
		}
	}
}

func TestLabelsCompressorCompressReinsertsFreedLabel(t *testing.T) {
	var lc LabelsCompressor
	labels := []prompb.Label{{Name: "job", Value: "api"}}
	key := lc.Compress(nil, labels)

	// Cleanup marks an entry freed before it removes it from labelToIdx.
	// A Compress that finds the entry in between must insert the label again with a new idx.
	v, _ := lc.labelToIdx.Load(labels[0])
	v.(*labelEntry).state.Store(labelFreed)
	newKey := lc.Compress(nil, labels)
	if bytes.Equal(newKey, key) {
		t.Fatalf("Compress used the idx of a freed label")
	}
	if got := lc.Decompress(nil, newKey); labelsToString(got) != labelsToString(labels) {
		t.Fatalf("unexpected labels; got %s; want %s", labelsToString(got), labelsToString(labels))
	}
}
