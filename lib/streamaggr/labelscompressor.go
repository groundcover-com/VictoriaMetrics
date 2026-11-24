package streamaggr

import (
	"sync"
	"sync/atomic"
	"time"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
)

// A background goroutine frees the labels of lc that no aggregator or deduplicator uses,
// so label churn doesn't grow memory until restart.
//
// A label is freed when no stored key has it and Compress didn't use it since the previous cleanup.
// Every aggregator and deduplicator reports the keys it stores via markLabels.
// Keys that a Push compressed but didn't store yet are covered by pushesInFlight.

// labelsCleanupInterval is how often the labels that no stored key has are freed.
const labelsCleanupInterval = 5 * time.Minute

func init() {
	go func() {
		ticker := time.NewTicker(labelsCleanupInterval)
		for range ticker.C {
			cleanupLabels()
		}
	}()
}

// labelsCleanupLock serializes cleanupLabels calls.
var labelsCleanupLock sync.Mutex

// cleanupLabels frees the labels of lc that no aggregator or deduplicator uses and returns their number.
func cleanupLabels() int {
	labelsCleanupLock.Lock()
	defer labelsCleanupLock.Unlock()

	if pushesInFlight[(pushesGen.Load()+1)%2].Load() != 0 {
		// A Push that started before the previous cleanup may still hold keys it didn't store.
		return 0
	}
	freed := lc.Cleanup(func(mark func([]byte)) {
		labelsUsers.Range(func(u, _ any) bool {
			u.(labelsUser).markLabels(mark)
			return true
		})
	})
	pushesGen.Add(1)
	return freed
}

// pushesInFlight counts the Push calls in flight in two generations.
//
// A Push compresses its keys before it stores them, so markLabels can't see them in between.
// Compress marks their labels used, so the next cleanup keeps them, but the one after could free them.
// So every cleanup starts a new generation and runs only when no Push of the previous generation is in flight.
var (
	pushesGen      atomic.Uint64
	pushesInFlight [2]atomic.Int64
)

// pushInFlight is a Push counted in pushesInFlight.
type pushInFlight struct {
	n *atomic.Int64
}

// done must be called when the Push stored its keys.
func (p pushInFlight) done() {
	p.n.Add(-1)
}

// startPush counts a Push in the current generation.
func startPush() pushInFlight {
	for {
		gen := pushesGen.Load()
		n := &pushesInFlight[gen%2]
		n.Add(1)
		if pushesGen.Load() == gen {
			return pushInFlight{n: n}
		}
		// A cleanup started a new generation meanwhile, so count the Push in it.
		n.Add(-1)
	}
}

// labelsUser stores keys compressed with lc.
type labelsUser interface {
	// markLabels calls mark for the compressed labels of every key it stores.
	markLabels(mark func(compressedLabels []byte))
}

// labelsUsers holds every running aggregator and deduplicator.
var labelsUsers sync.Map

func (a *aggregator) markLabels(mark func([]byte)) {
	// A dedup flush moves keys from a.da to a.aggrOutputs, so mark a.da first.
	if a.da != nil {
		a.da.markKeys(func(key string) {
			markKey(mark, key)
		})
	}
	a.aggrOutputs.markLabels(mark)
}

func (d *Deduplicator) markLabels(mark func([]byte)) {
	d.da.markKeys(func(key string) {
		mark(bytesutil.ToUnsafeBytes(key))
	})
}

// markKeys calls mark for every key in da, including the keys a flush took but didn't pass on yet.
func (da *dedupAggr) markKeys(mark func(key string)) {
	for i := range da.shards {
		shard := &da.shards[i]
		for _, state := range []*dedupAggrState{&shard.blue, &shard.green} {
			state.mu.Lock()
			for key := range state.m {
				mark(key)
			}
			for key := range state.flushing {
				mark(key)
			}
			state.mu.Unlock()
		}
	}
}

// inputKeysHolder is an aggrValue that stores input keys. Every output that stores input keys must implement it.
type inputKeysHolder interface {
	markInputKeys(mark func(key string))
}

// markLabels calls mark for the labels of every output key and every input key stored in ao.
func (ao *aggrOutputs) markLabels(mark func([]byte)) {
	markInputKey := func(key string) {
		if ao.useInputKey {
			mark(bytesutil.ToUnsafeBytes(key))
		} else {
			markKey(mark, key)
		}
	}
	ao.m.Range(func(k, v any) bool {
		mark(bytesutil.ToUnsafeBytes(k.(string)))
		av := v.(*aggrValues)
		av.mu.Lock()
		for _, outputs := range [][]aggrValue{av.blue, av.green} {
			for _, o := range outputs {
				if h, ok := o.(inputKeysHolder); ok {
					h.markInputKeys(markInputKey)
				}
			}
		}
		av.mu.Unlock()
		return true
	})
}

func (av *totalAggrValue) markInputKeys(mark func(key string)) {
	for key := range av.shared.lastValues {
		mark(key)
	}
}

func (av *increaseAggrValue) markInputKeys(mark func(key string)) {
	for key := range av.shared {
		mark(key)
	}
}

func (av *rateAggrValue) markInputKeys(mark func(key string)) {
	for key := range av.shared {
		mark(key)
	}
}

func (av *countSeriesAggrValue) markInputKeys(mark func(key string)) {
	for src := av.keys; len(src) > 0; {
		key, n := encoding.UnmarshalBytes(src)
		mark(bytesutil.ToUnsafeString(key))
		src = src[n:]
	}
}

// markKey calls mark for the output and input labels of a key made by compressLabels.
func markKey(mark func([]byte), key string) {
	src := bytesutil.ToUnsafeBytes(key)
	outputKeyLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal outputKeyLen from uvarint")
	}
	src = src[nSize:]
	mark(src[:outputKeyLen])
	mark(src[outputKeyLen:])
}
