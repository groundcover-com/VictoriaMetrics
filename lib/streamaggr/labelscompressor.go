package streamaggr

import (
	"flag"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
)

var labelsCompressorCleanupInterval = flag.Duration("streamAggr.labelsCompressorCleanupInterval", time.Minute, "The interval for freeing memory "+
	"occupied by labels of series no longer processed by stream aggregation and deduplication. Zero disables freeing. "+
	"See https://docs.victoriametrics.com/victoriametrics/stream-aggregation/")

var (
	lcUsersLock sync.Mutex

	// lcUsers holds the keysGoneBeforeMsec of every running aggregator and deduplicator:
	// the time before which no key in its state was last compressed.
	lcUsers = map[*atomic.Int64]struct{}{}

	lcNextCleanupMsec atomic.Int64

	// lcCleanupWG tracks the cleanups running in the background.
	lcCleanupWG sync.WaitGroup

	// lcClockMsec is the latest time seen by Push calls and flushes. Labels moved to standby are
	// stamped no earlier, so a backward step of the system clock cannot free them early.
	lcClockMsec atomic.Int64
)

func registerLabelsCompressorUser(keysGoneBeforeMsec *atomic.Int64) {
	lcUsersLock.Lock()
	lcUsers[keysGoneBeforeMsec] = struct{}{}
	lcUsersLock.Unlock()
}

func unregisterLabelsCompressorUser(keysGoneBeforeMsec *atomic.Int64) {
	lcUsersLock.Lock()
	delete(lcUsers, keysGoneBeforeMsec)
	lcUsersLock.Unlock()
}

// cleanupLabelsCompressorIfDue starts cleanupLabelsCompressor at most once per -streamAggr.labelsCompressorCleanupInterval.
// It runs in the background, so a long cleanup does not delay the flush that started it.
func cleanupLabelsCompressorIfDue() {
	interval := *labelsCompressorCleanupInterval
	if interval <= 0 {
		return
	}
	nowMsec := time.Now().UnixMilli()
	next := lcNextCleanupMsec.Load()
	if nowMsec < next || !lcNextCleanupMsec.CompareAndSwap(next, nowMsec+interval.Milliseconds()) {
		return
	}
	lcCleanupWG.Go(func() {
		cleanupLabelsCompressor()
	})
}

// cleanupLabelsCompressor frees the labels that no running aggregator or deduplicator can still reference.
func cleanupLabelsCompressor() int {
	lcUsersLock.Lock()
	keysGoneBeforeMsec := time.Now().UnixMilli()
	for u := range lcUsers {
		keysGoneBeforeMsec = min(keysGoneBeforeMsec, u.Load())
	}
	lcUsersLock.Unlock()
	return lc.Cleanup(time.UnixMilli(keysGoneBeforeMsec), labelsCompressorNow)
}

func labelsCompressorNow() time.Time {
	return time.UnixMilli(max(time.Now().UnixMilli(), lcClockMsec.Load()))
}

func advanceLabelsCompressorClock(msec int64) {
	for {
		clock := lcClockMsec.Load()
		if msec <= clock || lcClockMsec.CompareAndSwap(clock, msec) {
			return
		}
	}
}

// pushesInFlight bounds the start times of the Push calls in flight without locking: a Push counts
// itself in the current generation, and the flusher starts a new generation once the previous one
// has no Push in flight.
type pushesInFlight struct {
	gen atomic.Uint64

	// shards spread the per-generation counts of the Push calls in flight over cache lines.
	shards [64]pushesInFlightShard

	// startMsec holds the start time of each generation. It is accessed only by the flusher.
	startMsec [2]int64
}

type pushesInFlightShard struct {
	counts [2]atomic.Int64

	// The padding prevents false sharing with the neighboring shards.
	_ [atomicutil.CacheLineSize - unsafe.Sizeof([2]atomic.Int64{})%atomicutil.CacheLineSize]byte
}

// pushShards assigns a shard of pushesInFlight to every push context, which sync.Pool keeps on one CPU.
var pushShards atomic.Uint32

// start registers a Push from shardIdx and returns its start time and the count to pass to done.
func (p *pushesInFlight) start(shardIdx uint32) (time.Time, *atomic.Int64) {
	shard := &p.shards[shardIdx%uint32(len(p.shards))]
	for {
		gen := p.gen.Load()
		count := &shard.counts[gen%2]
		count.Add(1)
		if p.gen.Load() == gen {
			now := time.Now()
			advanceLabelsCompressorClock(now.UnixMilli())
			return now, count
		}
		count.Add(-1)
	}
}

func (p *pushesInFlight) done(count *atomic.Int64) {
	count.Add(-1)
}

func (p *pushesInFlight) inFlight(gen uint64) bool {
	for i := range p.shards {
		if p.shards[i].counts[gen%2].Load() > 0 {
			return true
		}
	}
	return false
}

// oldestMsec returns a time no later than the start of any Push in flight.
//
// It must be called by one goroutine at a time.
func (p *pushesInFlight) oldestMsec() int64 {
	nowMsec := time.Now().UnixMilli()
	advanceLabelsCompressorClock(nowMsec)
	gen := p.gen.Load()
	if p.inFlight(gen - 1) {
		return p.startMsec[(gen-1)%2]
	}
	// No Push of the previous generation is in flight, so start a new generation.
	p.startMsec[(gen+1)%2] = nowMsec
	p.gen.Store(gen + 1)
	if p.inFlight(gen) {
		return p.startMsec[gen%2]
	}
	return nowMsec
}
