package promutil

import (
	"slices"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/uint64set"
)

// LabelsCompressor compresses []prompb.Label into short binary strings
type LabelsCompressor struct {
	labelToIdx sync.Map
	idxToLabel labelsMap

	totalSizeBytes atomic.Uint64
	itemsCount     atomic.Uint64

	// cleanupLock serializes Cleanup calls. Compress and Decompress never take it.
	cleanupLock sync.Mutex
}

// labelEntry is the labelToIdx value of a label.
type labelEntry struct {
	idx uint64

	// state is labelUsed, labelIdle or labelFreed.
	state atomic.Uint32
}

const (
	// labelUsed means Compress used the label since the previous Cleanup. New entries start in this state.
	labelUsed = iota

	// labelIdle means Compress didn't use the label since the previous Cleanup.
	labelIdle

	// labelFreed means Cleanup freed the label, so its idx must not be used. This state is final.
	labelFreed
)

// use marks e as used. It returns false if Cleanup already freed e.
func (e *labelEntry) use() bool {
	for {
		switch e.state.Load() {
		case labelUsed:
			return true
		case labelFreed:
			return false
		}
		if e.state.CompareAndSwap(labelIdle, labelUsed) {
			return true
		}
	}
}

// SizeBytes returns the size of lc data in bytes
func (lc *LabelsCompressor) SizeBytes() uint64 {
	return uint64(unsafe.Sizeof(*lc)) + lc.totalSizeBytes.Load()
}

// ItemsCount returns the number of items in lc
func (lc *LabelsCompressor) ItemsCount() uint64 {
	return lc.itemsCount.Load()
}

// Compress compresses labels, appends the compressed labels to dst and returns the result.
//
// It is safe calling Compress from concurrent goroutines.
func (lc *LabelsCompressor) Compress(dst []byte, labels []prompb.Label) []byte {
	if len(labels) == 0 {
		// Fast path
		return append(dst, 0)
	}

	a := encoding.GetUint64s(len(labels) + 1)
	a.A[0] = uint64(len(labels))
	lc.compress(a.A[1:], labels)
	dst = encoding.MarshalVarUint64s(dst, a.A)
	encoding.PutUint64s(a)
	return dst
}

func (lc *LabelsCompressor) compress(dst []uint64, labels []prompb.Label) {
	if len(labels) == 0 {
		return
	}
	_ = dst[len(labels)-1]
	for i, label := range labels {
		dst[i] = lc.compressLabel(label)
	}
}

func (lc *LabelsCompressor) compressLabel(label prompb.Label) uint64 {
	for {
		v, ok := lc.labelToIdx.Load(label)
		if !ok {
			v = lc.insert(label)
		}
		e := v.(*labelEntry)
		if e.use() {
			return e.idx
		}
		// Cleanup freed the label after it was loaded. Remove it if Cleanup didn't yet, and insert it again.
		lc.labelToIdx.CompareAndDelete(label, e)
	}
}

func (lc *LabelsCompressor) insert(label prompb.Label) *labelEntry {
	labelCopy := cloneLabel(label)
	// Must store idxToLabel entry before labelToIdx, so it can be found by concurrent goroutines.
	e := &labelEntry{
		idx: lc.idxToLabel.Add(labelCopy),
	}
	v, loaded := lc.labelToIdx.LoadOrStore(labelCopy, e)
	if loaded {
		// A concurrent goroutine stored this label first. No key uses e.idx, so free it at once.
		lc.idxToLabel.free([]uint64{e.idx})
		return v.(*labelEntry)
	}
	lc.totalSizeBytes.Add(labelEntrySizeBytes(label))
	lc.itemsCount.Add(1)
	return e
}

func labelEntrySizeBytes(label prompb.Label) uint64 {
	labelSizeBytes := uint64(len(label.Name) + len(label.Value))
	return labelSizeBytes + uint64(2*(unsafe.Sizeof(label)+unsafe.Sizeof(&label))+unsafe.Sizeof(labelEntry{}))
}

// Cleanup frees labels that no key uses and returns the number of freed labels.
//
// A label is freed if Compress didn't use it since the previous Cleanup and no key in use has it.
// markLiveKeys must call mark for the compressed labels of every key in use. It may skip keys
// compressed after the previous Cleanup returned, because Compress marked their labels used.
//
// It is safe calling Cleanup concurrently with Compress and Decompress.
func (lc *LabelsCompressor) Cleanup(markLiveKeys func(mark func(compressedLabels []byte))) int {
	lc.cleanupLock.Lock()
	defer lc.cleanupLock.Unlock()

	var live uint64set.Set
	markLiveKeys(func(src []byte) {
		n, nSize := encoding.UnmarshalVarUint64(src)
		if nSize <= 0 {
			logger.Panicf("BUG: cannot unmarshal labels length from uvarint")
		}
		src = src[nSize:]
		for range n {
			idx, nSize := encoding.UnmarshalVarUint64(src)
			if nSize <= 0 {
				logger.Panicf("BUG: cannot unmarshal label idx from uvarint")
			}
			src = src[nSize:]
			live.Add(idx)
		}
	})

	var idxs []uint64
	var sizeBytes uint64
	lc.labelToIdx.Range(func(k, v any) bool {
		e := v.(*labelEntry)
		if e.state.CompareAndSwap(labelUsed, labelIdle) || live.Has(e.idx) {
			return true
		}
		// A Compress that uses the label from now on sees labelFreed.
		if e.state.CompareAndSwap(labelIdle, labelFreed) {
			lc.labelToIdx.CompareAndDelete(k, e)
			idxs = append(idxs, e.idx)
			sizeBytes += labelEntrySizeBytes(k.(prompb.Label))
		}
		return true
	})
	// Free in batches, so inserts wait for a short time only.
	for batch := range slices.Chunk(idxs, 4096) {
		lc.idxToLabel.free(batch)
	}
	lc.totalSizeBytes.Add(-sizeBytes)
	lc.itemsCount.Add(-uint64(len(idxs)))
	return len(idxs)
}

func cloneLabel(label prompb.Label) prompb.Label {
	// pre-allocate memory for label name and value
	n := len(label.Name) + len(label.Value)
	buf := make([]byte, 0, n)

	buf = append(buf, label.Name...)
	labelName := bytesutil.ToUnsafeString(buf)

	buf = append(buf, label.Value...)
	labelValue := bytesutil.ToUnsafeString(buf[len(labelName):])
	return prompb.Label{
		Name:  labelName,
		Value: labelValue,
	}
}

// Decompress decompresses src into []prompb.Label, appends it to dst and returns the result.
//
// It is safe calling Decompress from concurrent goroutines.
func (lc *LabelsCompressor) Decompress(dst []prompb.Label, src []byte) []prompb.Label {
	labelsLen, nSize := encoding.UnmarshalVarUint64(src)
	if nSize <= 0 {
		logger.Panicf("BUG: cannot unmarshal labels length from uvarint")
	}
	tail := src[nSize:]
	if labelsLen == 0 {
		// fast path - nothing to decode
		if len(tail) > 0 {
			logger.Panicf("BUG: unexpected non-empty tail left; len(tail)=%d; tail=%X", len(tail), tail)
		}
		return dst
	}

	a := encoding.GetUint64s(int(labelsLen))
	var err error
	tail, err = encoding.UnmarshalVarUint64s(a.A, tail)
	if err != nil {
		logger.Panicf("BUG: cannot unmarshal label indexes: %s", err)
	}
	if len(tail) > 0 {
		logger.Panicf("BUG: unexpected non-empty tail left: len(tail)=%d; tail=%X", len(tail), tail)
	}
	dst = lc.decompress(dst, a.A)
	encoding.PutUint64s(a)
	return dst
}

func (lc *LabelsCompressor) decompress(dst []prompb.Label, src []uint64) []prompb.Label {
	for _, idx := range src {
		label, ok := lc.idxToLabel.Load(idx)
		if !ok {
			logger.Panicf("BUG: missing label for idx=%d", idx)
		}
		dst = append(dst, label)
	}
	return dst
}

// labelsMap maps uint64 key to prompb.Label
//
// uint64 keys must be packed close to 0. Otherwise the labelsMap structure will consume too much memory.
type labelsMap struct {
	readOnly atomic.Pointer[[]*prompb.Label]

	mutableLock sync.Mutex
	mutable     map[uint64]*prompb.Label
	misses      uint64

	// nextIdx and freeIdxs are protected by mutableLock.
	nextIdx  uint64
	freeIdxs []uint64
}

// Add stores label under a free idx and returns the idx.
//
// It is safe calling Add from concurrent goroutines.
func (lm *labelsMap) Add(label prompb.Label) uint64 {
	lm.mutableLock.Lock()
	var idx uint64
	if n := len(lm.freeIdxs); n > 0 {
		idx = lm.freeIdxs[n-1]
		lm.freeIdxs = lm.freeIdxs[:n-1]
	} else {
		lm.nextIdx++
		idx = lm.nextIdx
	}
	if lm.mutable == nil {
		lm.mutable = make(map[uint64]*prompb.Label)
	}
	lm.mutable[idx] = &label
	lm.mutableLock.Unlock()
	return idx
}

// Load returns the label for the given idx.
//
// Load returns false if lm doesn't contain label for the given idx.
//
// It is safe calling Load from concurrent goroutines.
//
// The performance of Load() scales linearly with CPU cores.
func (lm *labelsMap) Load(idx uint64) (prompb.Label, bool) {
	if pReadOnly := lm.readOnly.Load(); pReadOnly != nil && idx < uint64(len(*pReadOnly)) {
		if pLabel := (*pReadOnly)[idx]; pLabel != nil {
			// Fast path - the label for the given idx has been found in lm.readOnly.
			return *pLabel, true
		}
	}

	// Slow path - search in lm.mutable.
	return lm.loadSlow(idx)
}

func (lm *labelsMap) loadSlow(idx uint64) (prompb.Label, bool) {
	lm.mutableLock.Lock()

	// Try loading label from readOnly, since it could be updated while acquiring mutableLock.
	pReadOnly := lm.readOnly.Load()
	if pReadOnly != nil && idx < uint64(len(*pReadOnly)) {
		if pLabel := (*pReadOnly)[idx]; pLabel != nil {
			lm.mutableLock.Unlock()
			return *pLabel, true
		}
	}

	// The label for the idx wasn't found in readOnly. Search it in mutable.
	lm.misses++
	pLabel := lm.mutable[idx]
	if pReadOnly == nil || lm.misses > uint64(len(*pReadOnly)) {
		lm.moveMutableToReadOnlyLocked(pReadOnly)
		lm.misses = 0
	}
	lm.mutableLock.Unlock()

	if pLabel == nil {
		return prompb.Label{}, false
	}
	return *pLabel, true
}

// free removes the labels at idxs, so the idxs can be used again.
//
// No key uses idxs, so nothing loads them and readOnly can be changed in place.
func (lm *labelsMap) free(idxs []uint64) {
	lm.mutableLock.Lock()
	defer lm.mutableLock.Unlock()

	var readOnly []*prompb.Label
	if p := lm.readOnly.Load(); p != nil {
		readOnly = *p
	}
	for _, idx := range idxs {
		delete(lm.mutable, idx)
		if idx < uint64(len(readOnly)) {
			readOnly[idx] = nil
		}
	}
	lm.freeIdxs = append(lm.freeIdxs, idxs...)
}

func (lm *labelsMap) moveMutableToReadOnlyLocked(pReadOnly *[]*prompb.Label) {
	if len(lm.mutable) == 0 {
		// Nothing to move
		return
	}

	var labels []*prompb.Label
	if pReadOnly != nil {
		labels = append(labels, *pReadOnly...)
	}
	for idx, pLabel := range lm.mutable {
		if idx < uint64(len(labels)) {
			labels[idx] = pLabel
		} else {
			for idx > uint64(len(labels)) {
				labels = append(labels, nil)
			}
			labels = append(labels, pLabel)
		}
	}
	clear(lm.mutable)
	lm.readOnly.Store(&labels)
}
