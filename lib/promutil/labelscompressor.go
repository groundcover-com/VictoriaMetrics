package promutil

import (
	"slices"
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/atomicutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// LabelsCompressor compresses []prompb.Label into short binary strings
type LabelsCompressor struct {
	labelToIdx sync.Map
	idxToLabel labelsMap

	nextIdx atomic.Uint64

	totalSizeBytes atomic.Uint64
	itemsCount     atomic.Uint64

	// referenced holds a reference bit per idx, set by Compress and cleared when Cleanup visits the idx.
	// Compress loads it for every label, so it is padded away from the fields written by inserts.
	_          [atomicutil.CacheLineSize]byte
	referenced atomic.Pointer[referencedChunks]
	_          [atomicutil.CacheLineSize]byte

	// cleanupLock serializes Cleanup calls; mu serializes inserts into labelToIdx with the
	// parts of Cleanup that change the mapping.
	cleanupLock sync.Mutex
	mu          sync.Mutex

	// standby holds the labels moved out of labelToIdx by the last Cleanup, at standbySince.
	standby      map[prompb.Label]uint64
	standbySince time.Time

	// freeIdxs holds the idxs of freed labels for reuse.
	freeIdxs []uint64
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
		var idx uint64
		if v, ok := lc.labelToIdx.Load(label); ok {
			idx = v.(uint64)
		} else {
			idx = lc.insert(label)
		}
		word, bit := lc.referencedBit(idx)
		if word.Load()&bit == 0 {
			word.Or(bit)
		}
		dst[i] = idx
	}
}

// referencedChunks grows by chunks, which are never copied, so no bit is lost while it grows.
//
// It is padded, so the slice header loaded by Compress does not share a cache line with other heap objects.
type referencedChunks struct {
	_      [atomicutil.CacheLineSize]byte
	chunks []*referencedChunk
	_      [atomicutil.CacheLineSize]byte
}

// referencedChunk holds the reference bits of 64*referencedChunkWords consecutive idxs.
type referencedChunk [referencedChunkWords]atomic.Uint64

const referencedChunkWords = 1024

func (lc *LabelsCompressor) referencedBit(idx uint64) (*atomic.Uint64, uint64) {
	w := idx / 64
	chunk := lc.referenced.Load().chunks[w/referencedChunkWords]
	return &chunk[w%referencedChunkWords], uint64(1) << (idx % 64)
}

func (lc *LabelsCompressor) isReferenced(idx uint64) bool {
	word, bit := lc.referencedBit(idx)
	return word.Load()&bit != 0
}

// clearReferenced clears the reference bit of idx and reports whether it was set.
func (lc *LabelsCompressor) clearReferenced(idx uint64) bool {
	word, bit := lc.referencedBit(idx)
	return word.Load()&bit != 0 && word.And(^bit)&bit != 0
}

// growReferencedLocked adds the chunk for idx. It must be called before idx is published.
func (lc *LabelsCompressor) growReferencedLocked(idx uint64) {
	var chunks []*referencedChunk
	if p := lc.referenced.Load(); p != nil {
		chunks = p.chunks
	}
	if idx/64/referencedChunkWords < uint64(len(chunks)) {
		return
	}
	lc.referenced.Store(&referencedChunks{
		chunks: append(chunks, new(referencedChunk)),
	})
}

func (lc *LabelsCompressor) insert(label prompb.Label) uint64 {
	lc.mu.Lock()
	defer lc.mu.Unlock()

	if v, ok := lc.labelToIdx.Load(label); ok {
		return v.(uint64)
	}
	if idx, ok := lc.standby[label]; ok {
		// The label is on standby and its idx may still be referenced; restore it.
		delete(lc.standby, label)
		labelCopy, _ := lc.idxToLabel.Load(idx)
		lc.labelToIdx.Store(labelCopy, idx)
		return idx
	}

	var idx uint64
	if n := len(lc.freeIdxs); n > 0 {
		idx = lc.freeIdxs[n-1]
		lc.freeIdxs = lc.freeIdxs[:n-1]
	} else {
		idx = lc.nextIdx.Add(1)
	}
	lc.growReferencedLocked(idx)
	labelCopy := cloneLabel(label)
	// Must store idxToLabel entry before labelToIdx, so it can be found by concurrent goroutines.
	lc.idxToLabel.Store(idx, labelCopy)
	lc.labelToIdx.Store(labelCopy, idx)

	lc.totalSizeBytes.Add(labelEntrySizeBytes(label))
	lc.itemsCount.Add(1)
	return idx
}

func labelEntrySizeBytes(label prompb.Label) uint64 {
	labelSizeBytes := uint64(len(label.Name) + len(label.Value))
	return labelSizeBytes + uint64(2*(unsafe.Sizeof(label)+unsafe.Sizeof(&label))+unsafe.Sizeof(any(nil)))
}

// Cleanup frees labels that no key in use references and returns the number of freed labels.
//
// The caller guarantees that every key still in use was last compressed at or after keysGoneBefore.
// now returns the time Cleanup stamps on the labels it moves to standby; it must not be earlier
// than the time of any key compressed before the call.
//
// Every label gets a second chance: Compress sets its reference bit, and Cleanup clears the bit
// when it visits the label. Cleanup moves the labels not referenced since the previous visit
// to standby. A standby label leaves labelToIdx but stays decodable, and Compress restores its idx.
// A later Cleanup frees the standby labels once they were moved before keysGoneBefore.
//
// It is safe calling Cleanup concurrently with Compress and Decompress.
func (lc *LabelsCompressor) Cleanup(keysGoneBefore time.Time, now func() time.Time) int {
	lc.cleanupLock.Lock()
	defer lc.cleanupLock.Unlock()

	lc.mu.Lock()
	if len(lc.standby) > 0 && !lc.standbySince.Before(keysGoneBefore) {
		lc.mu.Unlock()
		return 0
	}

	// No key in use references the standby labels: a Compress that found one before the move
	// built a key last compressed before standbySince, and a later Compress restored the label.
	standby := lc.standby
	lc.standby = make(map[prompb.Label]uint64)
	lc.mu.Unlock()
	freed := lc.free(standby)

	// Move the labels not referenced since the previous visit to standby. The scan runs
	// without lc.mu, so it blocks inserts only while a batch is moved; a standby label
	// keeps its idx, so labels compressed concurrently are restored rather than re-encoded.
	batch := make([]prompb.Label, 0, cleanupBatchSize)
	idxs := make([]uint64, 0, cleanupBatchSize)
	moveToStandby := func() {
		lc.mu.Lock()
		for i, label := range batch {
			if !lc.isReferenced(idxs[i]) && lc.labelToIdx.CompareAndDelete(label, idxs[i]) {
				lc.standby[label] = idxs[i]
			}
		}
		lc.mu.Unlock()
		batch, idxs = batch[:0], idxs[:0]
	}
	lc.labelToIdx.Range(func(k, v any) bool {
		if idx := v.(uint64); !lc.clearReferenced(idx) {
			batch = append(batch, k.(prompb.Label))
			idxs = append(idxs, idx)
			if len(batch) == cleanupBatchSize {
				moveToStandby()
			}
		}
		return true
	})
	moveToStandby()

	// A Compress racing with the scan may still build a key for a label it moved to standby, so
	// the freeing of these labels waits for the keys compressed until now to be gone.
	lc.mu.Lock()
	lc.standbySince = now()
	lc.mu.Unlock()
	return freed
}

// cleanupBatchSize is the number of labels Cleanup moves or frees while blocking inserts.
const cleanupBatchSize = 4096

// free frees the given standby labels in batches, so it blocks inserts and loads only briefly.
func (lc *LabelsCompressor) free(standby map[prompb.Label]uint64) int {
	idxs := make([]uint64, 0, len(standby))
	var sizeBytes uint64
	for label, idx := range standby {
		idxs = append(idxs, idx)
		sizeBytes += labelEntrySizeBytes(label)
	}
	for batch := range slices.Chunk(idxs, cleanupBatchSize) {
		lc.idxToLabel.deleteBatch(batch)
	}
	lc.totalSizeBytes.Add(-sizeBytes)
	lc.itemsCount.Add(-uint64(len(idxs)))

	lc.mu.Lock()
	lc.freeIdxs = append(lc.freeIdxs, idxs...)
	lc.mu.Unlock()
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
}

// Store stores label under the given idx.
//
// It is safe calling Store from concurrent goroutines.
func (lm *labelsMap) Store(idx uint64, label prompb.Label) {
	lm.mutableLock.Lock()
	if lm.mutable == nil {
		lm.mutable = make(map[uint64]*prompb.Label)
	}
	lm.mutable[idx] = &label
	lm.mutableLock.Unlock()
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

// deleteBatch removes the labels at idxs, so the idxs can be stored again.
//
// No key references idxs, so nothing loads them and readOnly can be updated in place.
func (lm *labelsMap) deleteBatch(idxs []uint64) {
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
