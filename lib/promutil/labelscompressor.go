package promutil

import (
	"sync"
	"sync/atomic"
	"time"
	"unsafe"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// LabelsCompressor compresses []prompb.Label into short binary strings
//
// Labels are evicted with a second chance, so that a label maps to one idx for as long as
// anything may still hold a key referencing it. Compress sets a reference bit per idx and
// every Cleanup clears the bits. A Cleanup moves the labels whose bit stayed clear to standby:
// they leave labelToIdx but stay decodable, and a later Compress restores the same idx.
// A later Cleanup frees the standby labels that stayed unreferenced, after a grace period:
// once the caller knows no key compressed before the move can still be decompressed.
type LabelsCompressor struct {
	labelToIdx sync.Map
	idxToLabel labelsMap

	nextIdx atomic.Uint64

	totalSizeBytes atomic.Uint64
	itemsCount     atomic.Uint64

	// referenced has a reference bit per idx, set by Compress since the last Cleanup.
	referenced atomic.Pointer[[]atomic.Uint64]

	// cleanupLock serializes Cleanup calls; mu serializes inserts into labelToIdx with the
	// parts of Cleanup that change the mapping.
	cleanupLock  sync.Mutex
	mu           sync.Mutex
	standby      map[prompb.Label]uint64
	standbySince time.Time
	freeIdxs     []uint64
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
	referenced := lc.loadReferenced()
	for i, label := range labels {
		var idx uint64
		if v, ok := lc.labelToIdx.Load(label); ok {
			idx = v.(uint64)
		} else {
			idx = lc.insert(label)
		}
		if w := idx / 64; w < uint64(len(referenced)) {
			if bit := uint64(1) << (idx % 64); referenced[w].Load()&bit == 0 {
				referenced[w].Or(bit)
			}
		} else {
			lc.setReferencedSlow(idx)
		}
		dst[i] = idx
	}
}

func (lc *LabelsCompressor) setReferencedSlow(idx uint64) {
	lc.mu.Lock()
	lc.growReferencedLocked(idx)
	referenced := *lc.referenced.Load()
	lc.mu.Unlock()
	referenced[idx/64].Or(uint64(1) << (idx % 64))
}

func (lc *LabelsCompressor) loadReferenced() []atomic.Uint64 {
	if p := lc.referenced.Load(); p != nil {
		return *p
	}
	return nil
}

func isReferenced(referenced []atomic.Uint64, idx uint64) bool {
	return idx/64 < uint64(len(referenced)) && referenced[idx/64].Load()&(uint64(1)<<(idx%64)) != 0
}

func (lc *LabelsCompressor) growReferencedLocked(idx uint64) {
	pReferenced := lc.referenced.Load()
	n := 0
	if pReferenced != nil {
		n = len(*pReferenced)
	}
	if idx/64 < uint64(n) {
		return
	}
	newLen := max(2*n, int(idx/64)+1, 1024)
	referenced := make([]atomic.Uint64, newLen)
	for i := 0; i < n; i++ {
		referenced[i].Store((*pReferenced)[i].Load())
	}
	lc.referenced.Store(&referenced)
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
	labelCopy := cloneLabel(label)
	// Must store idxToLabel entry before labelToIdx, so it can be found by concurrent goroutines.
	lc.idxToLabel.Store(idx, labelCopy)
	lc.labelToIdx.Store(labelCopy, idx)
	lc.growReferencedLocked(idx)

	lc.totalSizeBytes.Add(labelEntrySizeBytes(label))
	lc.itemsCount.Add(1)
	return idx
}

func labelEntrySizeBytes(label prompb.Label) uint64 {
	labelSizeBytes := uint64(len(label.Name) + len(label.Value))
	return labelSizeBytes + uint64(2*(unsafe.Sizeof(label)+unsafe.Sizeof(&label))+unsafe.Sizeof(uint64(0)))
}

// Cleanup frees the standby labels not referenced since they were moved to standby, then
// moves the labels not referenced since the previous Cleanup to standby. Standby labels are
// freed only if they were moved before keysGoneBefore: the caller guarantees that no key
// compressed before that time can be decompressed anymore. It returns the number of freed labels.
//
// It is safe to call Cleanup at any rate, concurrently with Compress and Decompress.
func (lc *LabelsCompressor) Cleanup(keysGoneBefore time.Time) int {
	lc.cleanupLock.Lock()
	defer lc.cleanupLock.Unlock()

	lc.mu.Lock()
	if len(lc.standby) > 0 && !lc.standbySince.Before(keysGoneBefore) {
		lc.mu.Unlock()
		return 0
	}

	referenced := lc.loadReferenced()

	// Free the standby labels not referenced since the previous Cleanup.
	var freed []uint64
	for label, idx := range lc.standby {
		if isReferenced(referenced, idx) {
			labelCopy, _ := lc.idxToLabel.Load(idx)
			lc.labelToIdx.Store(labelCopy, idx)
		} else {
			freed = append(freed, idx)
			lc.totalSizeBytes.Add(^(labelEntrySizeBytes(label) - 1))
			lc.itemsCount.Add(^uint64(0))
		}
		delete(lc.standby, label)
	}
	lc.idxToLabel.deleteBatch(freed)
	lc.freeIdxs = append(lc.freeIdxs, freed...)
	if lc.standby == nil {
		lc.standby = make(map[prompb.Label]uint64)
	}
	lc.mu.Unlock()

	// Move the labels not referenced since the previous Cleanup to standby. The scan runs
	// without lc.mu, so inserts are blocked only while a batch is moved; a standby label
	// keeps its idx, so labels compressed concurrently are restored rather than re-encoded.
	const batchSize = 4096
	batch := make([]prompb.Label, 0, batchSize)
	idxs := make([]uint64, 0, batchSize)
	moveToStandby := func() {
		lc.mu.Lock()
		for i, label := range batch {
			if !isReferenced(referenced, idxs[i]) && lc.labelToIdx.CompareAndDelete(label, idxs[i]) {
				lc.standby[label] = idxs[i]
			}
		}
		lc.mu.Unlock()
		batch, idxs = batch[:0], idxs[:0]
	}
	lc.labelToIdx.Range(func(k, v any) bool {
		if idx := v.(uint64); !isReferenced(referenced, idx) {
			batch = append(batch, k.(prompb.Label))
			idxs = append(idxs, idx)
			if len(batch) == batchSize {
				moveToStandby()
			}
		}
		return true
	})
	moveToStandby()

	// A Compress racing with the scan may still build a key for a label it moved to standby, so
	// the freeing of these labels waits for the keys compressed until now to be gone.
	lc.mu.Lock()
	lc.standbySince = time.Now()
	for i := range referenced {
		referenced[i].Store(0)
	}
	lc.mu.Unlock()
	return len(freed)
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
func (lm *labelsMap) deleteBatch(idxs []uint64) {
	if len(idxs) == 0 {
		return
	}
	lm.mutableLock.Lock()
	defer lm.mutableLock.Unlock()

	lm.moveMutableToReadOnlyLocked(lm.readOnly.Load())
	pReadOnly := lm.readOnly.Load()
	if pReadOnly == nil {
		return
	}
	labels := append([]*prompb.Label(nil), *pReadOnly...)
	for _, idx := range idxs {
		if idx < uint64(len(labels)) {
			labels[idx] = nil
		}
	}
	lm.readOnly.Store(&labels)
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
