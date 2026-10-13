package streamaggr

import (
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/encoding"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/logger"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

// labelsCodec encodes labels in self-contained binary strings.
type labelsCodec struct{}

// SizeBytes returns the retained dictionary size. No dictionary is retained.
func (lc *labelsCodec) SizeBytes() uint64 { return 0 }

// ItemsCount returns the retained dictionary entries. No dictionary is retained.
func (lc *labelsCodec) ItemsCount() uint64 { return 0 }

// Compress appends encoded labels to dst. Concurrent calls are safe.
func (lc *labelsCodec) Compress(dst []byte, labels []prompb.Label) []byte {
	dst = encoding.MarshalVarUint64(dst, uint64(len(labels)))
	for _, label := range labels {
		dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(label.Name))
		dst = encoding.MarshalBytes(dst, bytesutil.ToUnsafeBytes(label.Value))
	}
	return dst
}

// Decompress appends decoded labels to dst. Concurrent calls are safe.
func (lc *labelsCodec) Decompress(dst []prompb.Label, src []byte) []prompb.Label {
	labelsLen, n := encoding.UnmarshalVarUint64(src)
	if n <= 0 {
		logger.Panicf("BUG: cannot unmarshal labels length from uvarint")
	}
	src = src[n:]
	for ; labelsLen > 0; labelsLen-- {
		name, n := encoding.UnmarshalBytes(src)
		if n <= 0 {
			logger.Panicf("BUG: cannot unmarshal label name")
		}
		src = src[n:]
		value, n := encoding.UnmarshalBytes(src)
		if n <= 0 {
			logger.Panicf("BUG: cannot unmarshal label value")
		}
		src = src[n:]
		dst = append(dst, prompb.Label{Name: string(name), Value: string(value)})
	}
	if len(src) != 0 {
		logger.Panicf("BUG: unexpected non-empty tail left; len(tail)=%d; tail=%X", len(src), src)
	}
	return dst
}
