package streamaggr

import (
	"bytes"
	"fmt"
	"slices"
	"strings"
	"sync"
	"testing"

	"github.com/VictoriaMetrics/VictoriaMetrics/lib/bytesutil"
	"github.com/VictoriaMetrics/VictoriaMetrics/lib/prompb"
)

func TestLabelsCodec(t *testing.T) {
	cases := [][]prompb.Label{
		nil,
		{},
		{{Name: "", Value: ""}},
		{{Name: "a", Value: "bc"}},
		{{Name: "ab", Value: "c"}},
		{{Name: "a\x00b", Value: "\xff\x00☃"}, {Name: "", Value: ""}},
		{{Name: strings.Repeat("n", 70000), Value: strings.Repeat("v", 70000)}},
	}
	for i, labels := range cases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			var encoder, decoder labelsCodec
			prefix := []byte{0x81, 0x00, 0xff}
			data := encoder.Compress(append([]byte(nil), prefix...), labels)
			if !bytes.Equal(data[:len(prefix)], prefix) {
				t.Fatalf("Compress changed destination prefix")
			}
			dst := []prompb.Label{{Name: "prefix", Value: "kept"}}
			got := decoder.Decompress(dst, data[len(prefix):])
			want := append([]prompb.Label{{Name: "prefix", Value: "kept"}}, labels...)
			if !slices.Equal(got, want) {
				t.Fatalf("unexpected roundtrip; got %q; want %q", got, want)
			}
			clear(data)
			if !slices.Equal(got, want) {
				t.Fatalf("decoded labels retain mutable encoded bytes")
			}
		})
	}

	var c labelsCodec
	a := c.Compress(nil, []prompb.Label{{Name: "a", Value: "bc"}})
	b := c.Compress(nil, []prompb.Label{{Name: "ab", Value: "c"}})
	if !bytes.Equal(a, []byte{1, 1, 'a', 2, 'b', 'c'}) {
		t.Fatalf("unexpected exact encoding: %v", a)
	}
	if bytes.Equal(a, b) {
		t.Fatalf("different name/value boundaries produced the same key")
	}

	source := []byte("mutable")
	data := c.Compress(nil, []prompb.Label{{Name: "label", Value: bytesutil.ToUnsafeString(source)}})
	clear(source)
	got := c.Decompress(nil, data)
	if !slices.Equal(got, []prompb.Label{{Name: "label", Value: "mutable"}}) {
		t.Fatalf("encoded labels retain mutable source strings: %q", got)
	}
}

func TestLabelsCodecConcurrent(t *testing.T) {
	var c labelsCodec
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Go(func() {
			var data []byte
			var got []prompb.Label
			for n := 0; n < 1000; n++ {
				labels := []prompb.Label{{Name: "pod", Value: fmt.Sprint(n)}, {Name: "job", Value: "constant"}}
				data = c.Compress(data[:0], labels)
				got = c.Decompress(got[:0], data)
				if !slices.Equal(got, labels) {
					t.Errorf("unexpected concurrent roundtrip; got %q; want %q", got, labels)
					return
				}
			}
		})
	}
	wg.Wait()
	if c.ItemsCount() != 0 || c.SizeBytes() != 0 {
		t.Fatal("stateless codec retained dictionary entries")
	}
}
