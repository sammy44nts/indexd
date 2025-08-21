package sdk

import (
	"bytes"
	"reflect"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/indexd/slabs"
	"lukechampine.com/frand"
)

func TestObjectEncoding(t *testing.T) {
	var key [32]byte
	frand.Read(key[:])

	slabs := []Slab{
		{ID: slabs.SlabID(frand.Entropy256()), Offset: 10, Length: 5000},
		{ID: slabs.SlabID(frand.Entropy256()), Offset: 32, Length: 4096},
	}
	objs := []struct {
		name string
		Object
	}{
		{
			name: "with key",
			Object: Object{
				Key:   &key,
				Slabs: slabs,
			},
		},
		{
			name: "without key",
			Object: Object{
				Slabs: slabs,
			},
		},
	}

	for _, expected := range objs {
		t.Run(expected.name, func(t *testing.T) {
			var buf bytes.Buffer
			e := types.NewEncoder(&buf)
			expected.Object.EncodeTo(e)
			e.Flush()

			var got Object
			d := types.NewBufDecoder(buf.Bytes())
			got.DecodeFrom(d)

			if !reflect.DeepEqual(expected.Object, got) {
				t.Fatalf("mismatch after encoding and decoding: expected %v, got %v", expected.Object, got)
			}
		})
	}
}
