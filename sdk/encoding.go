package sdk

import (
	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (x Slab) EncodeTo(e *types.Encoder) {
	e.Write(x.ID[:])
	e.WriteUint64(uint64(x.Offset))
	e.WriteUint64(uint64(x.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (x *Slab) DecodeFrom(d *types.Decoder) {
	d.Read(x.ID[:])
	x.Offset = uint32(d.ReadUint64())
	x.Length = uint32(d.ReadUint64())
}

// EncodeTo implements types.EncoderTo.
func (x Object) EncodeTo(e *types.Encoder) {
	e.WriteBool(x.Key != nil)
	if x.Key != nil {
		e.Write((*x.Key)[:])
	}
	types.EncodeSlice(e, x.Slabs)
}

// DecodeFrom implements types.DecoderFrom.
func (x *Object) DecodeFrom(d *types.Decoder) {
	if d.ReadBool() {
		x.Key = new([32]byte)
		d.Read((*x.Key)[:])
	}
	types.DecodeSlice(d, &x.Slabs)
}
