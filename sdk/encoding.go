package sdk

import (
	"go.sia.tech/core/types"
)

// EncodeTo implements types.EncoderTo.
func (s Slab) EncodeTo(e *types.Encoder) {
	e.Write(s.ID[:])
	e.WriteUint64(uint64(s.Offset))
	e.WriteUint64(uint64(s.Length))
}

// DecodeFrom implements types.DecoderFrom.
func (s *Slab) DecodeFrom(d *types.Decoder) {
	d.Read(s.ID[:])
	s.Offset = uint32(d.ReadUint64())
	s.Length = uint32(d.ReadUint64())
}

// EncodeTo implements types.EncoderTo.
func (obj Object) EncodeTo(e *types.Encoder) {
	e.WriteBool(obj.Key != nil)
	if obj.Key != nil {
		e.Write((*obj.Key)[:])
	}
	types.EncodeSlice(e, obj.Slabs)
}

// DecodeFrom implements types.DecoderFrom.
func (obj *Object) DecodeFrom(d *types.Decoder) {
	if d.ReadBool() {
		obj.Key = new([32]byte)
		d.Read((*obj.Key)[:])
	}
	types.DecodeSlice(d, &obj.Slabs)
}
