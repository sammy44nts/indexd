package sdk

import (
	"crypto/cipher"
	"encoding/binary"
	"fmt"
	"io"
	"math"

	"golang.org/x/crypto/chacha20"
)

type rekeyStream struct {
	key []byte
	c   *chacha20.Cipher

	counter uint64
	nonce   uint64
}

func (rs *rekeyStream) XORKeyStream(dst, src []byte) {
	rs.counter += uint64(len(src))
	if rs.counter < 64*math.MaxUint32 {
		rs.c.XORKeyStream(dst, src)
		return
	}
	// counter overflow; xor remaining bytes, then increment nonce and xor again
	rem := 64*math.MaxUint32 - (rs.counter - uint64(len(src)))
	rs.counter -= 64 * math.MaxUint32
	rs.c.XORKeyStream(dst[:rem], src[:rem])
	// NOTE: we increment the last 8 bytes because XChaCha uses the
	// first 16 bytes to derive a new key; leaving them alone means
	// the key will be stable, which might be useful.
	rs.nonce++
	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], rs.nonce)
	rs.c, _ = chacha20.NewUnauthenticatedCipher(rs.key, nonce)
	rs.c.XORKeyStream(dst[rem:], src[rem:])
}

// encrypt returns a cipher.StreamReader that encrypts r with k starting at the
// given offset.
func encrypt(key *[32]byte, r io.Reader, offset uint64) (cipher.StreamReader, error) {
	if offset%64 != 0 {
		return cipher.StreamReader{}, fmt.Errorf("offset must be a multiple of 64, got %v", offset)
	}
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(key[:], nonce)
	c.SetCounter(uint32(offset / 64))
	rs := &rekeyStream{key: key[:], c: c}
	return cipher.StreamReader{S: rs, R: r}, nil
}

// decrypt returns a cipher.StreamWriter that decrypts w with k, starting at the
// specified offset.
func decrypt(key *[32]byte, w io.Writer, offset uint64) cipher.StreamWriter {
	nonce64 := offset / (64 * math.MaxUint32)
	offset %= 64 * math.MaxUint32

	nonce := make([]byte, 24)
	binary.LittleEndian.PutUint64(nonce[16:], nonce64)
	c, _ := chacha20.NewUnauthenticatedCipher(key[:], nonce)
	c.SetCounter(uint32(offset / 64))

	var buf [64]byte
	c.XORKeyStream(buf[:offset%64], buf[:offset%64])
	rs := &rekeyStream{key: key[:], c: c, counter: offset, nonce: nonce64}
	return cipher.StreamWriter{S: rs, W: w}
}
