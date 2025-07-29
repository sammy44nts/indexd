package keys

import (
	"io"

	"go.sia.tech/core/blake2b"
	"go.sia.tech/core/types"
	"golang.org/x/crypto/hkdf"
)

// DeriveKey derives a key from the given private key and purpose using HKDF.
func DeriveKey(key types.PrivateKey, purpose string) types.PrivateKey {
	seed := make([]byte, 32)
	defer clear(seed)
	hkdf := hkdf.New(blake2b.New256, key[:], []byte(purpose), nil)
	if _, err := io.ReadFull(hkdf, seed); err != nil {
		panic(err) // never happens
	}
	return types.NewPrivateKeyFromSeed(seed)
}
