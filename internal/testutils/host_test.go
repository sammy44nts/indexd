package testutils

import (
	"context"
	"testing"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	rhp "go.sia.tech/indexd/internal/rhp/v4"
	"go.uber.org/goleak"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	goleak.VerifyTestMain(m)
}

func TestHost(t *testing.T) {
	n, genesis := testutil.V2Network()
	h := NewHost(t, types.GeneratePrivateKey(), n, genesis, zap.NewNop())

	settings, err := rhp.New().Settings(context.Background(), h.PublicKey(), h.Addr())
	if err != nil {
		t.Fatal(err)
	} else if settings.WalletAddress != h.w.Address() {
		t.Fatal("wallet address mismatch")
	}
}
