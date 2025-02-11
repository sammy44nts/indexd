package testutils

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	rhp4 "go.sia.tech/coreutils/rhp/v4"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

func TestMain(m *testing.M) {
	VerifyTestMain(m)
}

func TestHost(t *testing.T) {
	n, genesis := testutil.V2Network()
	h := NewHost(t, types.GeneratePrivateKey(), n, genesis, zap.NewNop())

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	transport, err := rhp4.DialSiaMux(ctx, h.Addr(), h.PublicKey())
	if err != nil {
		t.Fatal(err)
	}
	defer transport.Close()

	settings, err := rhp4.RPCSettings(ctx, transport)
	if err != nil {
		t.Fatal(err)
	} else if settings.WalletAddress != h.w.Address() {
		t.Fatal("wallet address mismatch")
	}
}
