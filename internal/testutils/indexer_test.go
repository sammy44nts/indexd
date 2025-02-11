package testutils

import (
	"testing"

	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/indexd/api"
	"go.uber.org/zap"
)

func TestIndexer(t *testing.T) {
	n, genesis := testutil.V2Network()
	indexer, close := NewIndexer(t, n, genesis, zap.NewNop())
	defer close()

	state, err := indexer.Client().State()
	if err != nil {
		t.Fatal(err)
	} else if state.BuildState == (api.BuildState{}) {
		t.Fatal("expected build state")
	} else if state.StartTime.IsZero() {
		t.Fatal("expected start time")
	}
}
