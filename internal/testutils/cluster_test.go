package testutils

import (
	"errors"
	"testing"
	"time"
)

func TestNewCluster(t *testing.T) {
	cluster := NewCluster(t)

	// assert blocks were mined
	indexer := cluster.Indexer
	state, err := indexer.TipState()
	if err != nil {
		t.Fatal(err)
	} else if state.Index.Height == 0 {
		t.Fatal("no blocks were mined")
	}

	// assert hosts were created
	if len(cluster.Hosts) != defaultClusterOpts.nHosts {
		t.Fatalf("expected %d hosts, got %d", defaultClusterOpts.nHosts, len(cluster.Hosts))
	}

	// assert hosts were funded
	Retry(t, 100, 100*time.Millisecond, func() error {
		for _, h := range cluster.Hosts {
			b, err := h.w.Balance()
			if err != nil {
				t.Fatal(err)
			} else if b.Confirmed.IsZero() {
				return errors.New("host not funded")
			}
		}
		return nil
	})

	// assert all peers are synced
	tip := indexer.cm.Tip()
	for _, h := range cluster.Hosts {
		if h.c.Tip() != tip {
			t.Fatal("host is not synced")
		}
	}

	// TODO: extend this as features get implemented
}
