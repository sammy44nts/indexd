package testutils

import (
	"context"
	"testing"
	"time"

	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/testutil"
	"go.uber.org/zap"
)

type (
	clusterOpts struct {
		logger *zap.Logger
		nHosts int
	}

	// ClusterOpt is a functional option for configuring a cluster for testing
	ClusterOpt func(*clusterOpts)
)

// Cluster is a test cluster that contains an indexer, multiple hosts and an app
// that interacts with them.
type Cluster struct {
	App     *App
	Hosts   []*Host
	Indexer *Indexer
}

var (
	defaultClusterOpts = clusterOpts{
		logger: zap.NewNop(),
		nHosts: 5,
	}
)

// WithLogger allows for attaching a custom logger to the cluster for debugging
// if necessary
func WithLogger(logger *zap.Logger) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.logger = logger
	}
}

// WithNHosts allows for overriding the default number of hosts in the cluster
func WithNHosts(n int) ClusterOpt {
	return func(cfg *clusterOpts) {
		cfg.nHosts = n
	}
}

// NewCluster creates a cluster for testing. A cluster contains an indexer and
// multiple hosts.
func NewCluster(t testing.TB, opts ...ClusterOpt) *Cluster {
	cfg := defaultClusterOpts
	for _, opt := range opts {
		opt(&cfg)
	}

	n, genesis := testutil.V2Network()
	indexer := NewIndexer(t, n, genesis, zap.NewNop())

	// mine until after v2 height to reach v2 and to fund indexer
	indexer.MineBlocks(t, types.Address{}, int(n.HardforkV2.AllowHeight)) // TODO: add wallet to indexer and actually fund it

	// create hosts and connect them to the indexer
	var hosts []*Host
	for i := 0; i < cfg.nHosts; i++ {
		pk := types.GeneratePrivateKey()
		h := NewHost(t, pk, n, genesis, cfg.logger)
		_, err := h.s.Connect(context.Background(), indexer.syncer.Addr())
		if err != nil {
			t.Fatal(err)
		}
		hosts = append(hosts, h)
	}

	// fund hosts with one block each
	for _, h := range hosts {
		indexer.MineBlocks(t, h.w.Address(), 1)
	}

	// mine more blocks to get outputs to mature
	indexer.MineBlocks(t, types.Address{}, int(n.MaturityDelay))

	// TODO: add volumes to hosts

	// TODO: announce hosts

	// TODO: wait for contracts with the hosts

	// TODO: mine blocks and sync up

	// TODO: create app
	app := &App{}

	return &Cluster{
		App:     app,
		Hosts:   hosts,
		Indexer: indexer,
	}
}

func Retry(t testing.TB, tries int, durationBetweenAttempts time.Duration, fn func() error) {
	t.Helper()
	var err error
	for i := 0; i < tries; i++ {
		err = fn()
		if err == nil {
			return
		}
		time.Sleep(durationBetweenAttempts)
	}
	t.Fatal(err)
}
