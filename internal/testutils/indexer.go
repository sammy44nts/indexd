package testutils

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
	"go.sia.tech/core/consensus"
	"go.sia.tech/core/gateway"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/chain"
	"go.sia.tech/coreutils/syncer"
	"go.sia.tech/coreutils/testutil"
	"go.sia.tech/indexd/api"
	"go.sia.tech/indexd/persist/postgres"
	"go.sia.tech/jape"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

// Indexer is a test utility combining an indexer, an http client for the
// indexer and useful helpers for testing.
type Indexer struct {
	*api.Client
}

// NewIndexer creates a new indexer for testing that is automatically closed up
// after the test is finished.
func NewIndexer(t testing.TB, n *consensus.Network, genesis types.Block, log *zap.Logger) *Indexer {
	indexer, cleanup := NewIndexerNoCleanup(t, n, genesis, log)
	t.Cleanup(cleanup)
	return indexer
}

// NewIndexerNoCleanup creates a new indexer for testing. It returns a cleanup
// function that closes all of its resources and causes the test to fail if any
// of them fail to close. Useful for tests that require closing the indexer
// ahead of time.
func NewIndexerNoCleanup(t testing.TB, n *consensus.Network, genesis types.Block, log *zap.Logger) (*Indexer, func()) {
	dbstore, tipState, err := chain.NewDBStore(chain.NewMemDB(), n, genesis)
	if err != nil {
		t.Fatalf("failed to create chain store: %v", err)
	}
	cm := chain.NewManager(dbstore, tipState, chain.WithLog(log.Named("chain")))

	syncerListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}

	// peers will reject us if our hostname is empty or unspecified, so use loopback
	s := syncer.New(syncerListener, cm, testutil.NewEphemeralPeerStore(), gateway.Header{
		GenesisID:  genesis.ID(),
		UniqueID:   gateway.GenerateUniqueID(),
		NetAddress: syncerListener.Addr().String(),
	},
		syncer.WithSendBlocksTimeout(2*time.Second),
		syncer.WithRPCTimeout(2*time.Second),
	)
	go s.Run()

	apiOpts := []api.ServerOption{
		api.WithLogger(log.Named("api")),
	}

	// prepare store
	store := initTestDB(t, log)

	password := hex.EncodeToString(frand.Bytes(16))
	web := http.Server{
		Handler: jape.BasicAuth(password)(api.NewServer(cm, s, store, apiOpts...)),
	}

	httpListener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on http address: %v", err)
	}

	go func() {
		if err := web.Serve(httpListener); err != nil && !errors.Is(err, http.ErrServerClosed) {
			log.Error("http server failed", zap.Error(err))
		}
	}()

	return &Indexer{
			Client: api.NewClient(fmt.Sprintf("http://%s", httpListener.Addr().String()), password),
		}, func() {
			if err := closeWithTimeout(shutdownCloser{&web}); err != nil {
				t.Errorf("failed to shutdown webserver: %v", err)
			}
			if err := closeWithTimeout(s); err != nil {
				t.Errorf("failed to close syncer: %v", err)
			}
			if err := closeWithTimeout(store); err != nil {
				t.Errorf("failed to close store: %v", err)
			}
		}
}

func initTestDB(t testing.TB, log *zap.Logger) *postgres.Store {
	// parse connection info from env vars
	ci := postgres.ConnectionInfo{
		Host:     "127.0.0.1",
		Port:     5432,
		User:     os.Getenv("POSTGRES_USER"),
		Password: os.Getenv("POSTGRES_PASSWORD"),
		Database: os.Getenv("POSTGRES_DB"),
		SSLMode:  "disable",
	}

	// create test-specific database
	dbName := t.Name()
	pool, err := pgxpool.New(context.Background(), ci.String())
	if err != nil {
		t.Fatal(err)
	}
	defer pool.Close()

	if _, err := pool.Exec(context.Background(), fmt.Sprintf("DROP DATABASE IF EXISTS %q", dbName)); err != nil {
		t.Fatal(err)
	} else if _, err := pool.Exec(context.Background(), fmt.Sprintf("CREATE DATABASE %q", dbName)); err != nil {
		t.Fatal(err)
	}
	pool.Close()
	ci.Database = dbName

	// connect
	store, err := postgres.Connect(context.Background(), ci, log.Named("postgres"))
	if err != nil {
		t.Fatalf("failed to connect to postgres database: %v", err)
	}
	return store
}

type shutdowner interface {
	Shutdown(context.Context) error
}

type shutdownCloser struct {
	inner shutdowner
}

func (s shutdownCloser) Close() error {
	return s.inner.Shutdown(context.Background())
}

func closeWithTimeout(c io.Closer) error {
	closed := make(chan struct{})
	defer close(closed)

	time.AfterFunc(30*time.Second, func() {
		select {
		case <-closed:
		default:
			panic("indexer ")
		}
	})

	return c.Close()
}
