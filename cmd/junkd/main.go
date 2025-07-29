package main

import (
	"context"
	"crypto/pbkdf2"
	"crypto/sha256"
	"flag"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	proto "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/coreutils/wallet"
	"go.sia.tech/indexd/api/admin"
	"go.sia.tech/indexd/keys"
	"go.sia.tech/indexd/sdk"
	"go.uber.org/zap"
	"lukechampine.com/frand"
)

const (
	dataShards        = 2
	parityShards      = 4
	slabSize          = dataShards * proto.SectorSize
	redundantSlabSize = (dataShards + parityShards) * proto.SectorSize
	redundancy        = (dataShards + parityShards) / dataShards
)

var (
	adminURL      string
	adminPassword string

	appSecret string
	appURL    string

	logLevel string
	logPath  string

	threads int

	clientMu    sync.Mutex
	client      *sdk.SDK
	clientUntil time.Time

	elapsedMu sync.Mutex
	elapsed   []time.Duration
)

func init() {
	flag.StringVar(&adminURL, "admin.url", "http://localhost:9980/api", "the URL of the admin API")
	flag.StringVar(&adminPassword, "admin.password", "", "the password for the admin API")

	flag.StringVar(&appSecret, "app.secret", "", "a secret used to derive the application key")
	flag.StringVar(&appURL, "app.url", "http://localhost:9880", "the URL of the application API")

	flag.StringVar(&logLevel, "log.level", "info", "the log level to use")
	flag.StringVar(&logPath, "log.path", "", "the path to write the log to")

	flag.IntVar(&threads, "threads", 1, "the number of upload threads")

	flag.Parse()
}

func main() {
	log, err := newLogger()
	if err != nil {
		log.Fatal("failed to build logger", zap.Error(err))
	}

	sk, err := loadPrivateKey()
	if err != nil {
		log.Fatal("failed to load private key", zap.Error(err))
	}

	ctx, cancel := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer cancel()

	var wg sync.WaitGroup
	for n := 1; n <= threads; n++ {
		wg.Add(1)
		go func(log *zap.Logger) {
			defer wg.Done()
			log.Debug("starting upload thread")

		loop:
			for {
				// prepare client (refresh the app key daily)
				client, err := loadClient(ctx, sk)
				if err != nil {
					log.Error("failed to load client, timing out for 5 minutes", zap.Error(err))
					if ok := <-waitFor(ctx, 5*time.Minute); ok {
						continue loop
					}
					break loop
				}

				// upload slab
				start := time.Now()
				slabs, err := client.Upload(ctx, io.LimitReader(frand.Reader, slabSize), sdk.WithRedundancy(dataShards, parityShards))
				if err != nil {
					log.Error("failed to upload slab, timing out for 5 minutes", zap.Error(err), zap.Duration("duration", time.Since(start)))
					if ok := <-waitFor(ctx, 5*time.Minute); ok {
						continue loop
					}
					break loop
				} else if len(slabs) != 1 {
					log.Error(fmt.Sprintf("expected 1 slab, got %d", len(slabs)))
					break loop
				}

				elapsedMu.Lock()
				elapsed = append(elapsed, time.Since(start))
				elapsedMu.Unlock()

				log.Info("upload completed", zap.Stringer("SlabID", slabs[0].ID), zap.Duration("duration", time.Since(start)), zap.String("speed", formatBpsString(redundantSlabSize, time.Since(start))))
			}
		}(log.Named(fmt.Sprintf("upload-thread-%d", n)))
	}
	go printUploadSpeeds(ctx, log)
	wg.Wait()

	log.Info("all upload threads finished, exiting")
}

func waitFor(ctx context.Context, d time.Duration) <-chan bool {
	c := make(chan bool, 1)
	go func() {
		select {
		case <-ctx.Done():
			c <- false
		case <-time.After(d):
			c <- true
		}
	}()
	return c
}

func loadClient(ctx context.Context, masterKey types.PrivateKey) (*sdk.SDK, error) {
	clientMu.Lock()
	defer clientMu.Unlock()

	// return cached client
	if time.Now().Before(clientUntil) {
		return client, nil
	}

	// register application key
	appKey := keys.DeriveKey(masterKey, fmt.Sprintf("junkd-%s", time.Now().Format(time.DateOnly)))
	err := admin.NewClient(adminURL, adminPassword).AccountsAdd(ctx, appKey.PublicKey())
	if err != nil {
		return nil, fmt.Errorf("failed to add app key to admin API: %w", err)
	}

	// create new client
	clientUntil = time.Now().AddDate(0, 0, 1)
	client, err = sdk.NewSDK(appURL, appKey)
	if err != nil {
		return nil, fmt.Errorf("failed to create SDK: %w", err)
	}

	return client, nil
}

func loadPrivateKey() (types.PrivateKey, error) {
	if appSecret == "" {
		return types.PrivateKey{}, fmt.Errorf("app secret is required")
	}

	derived, err := pbkdf2.Key(sha256.New, appSecret, []byte("junkd-pk-salt"), 4096, 32)
	if err != nil {
		return types.PrivateKey{}, fmt.Errorf("failed to derive key: %w", err)
	}

	var seed [32]byte
	copy(seed[:], derived)
	return wallet.KeyFromSeed(&seed, 0), nil
}

func newLogger() (*zap.Logger, error) {
	cfg := zap.NewProductionConfig()
	switch logLevel {
	case "debug":
		cfg.Level = zap.NewAtomicLevelAt(zap.DebugLevel)
	case "info":
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	case "warn":
		cfg.Level = zap.NewAtomicLevelAt(zap.WarnLevel)
	default:
		cfg.Level = zap.NewAtomicLevelAt(zap.InfoLevel)
	}

	cfg.OutputPaths = []string{"stdout"}
	if logPath != "" {
		cfg.OutputPaths = append(cfg.OutputPaths, logPath)
	}
	cfg.DisableStacktrace = true

	return cfg.Build()
}

func formatBpsString(b int64, t time.Duration) string {
	const units = "KMGTPE"
	const factor = 1000

	time := t.Truncate(time.Second).Seconds()
	if time <= 0 {
		return "0.00 bps"
	}

	// calculate bps
	speed := float64(b*8) / time

	// short-circuit for < 1000 bits/s
	if speed < factor {
		return fmt.Sprintf("%.2f bps", speed)
	}

	var i = -1
	for ; speed >= factor; i++ {
		speed /= factor
	}
	return fmt.Sprintf("%.2f %cbps", speed, units[i])
}

func printUploadSpeeds(ctx context.Context, log *zap.Logger) {
	t := time.NewTicker(2 * time.Minute)
	defer t.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			elapsedMu.Lock()
			if len(elapsed) > 1000 {
				elapsed = elapsed[len(elapsed)-1000:]
			}
			times := elapsed
			elapsedMu.Unlock()

			var avg time.Duration
			if len(times) == 0 {
				avg = time.Second
			} else {
				for _, t := range times {
					avg += t
				}
				avg /= time.Duration(len(times))
			}
			log.Info("average upload time", zap.String("averageSpeed", formatBpsString(int64(redundantSlabSize), avg)))
		}
	}
}
