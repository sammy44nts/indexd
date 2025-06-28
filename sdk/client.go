package sdk

import (
	"context"
	"errors"
	"fmt"
	"io"
	"sync"
	"sync/atomic"
	"time"

	"github.com/klauspost/reedsolomon"
	proto4 "go.sia.tech/core/rhp/v4"
	"go.sia.tech/core/types"
	"go.sia.tech/indexd/api"
	"golang.org/x/crypto/chacha20"
	"lukechampine.com/frand"
)

type (
	uploadOption struct {
		dataShards   uint8
		parityShards uint8
		hostTimeout  time.Duration
		maxInflight  int
	}

	downloadOption struct {
		hostTimeout time.Duration
		maxInflight int
	}

	slabUploadWork struct {
		shards        [][]byte
		length        uint32
		encryptionKey [32]byte
		err           error
	}

	// A HostDialer is an interface for writing and reading sectors to/from hosts.
	HostDialer interface {
		// Hosts returns the public keys of all hosts that are available for
		// upload or download.
		Hosts() []types.PublicKey

		// WriteSector writes a sector to the host identified by the public key.
		WriteSector(context.Context, types.PublicKey, *[proto4.SectorSize]byte) (types.Hash256, error)
		// ReadSector reads a sector from the host identified by the public key.
		ReadSector(ctx context.Context, hostKey types.PublicKey, root types.Hash256, offset, length uint64) ([]byte, error)
	}

	// An UploadOption configures the upload behavior
	UploadOption func(*uploadOption)

	// A DownloadOption configures the download behavior
	DownloadOption func(*downloadOption)

	// A Shard represents a sector in a slab
	Shard struct {
		Root    types.Hash256
		HostKey types.PublicKey
	}

	// A Slab represents a collection of erasure-coded sectors
	Slab struct {
		SectorKey [32]byte
		MinShards uint8
		Offset    uint32
		Length    uint32
		Shards    []Shard
	}

	// An Object represents a collection of slabs that are associated with a
	// specific key.
	Object struct {
		Key   string
		Slabs []Slab
	}

	// An SDK is a client for the indexd service.
	SDK struct {
		appKey types.PrivateKey
		c      api.Client

		dialer HostDialer
	}
)

var (
	// ErrNotEnoughShards is returned when not enough shards were
	// uploaded or downloaded to satisfy the minimum required shards.
	ErrNotEnoughShards = errors.New("not enough shards")

	// ErrNoMoreHosts is returned when there are no more hosts
	// available to attempt to upload a shard
	ErrNoMoreHosts = errors.New("no more hosts available")
)

func (s *SDK) uploadSlab(ctx context.Context, encryptionKey [32]byte, shards [][]byte, dataShards uint8, maxInFlight int, timeout time.Duration) (Slab, error) {
	if len(shards) == 0 {
		return Slab{}, errors.New("no shards to upload")
	} else if len(shards) < int(dataShards) {
		return Slab{}, fmt.Errorf("not enough shards to upload: %d, required: %d", len(shards), dataShards)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	slab := Slab{
		SectorKey: encryptionKey,
		MinShards: dataShards,
		Shards:    make([]Shard, len(shards)),
	}

	var hostsMu sync.Mutex
	hosts := shuffle(s.dialer.Hosts())
	if len(hosts) < len(shards) {
		return Slab{}, fmt.Errorf("not enough hosts available: %d, required: %d", len(hosts), len(shards))
	}

	errCh := make(chan error, len(shards))
	nonce := make([]byte, 24)
	sema := make(chan struct{}, maxInFlight)
	for i := range shards {
		// encrypt the shard before upload
		nonce[0] = byte(i)
		c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
		c.XORKeyStream(shards[i], shards[i])

		select {
		case <-ctx.Done():
			return Slab{}, ctx.Err()
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}

		go func(ctx context.Context, shard []byte, index int) {
			defer func() { <-sema }() // release semaphore
			sector := (*[proto4.SectorSize]byte)(shard)
			for {
				select {
				case <-ctx.Done():
					return
				default:
				}

				hostsMu.Lock()
				if len(hosts) == 0 {
					errCh <- ErrNoMoreHosts
					hostsMu.Unlock()
					return
				}
				hostKey := hosts[0]
				hosts = hosts[1:]
				hostsMu.Unlock()

				root, err := uploadShard(ctx, sector, hostKey, s.dialer, timeout) // error can be ignored, hosts will be retried until none are left and the upload fails.
				if err == nil {
					slab.Shards[index] = Shard{
						HostKey: hostKey,
						Root:    root,
					}
					errCh <- nil
					break
				}
			}
		}(ctx, shards[i], i)
	}
	for range len(shards) {
		select {
		case <-ctx.Done():
			return Slab{}, ctx.Err()
		case err := <-errCh:
			if err != nil {
				return Slab{}, fmt.Errorf("failed to upload shard: %w", err)
			}
		}
	}
	return slab, nil
}

func (s *SDK) downloadSlab(ctx context.Context, slab Slab, maxInflight int, timeout time.Duration) ([][]byte, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	offset, length := sectorRegion(slab.MinShards, slab.Offset, slab.Length)

	var successful atomic.Uint32
	var wg sync.WaitGroup
	shards := make([][]byte, len(slab.Shards))
	sema := make(chan struct{}, maxInflight)
top:
	for i, shard := range slab.Shards {
		select {
		case <-ctx.Done():
			break top
		case sema <- struct{}{}:
			// limit number of concurrent requests
		}
		wg.Add(1)
		go func(ctx context.Context, shard Shard, offset, length uint64, i int) {
			defer func() { <-sema }() // release semaphore
			defer wg.Done()
			data, err := downloadShard(ctx, shard.Root, shard.HostKey, s.dialer, offset, length, timeout)
			if err != nil {
				return
			} else if len(data) != int(length) {
				return
			}
			shards[i] = data
			if v := successful.Add(1); v >= uint32(slab.MinShards) {
				// got enough pieces to recover
				cancel()
			}
		}(ctx, shard, offset, length, i)
	}

	wg.Wait()
	if n := successful.Load(); n < uint32(slab.MinShards) {
		return nil, fmt.Errorf("retrieved %d shards, minimum required: %d: %w", n, slab.MinShards, ErrNotEnoughShards)
	}
	return shards, nil
}

// Upload uploads the data to hosts and pins it to the indexer.
//
// Returns the metadata of the slabs that were pinned
func (s *SDK) Upload(ctx context.Context, r io.Reader, opts ...UploadOption) ([]Slab, error) {
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()

	uo := uploadOption{
		dataShards:   10,
		parityShards: 20,
		hostTimeout:  4 * time.Second, // ~10 Mbps
		maxInflight:  30,
	}
	for _, opt := range opts {
		opt(&uo)
	}

	if (uo.parityShards+uo.dataShards)/uo.dataShards < 2 {
		return nil, errors.New("redundancy must be at least 2x")
	}

	enc, err := reedsolomon.New(int(uo.dataShards), int(uo.parityShards))
	if err != nil {
		return nil, fmt.Errorf("failed to create encoder: %w", err)
	}

	slabCh := make(chan slabUploadWork, 1)
	go processSlabs(ctx, enc, uo, r, s.appKey, slabCh)

	// TODO: cleanup on failure
	var pinned []Slab
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case work := <-slabCh:
			err := work.err
			shards := work.shards
			shardKey := work.encryptionKey

			if errors.Is(err, io.EOF) {
				// no more slabs to upload, return the pinned slabs
				return pinned, nil
			} else if work.err != nil {
				return nil, work.err
			}
			slab, err := s.uploadSlab(ctx, shardKey, shards, uo.dataShards, uo.maxInflight, uo.hostTimeout)
			if err != nil {
				return nil, fmt.Errorf("failed to upload slab %d: %w", i, err)
			}
			slab.Length = uint32(work.length)
			// TODO: pin slab
			pinned = append(pinned, slab)
		}
	}
}

// Download downloads object metadata
//
// TODO: support seeks
func (s *SDK) Download(ctx context.Context, w io.Writer, metadata []Slab, opts ...DownloadOption) error {
	if len(metadata) == 0 {
		return errors.New("no slabs to download")
	}

	dataShards := int(metadata[0].MinShards)
	parityShards := len(metadata[0].Shards) - dataShards

	do := downloadOption{
		hostTimeout: 4 * time.Second, // ~10 Mbps
		maxInflight: 10,
	}
	for _, opt := range opts {
		opt(&do)
	}

	enc, err := reedsolomon.New(dataShards, parityShards)
	if err != nil {
		return fmt.Errorf("failed to create encoder: %w", err)
	}

	type work struct {
		shards [][]byte
		err    error
	}
	workCh := make(chan work, 1)
	go func(ctx context.Context, slabs []Slab) {
		for i, slab := range metadata {
			shards, err := s.downloadSlab(ctx, slab, do.maxInflight, do.hostTimeout)
			if err != nil {
				workCh <- work{err: fmt.Errorf("failed to download slab %d: %w", i, err)}
				return
			}
			workCh <- work{shards: shards}
		}
		workCh <- work{err: io.EOF}
	}(ctx, metadata)

	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case work := <-workCh:
			err := work.err
			if errors.Is(err, io.EOF) {
				// EOF signals completion
				return nil
			} else if err != nil {
				return err
			}
			slab := metadata[i]
			encryptionKey := slab.SectorKey
			shards := work.shards
			nonce := make([]byte, 24)
			for i := range shards {
				nonce[0] = byte(i)
				c, _ := chacha20.NewUnauthenticatedCipher(encryptionKey[:], nonce)
				c.XORKeyStream(shards[i], shards[i]) // decrypt shard in place
			}
			enc.ReconstructData(shards)
			if err := stripedJoin(w, shards[:slab.MinShards], int(slab.Length)); err != nil {
				return fmt.Errorf("failed to write slab %d: %w", i, err)
			}
		}
	}
}

// stripedJoin joins the striped data shards, writing them to dst. The first 'skip'
// bytes of the recovered data are skipped, and 'writeLen' bytes are written in
// total.
func stripedJoin(dst io.Writer, dataShards [][]byte, writeLen int) error {
	for off := 0; writeLen > 0; off += proto4.LeafSize {
		for _, shard := range dataShards {
			if len(shard[off:]) < proto4.LeafSize {
				return reedsolomon.ErrShortData
			}
			shard = shard[off:][:proto4.LeafSize]
			if writeLen < len(shard) {
				shard = shard[:writeLen]
			}
			n, err := dst.Write(shard)
			if err != nil {
				return err
			}
			writeLen -= n
		}
	}
	return nil
}

// stripedSplit reads the shards of a slab from the given reader and stripes
// it across the provided shards. The shards must have been allocated with
// `make([][]byte, numShards)` and each shard must have a length of
// `proto4.SectorSize` bytes. The function returns the total number of bytes
// read, which is the sum of the lengths of all shards.
func stripedSplit(r io.Reader, shards [][]byte) (length uint32, err error) {
	for i := 0; i < proto4.SectorSize; i += proto4.LeafSize {
		for j := range shards {
			n, err := io.ReadFull(r, shards[j][i:i+proto4.LeafSize])
			if errors.Is(err, io.EOF) {
				return length, nil // no more data to read
			} else if err != nil && !errors.Is(err, io.ErrUnexpectedEOF) {
				return 0, fmt.Errorf("failed to read shard %d: %w", i, err)
			}
			length += uint32(n)
		}
	}
	return length, nil
}

func sectorRegion(minShards uint8, so, sl uint32) (offset, length uint64) {
	minChunkSize := proto4.LeafSize * uint32(minShards)
	start := (so / minChunkSize) * proto4.LeafSize
	end := ((so + sl) / minChunkSize) * proto4.LeafSize
	if (so+sl)%minChunkSize != 0 {
		end += proto4.LeafSize
	}
	return uint64(start), uint64(end - start)
}

// downloadShard reads a sector from a host
func downloadShard(ctx context.Context, root types.Hash256, hostKey types.PublicKey, dialer HostDialer, offset, length uint64, timeout time.Duration) ([]byte, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	return dialer.ReadSector(ctx, hostKey, root, offset, length)
}

// uploadShard uploads a shard to a host
func uploadShard(ctx context.Context, sector *[proto4.SectorSize]byte, hostKey types.PublicKey, dialer HostDialer, timeout time.Duration) (types.Hash256, error) {
	ctx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	rootCh := make(chan types.Hash256, 1)
	go func() {
		root := proto4.SectorRoot(sector)
		rootCh <- root
	}()

	uploaded, err := dialer.WriteSector(ctx, hostKey, sector)
	if err != nil {
		return types.Hash256{}, fmt.Errorf("failed to upload shard to host %s: %w", hostKey.String(), err)
	} else if root := <-rootCh; uploaded != root {
		return types.Hash256{}, fmt.Errorf("uploaded shard root %s does not match expected root %s", uploaded.String(), root.String())
	}
	return uploaded, nil
}

// processSlabs reads slabs from the provided reader, encodes them using the
// provided Reed-Solomon encoder, and sends the encoded shards to the provided
// channel. Slabs are processed in a loop until EOF is reached.

// The encryption key is derived for each slab by hashing the app key slab's raw data.
func processSlabs(ctx context.Context, enc reedsolomon.Encoder, uo uploadOption, r io.Reader, appKey types.PrivateKey, ch chan<- slabUploadWork) {
	h := types.NewHasher()
	for i := 0; ; i++ {
		select {
		case <-ctx.Done():
			return
		default:
		}
		h.Reset()
		h.E.Write(appKey[:]) // write app key to hasher
		shards := make([][]byte, uo.dataShards+uo.parityShards)
		for i := range shards {
			shards[i] = make([]byte, proto4.SectorSize)
		}
		tr := io.TeeReader(r, h.E)
		n, err := stripedSplit(tr, shards[:uo.dataShards])
		if err != nil {
			ch <- slabUploadWork{err: fmt.Errorf("failed to read slab %d: %w", i, err)}
			return
		} else if n == 0 {
			ch <- slabUploadWork{err: io.EOF} // signal done with EOF
			return
		}

		if err := enc.Encode(shards); err != nil {
			ch <- slabUploadWork{err: fmt.Errorf("failed to encode slab %d shards: %w", i, err)}
			return
		}
		encryptionKey := h.Sum()
		ch <- slabUploadWork{length: n, encryptionKey: encryptionKey, shards: shards}
	}
}

// shuffle shuffles the elements of a slice in place and returns it.
func shuffle[T any, S ~[]T](s S) S {
	frand.Shuffle(len(s), func(i, j int) {
		s[i], s[j] = s[j], s[i]
	})
	return s
}

// WithRedundancy sets the number of data and parity shards for the upload.
// The number of shards must be at least 2x redundancy:
// `(dataShards + parityShards) / dataShards >= 2`.
func WithRedundancy(dataShards, parityShards uint8) UploadOption {
	return func(uo *uploadOption) {
		uo.dataShards = dataShards
		uo.parityShards = parityShards
	}
}

// WithUploadHostTimeout sets the timeout for writing sectors to individual
// hosts. This avoids long hangs when a host is unresponsive or slow.
// The default timeout is 4 seconds, worst case around 300Mbps.
func WithUploadHostTimeout(timeout time.Duration) UploadOption {
	return func(uo *uploadOption) {
		uo.hostTimeout = timeout
	}
}

// WithUploadInflight sets the maximum number of concurrent shard uploads.
// This is useful to reduce bandwidth consumption, but will decrease
// performance.
func WithUploadInflight(maxInflight int) UploadOption {
	return func(uo *uploadOption) {
		uo.maxInflight = maxInflight
	}
}

// WithDownloadHostTimeout sets the timeout for reading sectors
// from individual hosts. This avoids long hangs when a host is unresponsive
// or slow. The default is 4 seconds, worst case around 300Mbps.
func WithDownloadHostTimeout(timeout time.Duration) DownloadOption {
	return func(do *downloadOption) {
		do.hostTimeout = timeout
	}
}

// WithDownloadInflight sets the maximum number of concurrent shard
// downloads. This is useful to reduce bandwidth waste, but may
// decrease performance.
func WithDownloadInflight(maxInflight int) DownloadOption {
	return func(do *downloadOption) {
		do.maxInflight = maxInflight
	}
}

// NewSDK creates a new indexd client with the given app key and base URL.
func NewSDK(baseURL string, appKey types.PrivateKey, dialer HostDialer) *SDK {
	return &SDK{
		appKey: appKey,
		dialer: dialer,
		c:      *api.NewClient(baseURL, ""),
	}
}
