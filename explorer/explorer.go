package explorer

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"time"

	"go.sia.tech/core/types"
)

var (
	// ErrDisabled is returned by the API when the explorer is disabled.
	ErrDisabled = errors.New("explorer is disabled")

	client = &http.Client{
		Timeout: 30 * time.Second,
	}
)

// An Explorer retrieves data about the Sia network from an external source.
type Explorer struct {
	url string
}

// New returns a new Explorer client.
func New(url string) *Explorer {
	return &Explorer{url: url}
}

// BaseURL returns the base URL of the Explorer.
func (e *Explorer) BaseURL() string {
	return e.url
}

// AddressCheckpoint returns the chain index at which the given address was
// first seen or the current tip if the address is not found.
func (e *Explorer) AddressCheckpoint(ctx context.Context, address types.Address) (types.ChainIndex, error) {
	url := fmt.Sprintf("%s/addresses/%s/checkpoint", e.url, address.String())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to send request to %q: %w", url, err)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return types.ChainIndex{}, fmt.Errorf("unexpected status code from %q: %d", url, resp.StatusCode)
		}
		return types.ChainIndex{}, errors.New(errorMessage)
	}

	var index types.ChainIndex
	if err := json.NewDecoder(resp.Body).Decode(&index); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to decode response: %w", err)
	}
	return index, nil
}

// TipHeight returns the chain index at the given height.
func (e *Explorer) TipHeight(ctx context.Context, height uint64) (types.ChainIndex, error) {
	url := fmt.Sprintf("%s/consensus/tip/%d", e.url, height)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to send request to %q: %w", url, err)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return types.ChainIndex{}, fmt.Errorf("unexpected status code from %q: %d", url, resp.StatusCode)
		}
		return types.ChainIndex{}, errors.New(errorMessage)
	}

	var index types.ChainIndex
	if err := json.NewDecoder(resp.Body).Decode(&index); err != nil {
		return types.ChainIndex{}, fmt.Errorf("failed to decode response: %w", err)
	}
	return index, nil
}

// SiacoinExchangeRate returns the exchange rate for the given currency.
func (e *Explorer) SiacoinExchangeRate(ctx context.Context, currency string) (float64, error) {
	url := fmt.Sprintf("%s/exchange-rate/siacoin/%s", e.url, currency)
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return 0, fmt.Errorf("failed to create request: %w", err)
	}

	resp, err := client.Do(req)
	if err != nil {
		return 0, fmt.Errorf("failed to send request to %q: %w", url, err)
	}
	defer func() {
		io.Copy(io.Discard, io.LimitReader(resp.Body, 1024*1024))
		resp.Body.Close()
	}()

	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		var errorMessage string
		if err := json.NewDecoder(io.LimitReader(resp.Body, 1024)).Decode(&errorMessage); err != nil {
			return 0, fmt.Errorf("unexpected status code from %q: %d", url, resp.StatusCode)
		}
		return 0, errors.New(errorMessage)
	}

	var rate float64
	if err := json.NewDecoder(resp.Body).Decode(&rate); err != nil {
		return 0, fmt.Errorf("failed to decode response: %w", err)
	}
	return rate, nil
}
