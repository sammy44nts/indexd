package slabs

import (
	"maps"
	"slices"
	"sync"
)

type (
	// A SectorEvent represents an event related to a sector being pinned or unpinned.
	SectorEvent struct {
		Sector PinnedSector
		Out    bool
	}

	// pinTracker tracks the sectors that are currently pinned and notifies subscribers of changes.
	pinTracker struct {
		mu          sync.Mutex
		subscribers map[chan<- SectorEvent]struct{}
	}
)

// Subscribe adds a new subscriber to the PinTracker. The subscriber will receive updates about pinned sectors.
func (pt *pinTracker) Subscribe(ch chan<- SectorEvent) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	pt.subscribers[ch] = struct{}{}
}

// Unsubscribe removes a subscriber from the PinTracker. The subscriber
// will no longer receive updates about pinned sectors.
func (pt *pinTracker) Unsubscribe(ch chan<- SectorEvent) {
	pt.mu.Lock()
	defer pt.mu.Unlock()
	delete(pt.subscribers, ch)
}

// Notify sends a notification to all subscribers about a sector
// being uploaded or downloaded.
func (pt *pinTracker) Notify(sector PinnedSector, out bool) {
	pt.mu.Lock()
	subscribers := slices.Collect(maps.Keys(pt.subscribers))
	pt.mu.Unlock()
	for _, subscriber := range subscribers {
		subscriber <- SectorEvent{
			Sector: sector,
			Out:    out,
		}
	}
}

// Subscribe adds a new subscriber to receive updates about pinned sectors.
func (sm *SlabManager) Subscribe(ch chan<- SectorEvent) {
	sm.tracker.Subscribe(ch)
}

// Unsubscribe removes a subscriber from receiving updates about pinned sectors.
func (sm *SlabManager) Unsubscribe(ch chan<- SectorEvent) {
	sm.tracker.Unsubscribe(ch)
}
