package topology

import (
	"sync"
)

// Broadcast notifies Watch subscribers when topology_version changes.
type Broadcast struct {
	mu      sync.RWMutex
	version int64
	subs    map[chan int64]struct{}
}

func NewBroadcast() *Broadcast {
	return &Broadcast{subs: make(map[chan int64]struct{})}
}

func (b *Broadcast) Notify(version int64) {
	b.mu.Lock()
	b.version = version
	for ch := range b.subs {
		select {
		case ch <- version:
		default:
		}
	}
	b.mu.Unlock()
}

func (b *Broadcast) Subscribe() (ch <-chan int64, current int64, unsubscribe func()) {
	c := make(chan int64, 1)
	b.mu.Lock()
	b.subs[c] = struct{}{}
	cur := b.version
	b.mu.Unlock()
	return c, cur, func() {
		b.mu.Lock()
		delete(b.subs, c)
		close(c)
		b.mu.Unlock()
	}
}
