// Package fanout delivers one stream of values to many independent readers.
//
// TWO READERS OF THE SAME STREAM WANT DIFFERENT THINGS, and this package is
// where that is decided once rather than twice:
//
//   - The admin UI's live tail must NEVER BLOCK THE PIPELINE IT IS WATCHING. A
//     browser tab on a slow connection is a slow reader, and a slow reader that
//     can apply backpressure to the tail would make the act of observing the
//     lab change the thing the lab measures. So a subscriber that cannot keep
//     up DROPS, and its drops are counted and reported — a silent drop is a
//     graph that lies.
//   - The control watcher must NEVER DROP. There are only a handful of settings
//     records in the whole life of the lab and missing one means a service runs
//     at a rate nobody asked for.
//
// Broadcast serves the first case. The second is served by holding the latest
// value rather than by queueing, which is what Latest is for: a compacted topic
// has no backlog worth replaying, so "the newest record wins" is both correct
// and unbounded-buffer-free.
package fanout

import "sync"

// Broadcast fans a stream out to subscribers, dropping for any subscriber whose
// buffer is full.
type Broadcast[T any] struct {
	mu      sync.Mutex
	next    int
	subs    map[int]chan T
	buf     int
	dropped uint64
	sent    uint64
	closed  bool
}

// NewBroadcast returns a Broadcast giving each subscriber a buffer of buf
// values. A buf below one is raised to one: a zero-buffered subscriber would
// drop everything it did not happen to be blocked on, which is not a slow
// reader policy, it is a coin flip.
func NewBroadcast[T any](buf int) *Broadcast[T] {
	if buf < 1 {
		buf = 1
	}
	return &Broadcast[T]{subs: make(map[int]chan T), buf: buf}
}

// Subscribe returns a channel of values and a function that unsubscribes it.
// The cancel function is idempotent and closes the channel, so a ranging reader
// terminates.
//
// Subscribing to a closed Broadcast returns an already-closed channel rather
// than an error: a browser that connects during shutdown should see the stream
// end, not a 500.
func (b *Broadcast[T]) Subscribe() (<-chan T, func()) {
	b.mu.Lock()
	defer b.mu.Unlock()

	if b.closed {
		ch := make(chan T)
		close(ch)
		return ch, func() {}
	}

	id := b.next
	b.next++
	ch := make(chan T, b.buf)
	b.subs[id] = ch

	var once sync.Once
	return ch, func() {
		once.Do(func() {
			b.mu.Lock()
			defer b.mu.Unlock()
			if c, ok := b.subs[id]; ok {
				delete(b.subs, id)
				close(c)
			}
		})
	}
}

// Send offers v to every subscriber, dropping for any whose buffer is full.
// It never blocks, which is the whole contract.
func (b *Broadcast[T]) Send(v T) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	for _, ch := range b.subs {
		select {
		case ch <- v:
			b.sent++
		default:
			b.dropped++
		}
	}
}

// Stats reports deliveries and drops since construction, and the current
// subscriber count. Drops are surfaced in the admin UI because an observer that
// silently loses messages is worse than one that admits it.
func (b *Broadcast[T]) Stats() (sent, dropped uint64, subscribers int) {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.sent, b.dropped, len(b.subs)
}

// Close ends every subscription. It is idempotent.
func (b *Broadcast[T]) Close() {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.closed {
		return
	}
	b.closed = true
	for id, ch := range b.subs {
		delete(b.subs, id)
		close(ch)
	}
}

// Latest holds one value and wakes waiters when it changes. It is the shape a
// COMPACTED topic wants: there is no backlog to replay, so a reader that fell
// behind should jump to the current state rather than walk history.
type Latest[T any] struct {
	mu      sync.Mutex
	v       T
	set     bool
	changed chan struct{}
}

// NewLatest returns a Latest holding v.
func NewLatest[T any](v T) *Latest[T] {
	return &Latest[T]{v: v, set: false, changed: make(chan struct{})}
}

// Get returns the current value and whether anything has ever been stored.
// The flag matters at boot: "nobody has published settings yet" and "somebody
// published exactly the defaults" are different facts, and the admin UI says so.
func (l *Latest[T]) Get() (T, bool) {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.v, l.set
}

// Set stores v and wakes every waiter.
func (l *Latest[T]) Set(v T) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.v = v
	l.set = true
	close(l.changed)
	l.changed = make(chan struct{})
}

// Changed returns a channel closed on the next Set. It is re-read each time
// round a wait loop; holding one across two Sets misses the second.
func (l *Latest[T]) Changed() <-chan struct{} {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.changed
}
