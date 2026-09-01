package fanout

import (
	"sync"
	"testing"
	"time"
)

func TestBroadcastDeliversToEverySubscriber(t *testing.T) {
	b := NewBroadcast[int](4)
	c1, stop1 := b.Subscribe()
	c2, stop2 := b.Subscribe()
	defer stop1()
	defer stop2()

	b.Send(7)
	for i, ch := range []<-chan int{c1, c2} {
		select {
		case got := <-ch:
			if got != 7 {
				t.Fatalf("subscriber %d: got %d want 7", i, got)
			}
		case <-time.After(time.Second):
			t.Fatalf("subscriber %d never received", i)
		}
	}
}

// THE CONTRACT. A browser tab on a slow connection must not be able to apply
// backpressure to the pipeline it is watching, so a full buffer drops.
func TestBroadcastDropsForASlowSubscriberAndNeverBlocks(t *testing.T) {
	b := NewBroadcast[int](2)
	ch, stop := b.Subscribe()
	defer stop()

	done := make(chan struct{})
	go func() {
		for i := 0; i < 1000; i++ {
			b.Send(i)
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(5 * time.Second):
		t.Fatal("Send blocked on a slow subscriber")
	}

	sent, dropped, subs := b.Stats()
	if subs != 1 {
		t.Fatalf("subscribers: got %d want 1", subs)
	}
	if sent != 2 {
		t.Fatalf("a buffer of 2 must accept exactly 2: got %d", sent)
	}
	if dropped != 998 {
		t.Fatalf("dropped: got %d want 998", dropped)
	}
	if len(ch) != 2 {
		t.Fatalf("buffer holds %d", len(ch))
	}
}

// A zero-buffered subscriber would drop everything it did not happen to be
// blocked on, which is a coin flip rather than a policy.
func TestNewBroadcastRaisesASubOneBuffer(t *testing.T) {
	for _, buf := range []int{0, -1} {
		b := NewBroadcast[int](buf)
		ch, stop := b.Subscribe()
		defer stop()
		b.Send(1)
		if len(ch) != 1 {
			t.Fatalf("buf %d: a lone send must land, buffer holds %d", buf, len(ch))
		}
	}
}

func TestUnsubscribeClosesTheChannelAndIsIdempotent(t *testing.T) {
	b := NewBroadcast[int](1)
	ch, stop := b.Subscribe()
	stop()
	stop() // must not panic on a double close
	select {
	case _, open := <-ch:
		if open {
			t.Fatal("channel delivered a value after unsubscribe")
		}
	case <-time.After(time.Second):
		t.Fatal("unsubscribe must close the channel so a ranging reader ends")
	}
	if _, _, subs := b.Stats(); subs != 0 {
		t.Fatalf("subscribers after unsubscribe: %d", subs)
	}
	b.Send(1) // must not panic writing to a removed subscriber
}

func TestCloseEndsEverySubscriptionAndIsIdempotent(t *testing.T) {
	b := NewBroadcast[int](1)
	c1, _ := b.Subscribe()
	c2, _ := b.Subscribe()
	b.Close()
	b.Close()
	for i, ch := range []<-chan int{c1, c2} {
		if _, open := <-ch; open {
			t.Fatalf("subscriber %d still open after Close", i)
		}
	}
	b.Send(1)
	if sent, _, subs := b.Stats(); sent != 0 || subs != 0 {
		t.Fatalf("Send after Close delivered %d to %d subscribers", sent, subs)
	}
}

// A browser connecting during shutdown should see the stream END, not an error.
func TestSubscribeAfterCloseReturnsAClosedChannel(t *testing.T) {
	b := NewBroadcast[int](1)
	b.Close()
	ch, stop := b.Subscribe()
	stop()
	if _, open := <-ch; open {
		t.Fatal("want an already-closed channel")
	}
}

func TestBroadcastIsRaceFree(t *testing.T) {
	b := NewBroadcast[int](8)
	var wg sync.WaitGroup
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				b.Send(j)
			}
		}()
	}
	for i := 0; i < 4; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			ch, stop := b.Subscribe()
			for j := 0; j < 20; j++ {
				select {
				case <-ch:
				case <-time.After(time.Millisecond):
				}
			}
			stop()
		}()
	}
	wg.Wait()
	b.Close()
}

func TestLatestStartsUnset(t *testing.T) {
	l := NewLatest(42)
	v, ok := l.Get()
	if v != 42 {
		t.Fatalf("value: got %d want 42", v)
	}
	// "Nobody has published yet" and "somebody published the defaults" are
	// different facts, and the admin UI says so.
	if ok {
		t.Fatal("a constructed Latest must report that nothing has been stored")
	}
}

func TestLatestSetStoresAndMarks(t *testing.T) {
	l := NewLatest(0)
	l.Set(9)
	v, ok := l.Get()
	if v != 9 || !ok {
		t.Fatalf("got (%d,%v) want (9,true)", v, ok)
	}
}

func TestLatestChangedFiresOnSet(t *testing.T) {
	l := NewLatest(0)
	ch := l.Changed()
	select {
	case <-ch:
		t.Fatal("Changed fired before any Set")
	default:
	}
	l.Set(1)
	select {
	case <-ch:
	case <-time.After(time.Second):
		t.Fatal("Set must wake waiters")
	}
}

// Holding one Changed channel across two Sets misses the second; the loop must
// re-read it. This pins that a fresh channel is installed each time.
func TestLatestInstallsAFreshChangedChannel(t *testing.T) {
	l := NewLatest(0)
	first := l.Changed()
	l.Set(1)
	second := l.Changed()
	if first == second {
		t.Fatal("Set must install a fresh Changed channel")
	}
	select {
	case <-second:
		t.Fatal("the fresh channel must not already be closed")
	default:
	}
	l.Set(2)
	select {
	case <-second:
	case <-time.After(time.Second):
		t.Fatal("the second Set must wake the second channel")
	}
}

func TestLatestIsRaceFree(t *testing.T) {
	l := NewLatest(0)
	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func(n int) {
			defer wg.Done()
			for j := 0; j < 200; j++ {
				l.Set(n)
				l.Get()
				l.Changed()
			}
		}(i)
	}
	wg.Wait()
}
