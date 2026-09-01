package ratelimit

import "time"

// timer exists as an interface so the wait path can be driven without real
// sleeping in tests. Production is a time.Timer and nothing else.
type timer interface {
	C() <-chan time.Time
	Stop()
}

type realTimer struct{ t *time.Timer }

func (r realTimer) C() <-chan time.Time { return r.t.C }
func (r realTimer) Stop()               { r.t.Stop() }

// newTimer is a variable so a test can substitute a channel it controls.
var newTimer = func(d time.Duration) timer { return realTimer{t: time.NewTimer(d)} }
