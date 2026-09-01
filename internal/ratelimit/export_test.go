package ratelimit

import "golang.org/x/time/rate"

// MaxRateForTest is fast enough that a token is always already available, which
// is what makes the cancelled-context ordering test meaningful rather than a
// coin flip.
const MaxRateForTest = 1e6

// newZeroBurstLimiter builds the one shape whose Reserve reports !OK. New never
// constructs it; the test reaches past New on purpose.
func newZeroBurstLimiter() *rate.Limiter { return rate.NewLimiter(rate.Limit(1), 0) }
