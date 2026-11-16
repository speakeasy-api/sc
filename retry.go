package sc

import "time"

// Forever is the [Retry.Max] value for a service that runs again each time
// that it fails, with no limit.
const Forever = -1

// Retry is a policy that runs a failed service again. You set it with
// [Invocation.Retry]. The first wait is First. After each failure, the wait
// becomes two times longer, until it is equal to Cap.
type Retry struct {
	// Max is the number of times that the service runs again before its
	// failure goes to the chart. For example, 3 gives a total of four runs.
	// [Forever] sets no limit.
	Max int

	// First is the wait before the first retry.
	First time.Duration

	// Cap is the longest wait. If Cap is zero, each wait is equal to First.
	Cap time.Duration

	// Jitter is the fraction, from 0 to 1, by which the driver changes each
	// wait at random. [Transition.Jitter] does the same for a delayed
	// transition.
	Jitter float64

	// If decides if the service runs again after a failure. If If is nil,
	// the service runs again after each failure. If must be a pure function
	// of the error. A driver that persists events keeps only the message and
	// the [Coder] code of the error. Thus, make the decision with
	// [ErrorCode].
	If func(err error) bool
}

// wait is how long to wait after the failures-th failure, counting from 1.
func (p Retry) wait(failures int) time.Duration {
	if p.Cap <= p.First {
		return p.First
	}

	// Doubling stops at the cap, and before it could overflow.
	wait := p.First
	for range failures - 1 {
		if wait >= p.Cap/2 {
			return p.Cap
		}
		wait *= 2
	}

	return min(wait, p.Cap)
}

// steps is the number of failures after which the wait no longer grows.
func (p Retry) steps() int {
	n := 1
	for p.First > 0 && p.wait(n) < p.Cap {
		n++
	}

	return n
}
