package sc

import (
	"context"
	"errors"

	"golang.org/x/sync/semaphore"
)

// Permit is the value of a resource that you declare with
// [StateBuilder.HoldsPermit]. A Permit is one unit of a shared budget.
type Permit struct{}

// ErrNoPermit is the failure of a permit acquisition if the budget has no
// free unit.
var ErrNoPermit = errors.New("no permit is free")

// HoldsPermit makes the state hold one unit of sem for all of the time that
// the state is active. When the chart enters the state, the state tries to
// take a unit, and does not wait:
//
//   - If it gets a unit, OnAcquired fires.
//   - If no unit is free, OnError fires with [ErrNoPermit].
//
// When the chart exits the state, or when you close the instance, the state
// releases the unit.
//
// If instances share one semaphore, it sets a limit on the number of them
// that can be in the state at the same time. For example, it can be a limit
// for a full pod on the requests in progress, or on the hosts that run. Send
// OnError back to a state that tries again later. Then the state skips the
// work, but [LimitServices] waits.
//
// Declare the permit before the other resources of the state. The driver then
// acquires those resources only after it holds the permit.
func (s *StateBuilder[S, C]) HoldsPermit(name string, sem *semaphore.Weighted) *Resource[S, C, Permit] {
	acquire, release := permitFuncs[S, C](sem)

	return s.Holds(name, acquire, release)
}

// Permit binds the resource with the given name to one unit of sem, as
// [StateBuilder.HoldsPermit] does. You declare that resource with
// HoldsRequired[sc.Permit].
func (b *Binder[S, C]) Permit(name string, sem *semaphore.Weighted) {
	acquire, release := permitFuncs[S, C](sem)
	b.Resource(name, acquire, release)
}

func permitFuncs[S comparable, C any](sem *semaphore.Weighted) (func(context.Context, *Run, C) (Permit, error), func(context.Context, Permit, Leave[S])) {
	acquire := func(context.Context, *Run, C) (Permit, error) {
		if !sem.TryAcquire(1) {
			return Permit{}, ErrNoPermit
		}

		return Permit{}, nil
	}

	release := func(context.Context, Permit, Leave[S]) { sem.Release(1) }

	return acquire, release
}
