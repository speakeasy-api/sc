package sc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"slices"
	"sync"
)

// ErrNoInstance is the error that [Group.Send] returns for a key that has
// no instance in the group.
var ErrNoInstance = errors.New("no instance for key")

// Group holds many [Instance] values of one machine, with a key of type K.
// For example, a group can have one instance for each host, grant, or
// connection slot. A Group does these tasks:
//   - It makes instances when the caller asks.
//   - It routes events to the instances by key.
//   - It reads across all the instances.
//   - It closes and forgets the instances that the caller does not need.
//
// You can use a Group from many goroutines at the same time.
//
// A group does not decide when an instance must exist. If the caller sets a
// limit on the number of instances, the caller examines [Group.Count]
// before it calls [Group.Start]. The caller controls the two calls. Thus a
// poll loop that owns the group can use the two calls as one decision.
type Group[K comparable, S comparable, C any] struct {
	m    *Machine[S, C]
	opts []RunOption

	mu sync.Mutex

	// instances holds the group's instances by key, guarded by mu.
	instances map[K]*Instance[S, C]

	// starting holds, for each key whose instance is being created, a
	// channel closed when that ends, guarded by mu.
	starting map[K]chan struct{}
}

// NewGroup returns an empty group. The instances of the group run m with
// opts, for example shared hooks or a shared [LimitServices] semaphore.
func NewGroup[K comparable, S comparable, C any](m *Machine[S, C], opts ...RunOption) *Group[K, S, C] {
	return &Group[K, S, C]{m: m, opts: opts, mu: sync.Mutex{}, instances: map[K]*Instance[S, C]{}, starting: map[K]chan struct{}{}}
}

// Start returns the instance for key. If the group has no instance for key,
// Start makes one with context c. The bool result is true if Start made an
// instance. If Start returns an error, the bool result is false.
//
// To make an instance, Start does the effects of the entry to its initial
// states. The actions in these effects use ctx.
//
// The group is not locked while Start makes the instance. Thus a slow
// initial action stops only the callers that start the same key. These
// callers wait for the instance and then share it. ctx sets the limit of
// that wait.
func (g *Group[K, S, C]) Start(ctx context.Context, key K, c C) (*Instance[S, C], bool, error) {
	for {
		inst, pending, reserved := g.reserve(key)
		if inst != nil {
			return inst, false, nil
		}

		if !reserved {
			select {
			case <-pending:
				continue
			case <-ctx.Done():
				return nil, false, fmt.Errorf("start instance for %v: %w", key, context.Cause(ctx))
			}
		}

		inst, err := g.m.Instance(ctx, c, g.opts...)
		g.settle(key, inst)

		if err != nil {
			return nil, false, fmt.Errorf("start instance for %v: %w", key, err)
		}

		return inst, true, nil
	}
}

// Resume adds an instance for key that continues from snap, for example
// after a restart. Resume does not replace an instance. Resume fails if the
// group holds an instance for key, or if the group is starting one.
func (g *Group[K, S, C]) Resume(ctx context.Context, key K, snap Snapshot[S, C]) (*Instance[S, C], error) {
	if _, _, reserved := g.reserve(key); !reserved {
		return nil, fmt.Errorf("resume instance for %v: the group already holds one", key)
	}

	inst, err := g.m.InstanceFrom(ctx, snap, g.opts...)
	g.settle(key, inst)

	if err != nil {
		return nil, fmt.Errorf("resume instance for %v: %w", key, err)
	}

	return inst, nil
}

// reserve claims key for an instance about to be created. It returns the
// instance the group already holds, or the channel closed when the creation
// another caller has under way ends, or reports that the key is now the
// caller's to fill with settle.
func (g *Group[K, S, C]) reserve(key K) (*Instance[S, C], <-chan struct{}, bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if inst, ok := g.instances[key]; ok {
		return inst, nil, false
	}

	if pending, ok := g.starting[key]; ok {
		return nil, pending, false
	}

	g.starting[key] = make(chan struct{})

	return nil, nil, true
}

// settle ends the reservation of key, keeping inst unless creating it
// failed, and wakes the callers waiting for it.
func (g *Group[K, S, C]) settle(key K, inst *Instance[S, C]) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if inst != nil {
		g.instances[key] = inst
	}

	close(g.starting[key])
	delete(g.starting, key)
}

// Get returns the instance for key.
func (g *Group[K, S, C]) Get(key K) (*Instance[S, C], bool) {
	g.mu.Lock()
	defer g.mu.Unlock()

	inst, ok := g.instances[key]

	return inst, ok
}

// Send sends ev to the instance for key, as [Instance.Send] does. If the
// group has no instance for key, Send fails with [ErrNoInstance].
func (g *Group[K, S, C]) Send(ctx context.Context, key K, ev any) (Step[S, C], error) {
	inst, ok := g.Get(key)
	if !ok {
		return Step[S, C]{}, fmt.Errorf("send to %v: %w", key, ErrNoInstance)
	}

	return inst.Send(ctx, ev)
}

// Each yields the key and the current snapshot of each instance, in no
// specified order. Each can yield an instance that starts or that is
// evicted while Each runs, but this is not certain.
func (g *Group[K, S, C]) Each() iter.Seq2[K, Snapshot[S, C]] {
	return func(yield func(K, Snapshot[S, C]) bool) {
		for _, key := range g.Keys() {
			inst, ok := g.Get(key)
			if !ok {
				continue
			}

			if !yield(key, inst.Snapshot()) {
				return
			}
		}
	}
}

// Keys returns the keys of the instances in the group, in no specified
// order.
func (g *Group[K, S, C]) Keys() []K {
	g.mu.Lock()
	defer g.mu.Unlock()

	return slices.Collect(maps.Keys(g.instances))
}

// Count returns the number of instances for which cond is true for the
// current snapshot.
func (g *Group[K, S, C]) Count(cond func(Snapshot[S, C]) bool) int {
	n := 0
	for _, snap := range g.Each() {
		if cond(snap) {
			n++
		}
	}

	return n
}

// Remove closes the instance for key and forgets it. Remove returns the
// final snapshot of the instance, so the caller can persist it. If the group
// has no instance for key, the bool result is false.
func (g *Group[K, S, C]) Remove(key K) (Snapshot[S, C], bool) {
	g.mu.Lock()
	inst, ok := g.instances[key]
	delete(g.instances, key)
	g.mu.Unlock()

	if !ok {
		var none Snapshot[S, C]
		return none, false
	}

	inst.Close()

	return inst.Snapshot(), true
}

// Evict closes and forgets each instance for which cond is true for the
// current snapshot, for example the instances that are done or idle. Evict
// returns the keys of these instances. Evict does not touch an instance
// that changes between the check and the removal. This includes an instance
// whose key goes to a different instance in that time.
func (g *Group[K, S, C]) Evict(cond func(Snapshot[S, C]) bool) []K {
	var evicted []K
	for _, key := range g.Keys() {
		inst, ok := g.Get(key)
		if !ok {
			continue
		}

		snap := inst.Snapshot()
		if !cond(snap) {
			continue
		}

		// cond runs outside the lock, so the match is confirmed under it:
		// the key still names this instance, and the instance is still at
		// the snapshot cond saw.
		g.mu.Lock()
		same := g.instances[key] == inst && inst.Snapshot().seq == snap.seq
		if same {
			delete(g.instances, key)
		}
		g.mu.Unlock()

		if same {
			inst.Close()
			evicted = append(evicted, key)
		}
	}

	return evicted
}

// Close closes all the instances that are in the group when you call
// Close. Close does not prevent more use of the group. If Start or Resume
// adds an instance at the same time, that instance stays in the group. Thus
// you must stop all calls that start instances before you call Close.
func (g *Group[K, S, C]) Close() {
	for _, key := range g.Keys() {
		g.Remove(key)
	}
}
