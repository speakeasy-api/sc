package sc

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"sync"
)

// childEnded is what the goroutine watching a child machine puts in the
// queue of its events once the child is done or the watch is over, and
// childMoved what it puts there when the child's snapshot has changed.
type (
	childEnded struct{ err error }
	childMoved struct{}
)

// InvokeMachine declares a service that runs another machine as a child.
// The child runs while the state is active. input gets the context of the
// parent and returns the context that the child starts with.
//
// When the child gets to a top-level final state, output gets the last
// snapshot of the child. If output returns a value, the service succeeds
// with that value. If output returns an error, the service fails with that
// error. Thus a child that ended badly gets to the OnError of the parent.
//
// The parent and the child use events to communicate:
//   - The parent sends to the child with [Transition.SendTo]. The address is
//     the handle that InvokeMachine returns. The messages of that handle are
//     the events of the child, of any type.
//   - The child sends to the parent with [Transition.Emit]. The parent
//     handles these events with On, as it does all other events.
//
// When the parent exits the state, the child closes.
//
// The child runs under an [Instance] with the clock and opts of the parent
// driver. The snapshot of the child is kept in the snapshot of the parent,
// as the checkpoint of the service. Each time that the child moves, the
// driver replaces this snapshot. Thus each step of the child is also a step
// of the parent.
//
// A parent can resume from a stored snapshot. Then its child continues from
// the point where it was. The timers and the unfinished services of the
// child start again. The child does not run its entry actions a second
// time. If a crash occurs, the child can emit again an event that it
// emitted immediately before the crash.
//
// When you explore the parent, the child is a service like all other
// services. Its outcomes are the outcomes that you give for name. To explore
// the child, explore it separately.
func (s *StateBuilder[S, C]) InvokeMachine[S2 comparable, C2, R any](name string, child *Machine[S2, C2], input func(C) C2, output func(Snapshot[S2, C2]) (R, error), opts ...RunOption) *Child[S, C, S2, C2, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, runChild(child, func(c C, _ string) C2 { return input(c) }, output, opts)))
	inv.inboxType = reflect.TypeFor[any]()

	return newChild[S, C, S2, C2, R](s, inv, child)
}

// SpawnMachine declares a child machine as [StateBuilder.InvokeMachine]
// does, but the chart spawns it as [Invocation.Spawned] describes:
//   - Any number of children can run at the same time.
//   - Each child runs under the key that its [Transition.Spawn] gives.
//   - input gets that key, so each child can start with its own context.
//
// The parent sends to one child with [Transition.SendToKey]. To identify
// the result of each child, the parent uses [SpawnKey] on the ID of the
// result. An event that a child emits arrives as a plain event. Thus, if
// the parent must identify the child, the child must put its identity in
// the events that it emits. input gives the child this identity.
func (s *StateBuilder[S, C]) SpawnMachine[S2 comparable, C2, R any](name string, child *Machine[S2, C2], input func(c C, key string) C2, output func(Snapshot[S2, C2]) (R, error), opts ...RunOption) *Child[S, C, S2, C2, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, runChild(child, input, output, opts)))
	inv.inboxType = reflect.TypeFor[any]()
	inv.spawned = true

	return newChild[S, C, S2, C2, R](s, inv, child)
}

// newChild is the handle of the child machine inv runs.
func newChild[S comparable, C any, S2 comparable, C2, R any](s *StateBuilder[S, C], inv *invocation[S, C], child *Machine[S2, C2]) *Child[S, C, S2, C2, R] {
	receiving := &Receiving[S, C, any, R]{Invocation: &Invocation[S, C, R]{b: s.b, n: s.n, inv: inv}, Port: Port[any]{name: inv.name}}

	return &Child[S, C, S2, C2, R]{Receiving: receiving, machine: child}
}

// runChild is the service that runs child for a parent.
func runChild[C any, S2 comparable, C2, R any](child *Machine[S2, C2], input func(C, string) C2, output func(Snapshot[S2, C2]) (R, error), opts []RunOption) func(context.Context, C, string) iter.Seq[any] {
	return func(ctx context.Context, c C, id string) iter.Seq[any] {
		return func(yield func(any) bool) {
			// The child's events queue here, so that neither machine waits
			// for the other while it is busy.
			// It needs no limit of its own: every step of the child is
			// saved through it, so the child cannot run ahead unseen.
			up := newInbox(0)

			settings := append([]RunOption{WithClock(clockFrom(ctx))}, opts...)

			// Under a caller that drives time itself, the child's timers
			// are driven with the parent's.
			manual, _ := ctx.Value(manualClock{}).(bool)
			if manual {
				settings = append(settings, ManualTimers())
			}

			// What the child emits during a step waits until that step's
			// snapshot has been handed up, so the parent never handles an
			// event from a child it still sees in the state before.
			var (
				mu      sync.Mutex
				emitted []any
			)
			settings = append(settings, onEmit(func(_ context.Context, ev any) {
				mu.Lock()
				defer mu.Unlock()

				emitted = append(emitted, ev)
			}))

			// moved hands up the child's snapshot, then what it emitted
			// on the way there.
			moved := func() {
				mu.Lock()
				defer mu.Unlock()

				up.put(childMoved{})
				for _, ev := range emitted {
					up.put(ev)
				}
				emitted = nil
			}

			inst, _, err := startChild(ctx, child, input(c, keyOf(id)), settings)
			if err != nil {
				yield(Failed{ID: id, Err: err})
				return
			}
			defer inst.Close()

			// The child's snapshot is handed up at once: a new child must
			// be saved so that a resumed parent does not start it a second
			// time, and either kind may already have taken a step, on a
			// timer or a service of its own. The watcher below counts from
			// the snapshot read before that, so no step falls between.
			last := inst.Snapshot().Seq()
			moved()

			ctx, stop := context.WithCancel(ctx)
			defer stop()

			go func() {
				box, ok := ctx.Value(serviceInbox{}).(*inbox)
				if !ok {
					return
				}

				// An action's error has no caller here; the child's hooks
				// see it.
				for ev := range box.events(ctx) {
					if _, moved := ev.(advance); moved {
						_, _ = inst.Advance(ctx)
						continue
					}

					_, _ = inst.Send(ctx, ev)
				}
			}()

			go func() {
				for {
					snap, err := inst.Await(ctx, func(snap Snapshot[S2, C2]) bool { return snap.Seq() != last || snap.Done() })
					if err != nil {
						up.put(childEnded{err: err})
						return
					}

					if snap.Seq() != last {
						last = snap.Seq()
						moved()
					}

					if snap.Done() {
						up.put(childEnded{err: nil})
						return
					}
				}
			}()

			for ev := range up.events(ctx) {
				var next any = Emitted{ID: id, Event: ev}

				if _, moved := ev.(childMoved); moved {
					data, err := json.Marshal(inst.Snapshot())
					if err != nil {
						yield(Failed{ID: id, Err: fmt.Errorf("save child machine: %w", err)})
						return
					}

					next = Checkpointed{ID: id, Data: data}
				}

				ended, ok := ev.(childEnded)
				if !ok {
					if !yield(next) {
						return
					}

					continue
				}

				// The wait only fails when the service was stopped.
				if ended.err != nil {
					return
				}

				result, err := output(inst.Snapshot())
				if err != nil {
					yield(Failed{ID: id, Err: err})
					return
				}

				yield(Done[R]{ID: id, Result: result})

				return
			}
		}
	}
}

// startChild starts the child, or continues it from the snapshot an earlier
// run of the service saved, and reports which.
func startChild[S2 comparable, C2 any](ctx context.Context, child *Machine[S2, C2], c C2, settings []RunOption) (*Instance[S2, C2], bool, error) {
	saved, _ := ctx.Value(savedProgress{}).(json.RawMessage)
	if len(saved) == 0 {
		inst, err := child.Instance(ctx, c, settings...)

		return inst, false, err
	}

	snap, _, err := child.Restore(saved, clockFrom(ctx)())
	if err != nil {
		return nil, false, fmt.Errorf("restore child machine: %w", err)
	}

	inst, err := child.InstanceFrom(ctx, snap, settings...)

	return inst, true, err
}
