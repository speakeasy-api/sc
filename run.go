package sc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"iter"
	"reflect"
)

// Run is one run of a service. The service function gets the Run together
// with its context. The service uses the Run to get the data that the
// driver keeps for it:
//   - Its ID.
//   - The progress that it recorded on an earlier run.
//   - The resources of the states around it.
//
// The context is only for cancellation, deadlines, and traces.
//
// A Run is valid only for the call that got it. You must use a Run only
// from the goroutine that called the service function.
type Run struct {
	id string

	// saved is the progress an earlier run recorded, and record how to
	// record more; nil outside a driver.
	saved  json.RawMessage
	record func(json.RawMessage)

	// lookup finds the value of a held resource by its key; nil under a
	// driver that holds none.
	lookup func(key string) (any, bool)
}

// ErrNoCheckpoint is the error that [Run.Checkpoint] returns for a Run that
// did not come from a driver, for example a zero Run in a test.
var ErrNoCheckpoint = errors.New("run does not belong to a driver")

// ID returns the ID of the run. The ID is unique in its instance. If a
// driver runs the same effect again after a crash, the ID does not change.
// Thus the ID, together with the identity of the instance, is a good
// idempotency key for the call that the service makes. A run has a new ID
// if it starts because of a new entry to the state, or because of
// [Invocation.Retry].
func (r *Run) ID() string { return r.id }

// Key returns the key that a [Transition.Spawn] gave the run. The bool
// result is true only if the run is a spawned run.
func (r *Run) Key() (string, bool) { return SpawnKey(r.id) }

// Checkpoint records v, encoded as JSON, as the progress of the run. For
// example, the progress can be a cursor, the pages that the run got, or
// the step that it got to.
//
// The snapshot keeps the progress. Thus, if the driver stores the instance
// and starts the service again after a restart, the new run reads the
// progress with [Run.Resumed]. Then the run continues from that point, and
// does not start again from the beginning.
//
// Each call replaces the last progress and is a step of the instance. If
// [Invocation.Retry] starts a run after a failure, the new run starts from
// the progress of the failed run.
func (r *Run) Checkpoint(v any) error {
	if r == nil || r.record == nil {
		return ErrNoCheckpoint
	}

	data, err := json.Marshal(v)
	if err != nil {
		return fmt.Errorf("marshal checkpoint: %w", err)
	}

	r.record(data)

	return nil
}

// Resumed decodes into v the progress that an earlier run of the service
// recorded with [Run.Checkpoint]. The bool result is true only if there was
// progress.
func (r *Run) Resumed(v any) (bool, error) {
	if r == nil || len(r.saved) == 0 {
		return false, nil
	}

	if err := json.Unmarshal(r.saved, v); err != nil {
		return false, fmt.Errorf("unmarshal checkpoint: %w", err)
	}

	return true, nil
}

// Held returns the value of the resource for the service or acquisition
// that got run. The bool result is false if the resource is not held. This
// occurs in these conditions:
//   - Before the driver acquires the resource.
//   - After the exit from the state of the resource.
//   - Under a driver that holds no resources.
func (r *Resource[S, C, R]) Held(run *Run) (R, bool) {
	var zero R

	if run == nil || run.lookup == nil {
		return zero, false
	}

	return typedValue[R](run.lookup(r.inv.resource.key))
}

// From returns the value of the resource from the context of an action, as
// [Resource.Held] does for a service. Actions have no [Run]. Thus actions
// use From to get a resource of their state.
func (r *Resource[S, C, R]) From(ctx context.Context) (R, bool) {
	var zero R

	lookup, ok := ctx.Value(heldResources{}).(func(key string) (any, bool))
	if !ok {
		return zero, false
	}

	return typedValue[R](lookup(r.inv.resource.key))
}

func typedValue[R any](value any, ok bool) (R, bool) {
	typed, isR := value.(R)

	return typed, ok && isR
}

// heldResources is the context key under which a driver exposes the
// resources it holds.
type heldResources struct{}

// startRun builds the Run for a service function under a driver, from what
// the driver put in ctx, and wraps yield so that the function's checkpoints
// and results reach the driver until the driver stops listening.
func startRun(ctx context.Context, id string, yield func(any) bool) (*Run, func(any)) {
	stopped := false
	emit := func(ev any) {
		if !stopped && !yield(ev) {
			stopped = true
		}
	}

	saved, _ := ctx.Value(savedProgress{}).(json.RawMessage)
	lookup, _ := ctx.Value(heldResources{}).(func(key string) (any, bool))

	run := &Run{
		id:     id,
		saved:  saved,
		record: func(data json.RawMessage) { emit(Checkpointed{ID: id, Data: data}) },
		lookup: lookup,
	}

	return run, emit
}

// Port identifies a service that receives messages of type M. These
// functions return a handle that contains a Port:
//   - [StateBuilder.Receive]
//   - [StateBuilder.ReceiveStream]
//   - [StateBuilder.InvokeMachine]
//   - [StateBuilder.SpawnMachine]
//
// [Transition.SendTo] and the other send functions take a Port.
type Port[M any] struct {
	name string
}

func (p Port[M]) port() Port[M] { return p }

// Mailbox is a service that receives messages of type M. A Mailbox is the
// handle that the declaration of a receiver service returns. The send
// functions take a Mailbox. Thus the message that a send function makes
// must be of the type that the service reads.
type Mailbox[M any] interface {
	port() Port[M]
}

// Receiving is the handle of a service that receives messages of type M.
// Receiving routes the outcome of the service, as an [Invocation] does. It
// is also the [Mailbox] that the send functions use as the address.
type Receiving[S comparable, C, M, R any] struct {
	*Invocation[S, C, R]
	Port[M]
}

// messages returns what the chart sends the run, as messages of type M, in
// order, waiting for more until ctx ends.
func messages[M any](ctx context.Context) iter.Seq[M] {
	box, ok := ctx.Value(serviceInbox{}).(*inbox)
	if !ok {
		return func(func(M) bool) {}
	}

	return func(yield func(M) bool) {
		for ev := range box.events(ctx) {
			// What the driver tells a child machine about time, and
			// anything not meant for this inbox, is not the service's
			// to see.
			msg, ok := ev.(M)
			if !ok {
				continue
			}

			if !yield(msg) {
				return
			}
		}
	}
}

// triggerOf returns the event the run's state was entered with, as an Ev.
func triggerOf[Ev any](ctx context.Context) Ev {
	ev, _ := ctx.Value(trigger{}).(Ev)
	return ev
}

// trigger is the context key under which a service run finds the event its
// state was entered with.
type trigger struct{}

// adaptService adapts a service function to the form the core runs.
func adaptService[C, R any](fn func(context.Context, *Run, C) (R, error)) func(context.Context, C, string) iter.Seq[any] {
	return func(ctx context.Context, c C, id string) iter.Seq[any] {
		return func(yield func(any) bool) {
			run, emit := startRun(ctx, id, yield)

			res, err := fn(ctx, run, c)
			if err != nil {
				emit(Failed{ID: id, Err: err})
				return
			}

			emit(Done[R]{ID: id, Result: res})
		}
	}
}

// adaptStream adapts a stream function to the form the core runs.
func adaptStream[C any](fn func(context.Context, *Run, C) iter.Seq2[any, error]) func(context.Context, C, string) iter.Seq[any] {
	return func(ctx context.Context, c C, id string) iter.Seq[any] {
		return func(yield func(any) bool) {
			stopped := false
			run, emit := startRun(ctx, id, func(ev any) bool {
				stopped = !yield(ev)
				return !stopped
			})

			for ev, err := range fn(ctx, run, c) {
				if err != nil {
					emit(Failed{ID: id, Err: err})
					return
				}

				if emit(Emitted{ID: id, Event: ev}); stopped {
					return
				}
			}

			emit(Done[struct{}]{ID: id, Result: struct{}{}})
		}
	}
}

// adaptAcquire adapts an acquire function to the form the core runs.
func adaptAcquire[C, R any](fn func(context.Context, *Run, C) (R, error)) func(context.Context, C, string) iter.Seq[any] {
	return func(ctx context.Context, c C, id string) iter.Seq[any] {
		return func(yield func(any) bool) {
			run, emit := startRun(ctx, id, yield)

			value, err := fn(ctx, run, c)
			if err != nil {
				emit(Failed{ID: id, Err: err})
				return
			}

			emit(Acquired[R]{ID: id, Value: value})
		}
	}
}

// adaptRelease adapts a release function to the form the core runs.
func adaptRelease[S comparable, R any](fn func(context.Context, R, Leave[S])) func(context.Context, any, Leave[S]) {
	return func(ctx context.Context, value any, why Leave[S]) {
		if typed, ok := value.(R); ok {
			fn(ctx, typed, why)
		}
	}
}

// addInvocation declares a service of state s and returns its routing
// handle.
func (s *StateBuilder[S, C]) addInvocation(inv *invocation[S, C]) *invocation[S, C] {
	inv.state = s.n
	s.n.invocations = append(s.n.invocations, inv)

	return inv
}

// newInvocation is a service named name reporting Done[R], run by run.
func newInvocation[S comparable, C, R any](name string, run func(context.Context, C, string) iter.Seq[any]) *invocation[S, C] {
	return &invocation[S, C]{
		state:       nil,
		name:        name,
		doneType:    reflect.TypeFor[Done[R]](),
		run:         run,
		resource:    nil,
		retry:       nil,
		spawned:     false,
		inboxType:   nil,
		triggerType: nil,
	}
}
