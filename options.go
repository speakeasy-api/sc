package sc

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"time"

	"golang.org/x/sync/semaphore"
)

// runOptions holds what the RunOption values passed to a driver set.
type runOptions struct {
	// hooks is a *Hooks[S, C] for the driver's S and C.
	hooks any

	services *semaphore.Weighted

	now func() time.Time

	manualTimers bool

	// emit is an unexported catch-all for emitted events, which a child
	// machine's runner uses to hand them to its parent, and listeners the
	// typed listeners given with Listen, by event type.
	emit      func(ctx context.Context, ev any)
	listeners map[reflect.Type][]func(ctx context.Context, ev any)

	throttle Waiter
}

// RunOption configures a driver in this package. The drivers are [Actor]
// and [Instance].
type RunOption func(*runOptions)

// runSettings applies opts over the defaults.
func runSettings(opts []RunOption) runOptions {
	o := runOptions{hooks: nil, services: nil, now: time.Now, manualTimers: false, emit: nil, listeners: map[reflect.Type][]func(context.Context, any){}, throttle: nil}
	for _, opt := range opts {
		opt(&o)
	}

	return o
}

// hooksFor returns the hooks set by WithHooks, panicking when they are for
// another state or context type.
func hooksFor[S comparable, C any](o runOptions, machine string) *Hooks[S, C] {
	if o.hooks == nil {
		return nil
	}

	hooks, ok := o.hooks.(*Hooks[S, C])
	if !ok {
		panic(fmt.Sprintf("sc: WithHooks was given %T, which is not for machine %q", o.hooks, machine))
	}

	return hooks
}

// WithHooks makes the driver report to h. If h is for a different state type
// or context type, the driver constructor panics.
func WithHooks[S comparable, C any](h *Hooks[S, C]) RunOption {
	return func(o *runOptions) { o.hooks = h }
}

// WithClock sets the clock that the driver gives to the core. The clock
// supplies the time of each event. Guards, assignments and timers see this
// time. The default clock is [time.Now].
//
// Drivers continue to use the time package to wait for timers. Thus, if the
// clock does not follow real time, a timer fires only in these conditions:
//   - An event arrives after the due time of the timer.
//   - The caller tells the driver to advance.
//
// WithClock panics if now is nil.
func WithClock(now func() time.Time) RunOption {
	if now == nil {
		panic("sc: WithClock: the clock is nil")
	}

	return func(o *runOptions) { o.now = now }
}

// LimitServices makes each service run wait for one unit of sem before the
// run starts. The run releases the unit when it ends. If drivers share one
// semaphore, it sets the maximum number of services that run at the same
// time across all of these drivers. For example, you can use this to stay
// in the rate limit of an external API. If the chart exits the state of a
// run before the run gets a unit, the run does not start.
func LimitServices(sem *semaphore.Weighted) RunOption {
	return func(o *runOptions) { o.services = sem }
}

// ManualTimers stops the automatic wake-up of an [Instance] when a timer is
// due. Then, timers fire only when the caller sends an event or calls
// [Instance.Advance]. Use this option for an instance that the loop of the
// caller drives, or for a clock that does not follow real time. The [Actor]
// ignores this option.
func ManualTimers() RunOption {
	return func(o *runOptions) { o.manualTimers = true }
}

// Listen makes the driver call fn for each event of type T that the chart
// emits with [Transition.Emit] or [Steps.Emit]. The driver calls fn in the
// order of the emits. It calls fn on the goroutine that processes the step
// that emitted the event.
//
// Go infers T from fn. Give one Listen for each type of event that you want
// to receive. If you give more than one listener for a type, the driver calls
// all of them, in the order that you gave them. If a listener is for a type
// that the chart never emits, the driver constructor fails. Thus, you find a
// listener that the driver can never call immediately.
//
// fn must not block. fn must not send events to the same instance. If the
// listener is slow, put the event in a queue. ctx is the context of the event
// that caused the step. If a listener panics, the result is the same as for
// an action: the step stays, and the send returns the panic as
// [ErrPanicked].
func Listen[T any](fn func(ctx context.Context, ev T)) RunOption {
	return func(o *runOptions) {
		t := reflect.TypeFor[T]()
		o.listeners[t] = append(o.listeners[t], func(ctx context.Context, ev any) {
			if typed, ok := ev.(T); ok {
				fn(ctx, typed)
			}
		})
	}
}

// onEmit hands every emitted event to fn, whatever its type.
func onEmit(fn func(ctx context.Context, ev any)) RunOption {
	return func(o *runOptions) { o.emit = fn }
}

// listener returns what hears the chart's emitted events: the typed
// listeners by type, and then the catch-all. It is nil when there is
// neither.
func (o runOptions) listener() func(context.Context, any) {
	if len(o.listeners) == 0 && o.emit == nil {
		return nil
	}

	listeners, all := o.listeners, o.emit

	return func(ctx context.Context, ev any) {
		for _, fn := range listeners[reflect.TypeOf(ev)] {
			fn(ctx, ev)
		}
		if all != nil {
			all(ctx, ev)
		}
	}
}

// ErrNoSuchEmit is the error that a driver constructor returns if it gets a
// listener for a type of event that the chart never emits.
var ErrNoSuchEmit = errors.New("listener for an event the chart never emits")

// checkListeners reports a listener for a type the chart never emits.
func (m *Machine[S, C]) checkListeners(o runOptions) error {
	for t := range o.listeners {
		if !m.EmitsType(t) {
			return fmt.Errorf("%w: %s", ErrNoSuchEmit, t)
		}
	}

	return nil
}

// Waiter is an object that a service run waits on before the run starts. An
// example is a *rate.Limiter from golang.org/x/time/rate. Wait returns nil
// when the run can start. Wait returns an error if ctx ends first.
type Waiter interface {
	Wait(ctx context.Context) error
}

// ThrottleServices makes each service run wait on w before the run starts.
// If drivers share one rate limiter, it sets the maximum rate at which
// services start across all of these drivers. [LimitServices] sets the
// maximum number of services that run at the same time. If the chart exits
// the state of a run before w lets the run continue, the run does not start.
func ThrottleServices(w Waiter) RunOption {
	return func(o *runOptions) { o.throttle = w }
}
