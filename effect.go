package sc

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"runtime/debug"
	"time"
)

// Done is the event that the driver sends to the chart when an invoked
// service is successful.
type Done[R any] struct {
	// ID identifies the run of the service that made the result.
	ID string `json:"id"`

	// Result is the value that the service returned.
	Result R `json:"result"`
}

func (d Done[R]) invocationID() string { return d.ID }

// Failed is the event that the driver sends to the chart when an invoked
// service returns an error.
type Failed struct {
	// ID identifies the run of the service that failed.
	ID string

	// Err is the error that the service returned. When a driver stores the
	// event and loads it again, the message of Err stays the same. The code
	// of an error that implements [Coder] also stays the same, but the type
	// of the error does not. As a result, if a driver can persist the event,
	// use [ErrorCode] to select a transition. Do not use the type of the
	// error.
	Err error
}

// Coder is the interface of an error that has a code that does not change,
// for example "rate_limited". When a driver stores a [Failed] event, the
// event keeps the code. As a result, with all drivers, a chart can use
// [ErrorCode] to select a transition from the code.
type Coder interface {
	Code() string
}

// codedError is what a stored error with a code becomes when it is loaded.
type codedError struct {
	msg  string
	code string
}

func (e codedError) Error() string { return e.msg }

func (e codedError) Code() string { return e.code }

// WithCode returns err with code attached. Use it for an error from a package
// that has no codes. The result wraps err. As a result, errors.Is and
// errors.As continue to find err.
func WithCode(err error, code string) error {
	if err == nil {
		return nil
	}

	return codedWrapError{err: err, code: code}
}

// codedWrapError is an error given a code by WithCode.
type codedWrapError struct {
	err  error
	code string
}

func (e codedWrapError) Error() string { return e.err.Error() }

func (e codedWrapError) Code() string { return e.code }

func (e codedWrapError) Unwrap() error { return e.err }

// ErrorCode returns the code of the first error in the tree of err that
// implements [Coder]. If no error in the tree implements [Coder] but err is
// an error of the context package, ErrorCode returns [CodeDeadlineExceeded]
// or [CodeCanceled]. If not, ErrorCode returns "".
func ErrorCode(err error) string {
	var c Coder
	if errors.As(err, &c) {
		return c.Code()
	}

	// The context's own errors have codes of their own, so that a stored
	// failure can still be told for a timeout or a cancellation.
	switch {
	case errors.Is(err, context.DeadlineExceeded):
		return CodeDeadlineExceeded
	case errors.Is(err, context.Canceled):
		return CodeCanceled
	}

	return ""
}

// These are the codes that [ErrorCode] returns for the errors of the context
// package. A driver can load a [Failed] event with one of these codes from
// storage. Then errors.Is matches the error of the event to the context
// error again.
const (
	CodeDeadlineExceeded = "deadline_exceeded"
	CodeCanceled         = "canceled"
)

// Is makes a stored timeout or cancellation match the context's error.
func (e codedError) Is(target error) bool {
	return (e.code == CodeDeadlineExceeded && target == context.DeadlineExceeded) ||
		(e.code == CodeCanceled && target == context.Canceled)
}

func (f Failed) invocationID() string { return f.ID }

// failedJSON is the stored form of a Failed event.
type failedJSON struct {
	// ID identifies the run of the service that failed.
	ID string `json:"id"`

	// Error is the message of the service's error.
	Error string `json:"error"`

	// Code is the error's code, when it implements Coder.
	Code string `json:"code,omitempty"`
}

// MarshalJSON encodes the error as its message and its code.
func (f Failed) MarshalJSON() ([]byte, error) {
	msg := ""
	if f.Err != nil {
		msg = f.Err.Error()
	}

	data, err := json.Marshal(failedJSON{ID: f.ID, Error: msg, Code: ErrorCode(f.Err)})
	if err != nil {
		return nil, fmt.Errorf("marshal failed event: %w", err)
	}

	return data, nil
}

// UnmarshalJSON makes the error again from its message and its code.
func (f *Failed) UnmarshalJSON(data []byte) error {
	var raw failedJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return fmt.Errorf("unmarshal failed event: %w", err)
	}

	f.ID = raw.ID
	f.Err = errors.New(raw.Error)
	if raw.Code != "" {
		f.Err = codedError{msg: raw.Error, code: raw.Code}
	}

	return nil
}

// Acquired is the event that the driver sends to the chart after it acquires
// a resource. You declare a resource with [StateBuilder.Holds]. The driver
// keeps Value for the services and actions in the state of the resource. The
// chart can read Value in the OnAcquired transition. Do not put a live value
// into the context, because then the resource has no purpose.
type Acquired[R any] struct {
	// ID identifies the acquisition.
	ID string

	// Value is the resource that the driver acquired.
	Value R
}

func (a Acquired[R]) acquisition() (string, any) { return a.ID, a.Value }

// acquisition is implemented by Acquired events.
type acquisition interface {
	acquisition() (id string, value any)
}

// invocationResult is implemented by the events that end a service run.
type invocationResult interface {
	invocationID() string
}

// Emitted wraps an event from a [StateBuilder.Stream] service. The wrapper
// tells the machine which run of the service sent the event.
type Emitted struct {
	// ID identifies the run of the service that sent the event.
	ID string

	// Event is the value that the service yielded.
	Event any
}

// Checkpointed is the event that a service run sends through
// [Run.Checkpoint]. The event records the progress of the run in the
// snapshot. After a restart, the driver starts the run again. That run reads
// the last progress with [Run.Resumed].
type Checkpointed struct {
	// ID identifies the run of the service that recorded the progress.
	ID string

	// Data is the progress, in JSON.
	Data json.RawMessage
}

// TimerFired is the event that a driver sends when a timer is due. An
// [EffectStartTimer] effect starts the timer.
type TimerFired struct {
	// ID is the ID of the effect that started the timer.
	ID string
}

// EffectKind tells what a driver must do with an [Effect].
type EffectKind string

const (
	// EffectAction runs an action with [Machine.Exec].
	EffectAction EffectKind = "action"

	// EffectStartInvocation runs a service with [Machine.Exec]. The driver
	// sends the events of the service back to the machine.
	EffectStartInvocation EffectKind = "invocation.start"

	// EffectStopInvocation stops the service run with the same ID. If a
	// driver cannot stop the run, the run can continue until it ends. The
	// machine ignores the events of that run.
	EffectStopInvocation EffectKind = "invocation.stop"

	// EffectAcquire acquires a resource with [Machine.Exec]. [Machine.Exec]
	// sends an [Acquired] event with the value, or a [Failed] event. The
	// driver keeps the value until the EffectRelease with the same ID. Only
	// the [Instance] and [Actor] drivers hold resources.
	EffectAcquire EffectKind = "resource.acquire"

	// EffectRelease releases the resource that the driver acquired with the
	// same ID. If the acquisition did not end yet, the driver releases the
	// value immediately after it gets the value.
	EffectRelease EffectKind = "resource.release"

	// EffectSend sends Event to the service run with the same ID. The
	// service reads Event from its inbox. If a driver cannot send Event, the
	// driver discards it.
	EffectSend EffectKind = "invocation.send"

	// EffectEmit gives Event to the code that runs the machine. The driver
	// gives Event to the listeners that you set with [Listen].
	EffectEmit EffectKind = "emit"

	// EffectStartTimer asks for a [TimerFired] event with the ID of the
	// effect after Delay.
	EffectStartTimer EffectKind = "timer.start"

	// EffectCancelTimer cancels the timer with the same ID. If a driver
	// cannot cancel the timer, the timer can fire. The machine ignores the
	// event.
	EffectCancelTimer EffectKind = "timer.cancel"
)

// Effect is one unit of work that a transition gives to its driver. The
// machine returns the effects in the order that the driver must do them.
type Effect[C any] struct {
	// Kind tells what the driver must do.
	Kind EffectKind

	// ID identifies a timer or a service run. ID is unique in one machine
	// instance. ID is empty for actions.
	ID string

	// State is the name of the state that the effect is part of.
	State string

	// Name is the name of the action or service. For a timer, Name is the
	// name of a calculated delay or deadline. For a fixed delay, Name is
	// empty.
	Name string

	// Delay is the duration of an EffectStartTimer timer.
	Delay time.Duration

	// At is the time when an EffectStartTimer timer is due. At uses the
	// clock that the driver gave to the core. Drivers set the timer for At.
	// If At is in the past, the timer fires immediately.
	//
	// For an EffectStartInvocation or an EffectAcquire, At is the deadline
	// of the budgets around the service or the acquisition. You declare
	// these budgets with [StateBuilder.Within]. At uses the same clock. If
	// there is no budget, At is zero. [Hooks.Exec] applies At. For other
	// drivers, [Effect.Budget] changes At into a duration.
	At time.Time

	// Jitter is the fraction by which the driver changes the wait of a timer
	// at random. The change can make the wait longer or shorter.
	// [Effect.Wait] applies Jitter.
	Jitter float64

	// Context is the context that an action or service must get.
	Context C

	// Event is the event that started an action on a transition. For a
	// chart action, Event is the event of the step that the action runs in.
	// Event is nil for the entry and exit actions of a state.
	Event any

	// From and To contain the names of the active states before and after
	// the step of an action. A chart action gets them as its [Move].
	From []string

	// To contains the names of the active states after the step.
	To []string

	// Needs contains the IDs of the held resources that a service or an
	// acquisition runs in. When a driver starts an instance again, it waits
	// until it acquires these resources again. Then it runs the effect. The
	// driver does not persist Needs.
	Needs []string

	// Saved is for an EffectStartInvocation. Saved is the last progress
	// that the service recorded with [Run.Checkpoint]. This run reads Saved
	// with [Run.Resumed]. If the service recorded no progress, Saved is nil.
	Saved json.RawMessage
}

// Budget returns the time that an EffectStartInvocation service or an
// EffectAcquire acquisition can run, from now on the clock of the driver.
// If no [StateBuilder.Within] budget applies, Budget returns false. If the
// budget is spent, the duration is zero. Drivers change the duration into a
// context timeout, not a deadline. The reason is that the clock of the
// driver is not always the wall clock.
func (e Effect[C]) Budget(now time.Time) (time.Duration, bool) {
	if (e.Kind != EffectStartInvocation && e.Kind != EffectAcquire) || e.At.IsZero() {
		return 0, false
	}

	return max(e.At.Sub(now), 0), true
}

// Wait returns the time that a driver waits, from now, before an
// EffectStartTimer timer fires. The wait is the time until At. Wait uses
// random to change the wait by a fraction of Jitter or less. The random
// function returns a number in [0, 1). The result is never negative.
func (e Effect[C]) Wait(now time.Time, random func() float64) time.Duration {
	wait := e.At.Sub(now)

	if e.Jitter > 0 && wait > 0 {
		wait = time.Duration(float64(wait) * (1 + e.Jitter*(2*random()-1)))
	}

	return max(wait, 0)
}

// ErrPanicked is the error that [Machine.Exec] returns when an action or
// service panics. The error contains the panic value and the stack. Drivers
// send the panic of a service to the chart as its [Failed] event. As a
// result, the chart handles a service that panics as it handles all other
// failures.
var ErrPanicked = errors.New("action or service panicked")

// sendPanic carries a panic raised by a driver's send callback through the
// service that was producing events, so that Exec does not report it as the
// service's own.
type sendPanic struct{ value any }

func guardedSend(send func(ev any), ev any) {
	defer func() {
		if p := recover(); p != nil {
			panic(sendPanic{value: p})
		}
	}()

	send(ev)
}

// ErrUnknownEffect is the error for an effect that names an action or
// service that the chart does not declare. This occurs when an effect stays
// after a change to the chart.
var ErrUnknownEffect = errors.New("effect does not match the chart")

// Exec does an [EffectAction], [EffectStartInvocation] or [EffectAcquire]
// effect. For a service or an acquisition, Exec blocks until the work ends or
// until ctx is cancelled. Exec calls send with each event of the work. The
// driver gives these events to [Machine.Transition]. The driver must do all
// other effects, for example timer, stop and release effects. Exec returns an
// error for them.
//
// If the action or service panics, Exec returns [ErrPanicked]. A panic in
// send is a panic of the driver. Exec does not recover it.
func (m *Machine[S, C]) Exec(ctx context.Context, e Effect[C], send func(ev any)) (err error) {
	defer func() {
		if p := recover(); p != nil {
			if own, ok := p.(sendPanic); ok {
				panic(own.value)
			}

			err = fmt.Errorf("%w: %s %q in state %q: %v\n%s", ErrPanicked, e.Kind, e.Name, e.State, p, debug.Stack())
		}
	}()

	n, ok := m.owner(e.State)
	if !ok {
		return fmt.Errorf("%w: state %q", ErrUnknownEffect, e.State)
	}

	switch e.Kind {
	case EffectAction:
		return m.execAction(ctx, n, e)
	case EffectStartInvocation, EffectAcquire:
		for _, inv := range n.invocations {
			if inv.name != e.Name || (inv.resource != nil) != (e.Kind == EffectAcquire) {
				continue
			}

			run := m.serviceRunner(inv)
			if run == nil {
				return fmt.Errorf("%w: service %q in state %q", ErrNotBound, e.Name, e.State)
			}

			ctx = context.WithValue(context.WithValue(ctx, savedProgress{}, e.Saved), trigger{}, e.Event)

			for ev := range run(ctx, e.Context, e.ID) {
				guardedSend(send, ev)
				if ctx.Err() != nil {
					break
				}
			}

			return nil
		}

		return fmt.Errorf("%w: service %q in state %q", ErrUnknownEffect, e.Name, e.State)
	case EffectStopInvocation, EffectStartTimer, EffectCancelTimer, EffectRelease, EffectSend, EffectEmit:
		return fmt.Errorf("%w: %s effects are handled by the driver", errors.ErrUnsupported, e.Kind)
	default:
		return fmt.Errorf("%w: kind %q", ErrUnknownEffect, e.Kind)
	}
}

func (m *Machine[S, C]) execAction(ctx context.Context, n *node[S, C], e Effect[C]) error {
	for _, t := range append([]*transition[S, C]{n.onEntry, n.onExit}, n.transitions...) {
		if t == nil {
			continue
		}

		for _, s := range t.steps {
			if s.kind == stepDo && s.name == e.Name {
				s.do(ctx, e.Context, e.Event)
				return nil
			}
		}
	}

	if fn, ok := m.actions[e.Name]; ok {
		if fn == nil {
			return fmt.Errorf("%w: action %q in state %q", ErrNotBound, e.Name, e.State)
		}

		fn(ctx, e.Context, Move[S]{Source: n.id, From: m.stateIDs(e.From), To: m.stateIDs(e.To), Event: e.Event})

		return nil
	}

	return fmt.Errorf("%w: action %q in state %q", ErrUnknownEffect, e.Name, e.State)
}

// release gives back a resource's value, as the EffectRelease e asks.
func (m *Machine[S, C]) release(ctx context.Context, e Effect[C], value any, closed bool) (err error) {
	defer func() {
		if p := recover(); p != nil {
			err = fmt.Errorf("%w: release of %q in state %q: %v\n%s", ErrPanicked, e.Name, e.State, p, debug.Stack())
		}
	}()

	n, ok := m.owner(e.State)
	if !ok {
		return fmt.Errorf("%w: state %q", ErrUnknownEffect, e.State)
	}

	for _, inv := range n.invocations {
		if inv.name != e.Name || inv.resource == nil {
			continue
		}

		release := m.resourceRelease(inv)
		if release == nil {
			return fmt.Errorf("%w: resource %q in state %q", ErrNotBound, e.Name, e.State)
		}

		why := Leave[S]{Move: Move[S]{Source: n.id, From: m.stateIDs(e.From), To: m.stateIDs(e.To), Event: e.Event}, Closed: false}
		if closed {
			var none Move[S]
			why = Leave[S]{Move: none, Closed: true}
		}
		release(ctx, value, why)

		return nil
	}

	return fmt.Errorf("%w: resource %q in state %q", ErrUnknownEffect, e.Name, e.State)
}

// stateIDs turns state names back into identifiers, skipping names the
// chart no longer has.
func (m *Machine[S, C]) stateIDs(names []string) []S {
	ids := make([]S, 0, len(names))
	for _, name := range names {
		if n, ok := m.names[name]; ok {
			ids = append(ids, n.id)
		}
	}

	return ids
}

// effectJSON is the stored form of an Effect.
type effectJSON struct {
	// Kind says what the driver must do.
	Kind EffectKind `json:"kind"`

	// ID identifies a timer or a service run.
	ID string `json:"id,omitempty"`

	// State is the name of the state the effect belongs to.
	State string `json:"state"`

	// Name is the action or service name.
	Name string `json:"name,omitempty"`

	// DelayNanos is the timer duration in nanoseconds.
	DelayNanos int64 `json:"delay_ns,omitempty"`

	// At is when a deadline timer fires.
	At time.Time `json:"at,omitzero"`

	// Jitter is the fraction by which the driver varies the wait.
	Jitter float64 `json:"jitter,omitempty"`

	// Context is the context an action or service must see. Timer and
	// stop effects carry none.
	Context json.RawMessage `json:"context,omitempty"`

	// EventName is the registered name of the triggering event's type.
	EventName string `json:"event_name,omitempty"`

	// Event is the triggering event.
	Event json.RawMessage `json:"event,omitempty"`

	// From names the states active before the action's step.
	From []string `json:"from,omitempty"`

	// To names the states active after it.
	To []string `json:"to,omitempty"`

	// Saved is the progress a service run starts from.
	Saved json.RawMessage `json:"saved,omitempty"`
}

// EncodeEffect encodes an effect. A driver can then store the effect and do
// it later. The context and the event of the effect must marshal to JSON.
func (m *Machine[S, C]) EncodeEffect(e Effect[C]) ([]byte, error) {
	raw := effectJSON{
		Kind:       e.Kind,
		ID:         e.ID,
		State:      e.State,
		Name:       e.Name,
		DelayNanos: int64(e.Delay),
		At:         e.At,
		Jitter:     e.Jitter,
		Context:    nil,
		EventName:  "",
		Event:      nil,
		From:       e.From,
		To:         e.To,
		Saved:      e.Saved,
	}

	if e.Kind == EffectAction || e.Kind == EffectStartInvocation || e.Kind == EffectAcquire {
		ctx, err := json.Marshal(e.Context)
		if err != nil {
			return nil, fmt.Errorf("marshal effect context: %w", err)
		}
		raw.Context = ctx
	}

	if e.Event != nil {
		name, ok := m.eventNames[reflect.TypeOf(e.Event)]
		if !ok {
			return nil, fmt.Errorf("marshal effect event: type %T is not handled by the chart", e.Event)
		}

		ev, err := json.Marshal(e.Event)
		if err != nil {
			return nil, fmt.Errorf("marshal effect event %q: %w", name, err)
		}
		raw.EventName = name
		raw.Event = ev
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal effect: %w", err)
	}

	return data, nil
}

// DecodeEffect is the inverse of [Machine.EncodeEffect].
func (m *Machine[S, C]) DecodeEffect(data []byte) (Effect[C], error) {
	var zero Effect[C]

	var raw effectJSON
	if err := json.Unmarshal(data, &raw); err != nil {
		return zero, fmt.Errorf("unmarshal effect: %w", err)
	}

	e := Effect[C]{
		Kind:    raw.Kind,
		ID:      raw.ID,
		State:   raw.State,
		Name:    raw.Name,
		Delay:   time.Duration(raw.DelayNanos),
		At:      raw.At,
		Jitter:  raw.Jitter,
		Context: zero.Context,
		Event:   nil,
		From:    raw.From,
		To:      raw.To,
		Needs:   nil,
		Saved:   raw.Saved,
	}

	if len(raw.Context) > 0 {
		if err := json.Unmarshal(raw.Context, &e.Context); err != nil {
			return zero, fmt.Errorf("unmarshal effect context: %w", err)
		}
	}

	if raw.EventName != "" {
		t, ok := m.eventTypes[raw.EventName]
		if !ok {
			return zero, fmt.Errorf("%w: event %q", ErrUnknownEffect, raw.EventName)
		}

		ev := reflect.New(t)
		if err := json.Unmarshal(raw.Event, ev.Interface()); err != nil {
			return zero, fmt.Errorf("unmarshal effect event %q: %w", raw.EventName, err)
		}
		e.Event = ev.Elem().Interface()
	}

	return e, nil
}

// owner returns the state an effect names: one of the chart's states, or
// the chart itself for the actions of its own transitions.
func (m *Machine[S, C]) owner(name string) (*node[S, C], bool) {
	if n, ok := m.names[name]; ok {
		return n, true
	}

	if name == m.root.name {
		return m.root, true
	}

	return nil, false
}
