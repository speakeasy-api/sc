package sc

import (
	"context"
	"errors"
	"slices"
	"time"
)

// Hooks is a set of functions that a driver calls while it runs machine
// instances. Use Hooks for logs, metrics, and traces. All fields are
// optional.
//
// The hooks run on the goroutines of the driver. Thus they must be safe
// for use from many goroutines at the same time. The hooks must also
// return quickly. An [Actor] does not process the next event until its
// hooks return.
//
// The sco11y subpackage makes Hooks that send data to OpenTelemetry.
type Hooks[S comparable, C any] struct {
	// Transition is the function that the driver calls after each event
	// that an instance accepts. It gets the snapshots before and after the
	// event, and the effects that the event made. ctx is the context that
	// came with the event. Thus a span in ctx becomes the parent of the
	// data that the hook records. For an event from a service, ctx is the
	// context that EffectStarted returned for that service run.
	Transition func(ctx context.Context, step Step[S, C])

	// Ignored is the function that the driver calls for an event that
	// changed nothing. These are the possible events:
	//   - An event that no active state handles.
	//   - An event about a timer or service run that is not pending now.
	//   - An event to an instance that is done.
	//
	// Some late results after the exit from a state are usual. But a
	// continuous flow of late results can show that a driver delivers
	// events too late.
	Ignored func(ctx context.Context, snap Snapshot[S, C], event any)

	// EffectStarted is the function that the driver calls before an action
	// runs or before a service run starts. The effect runs with the context
	// that EffectStarted returns. Thus a span that starts here becomes the
	// parent of the calls of the effect. If EffectStarted returns nil, the
	// effect uses ctx.
	EffectStarted func(ctx context.Context, effect Effect[C]) context.Context

	// EffectFinished is the function that the driver calls when an action
	// or service run ends, after EffectStarted reported it. It gets these
	// values:
	//   - The context that EffectStarted returned.
	//   - The duration of the effect.
	//   - The error at the end of the effect.
	//
	// If a service returned an error, EffectFinished gets that error. If a
	// service stopped because of the exit from its state, EffectFinished
	// gets the error of the context.
	//
	// The result of a service causes a transition. The driver can report
	// that transition before or after EffectFinished for that run. This
	// changes with the driver.
	EffectFinished func(ctx context.Context, effect Effect[C], elapsed time.Duration, err error)
}

// Observe reports a step. If the step changed the instance, Observe
// reports it to Transition. If the step changed nothing, Observe reports it
// to Ignored. Drivers call Observe after each [Machine.Transition]. If the
// Hooks is nil, Observe does nothing.
func (h *Hooks[S, C]) Observe(ctx context.Context, step Step[S, C]) {
	if h == nil {
		return
	}

	if !step.Accepted() {
		if h.Ignored != nil {
			h.Ignored(ctx, step.To, step.Event)
		}

		return
	}

	if h.Transition != nil {
		h.Transition(ctx, step)
	}
}

// Exec does an action or service effect with [Machine.Exec]. Exec reports
// the effect to EffectStarted and EffectFinished. Drivers call Exec instead
// of [Machine.Exec].
//
// now is the time on the clock of the driver. The [StateBuilder.Within]
// budget of a service starts at now. Exec sets the budget as a timeout on
// the context of the service.
//
// send gets each event that the effect makes, with the context of the
// effect. Thus the driver can give that context to [Hooks.Observe]. If the
// Hooks is nil, Exec only runs the effect.
func (h *Hooks[S, C]) Exec(ctx context.Context, m *Machine[S, C], e Effect[C], now time.Time, send func(ctx context.Context, ev any)) error {
	if budget, ok := e.Budget(now); ok {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, budget)
		defer cancel()

		// The budget holds whether or not the service heeds its context:
		// when it runs out the chart is told the service failed, and
		// whatever the service reports afterwards is ignored.
		if e.Kind == EffectStartInvocation {
			overdue := time.AfterFunc(budget, func() {
				send(ctx, Failed{ID: e.ID, Err: context.DeadlineExceeded})
			})
			defer overdue.Stop()
		}
	}

	if h != nil && h.EffectStarted != nil {
		if started := h.EffectStarted(ctx, e); started != nil {
			ctx = started
		}
	}

	// outcome is the error a service run ended with, and ended is set once
	// it reported an outcome at all.
	var outcome error
	ended := false

	begin := time.Now()
	err := m.Exec(ctx, e, func(ev any) {
		if r, ok := ev.(invocationResult); ok && r.invocationID() == e.ID {
			ended = true
			if f, failed := ev.(Failed); failed {
				outcome = f.Err
			}
		}

		send(ctx, ev)
	})
	elapsed := time.Since(begin)

	if h != nil && h.EffectFinished != nil {
		reported := err
		switch {
		case reported != nil:
		case outcome != nil:
			reported = outcome
		case e.Kind == EffectStartInvocation && !ended:
			reported = context.Cause(ctx)
			if reported == nil {
				reported = errServiceEndedEarly
			}
		}

		h.EffectFinished(ctx, e, elapsed, reported)
	}

	return err
}

// errServiceEndedEarly is reported for a service run that ended without a
// result although nothing stopped it. Exec never produces this for a
// service built with Invoke or Stream.
var errServiceEndedEarly = errors.New("service ended without reporting a result")

// joinedContexts carries, through an effect's context, the context each
// joined hook set's EffectStarted returned, so each EffectFinished gets its
// own.
type joinedContexts struct{}

// JoinHooks returns hooks that call each item of hs in sequence. JoinHooks
// ignores nil items. Thus, for example, OpenTelemetry hooks and the logs of
// the caller can observe one driver together.
//
// Each EffectStarted gets the context that the EffectStarted before it
// returned. Each EffectFinished gets the context that its own
// EffectStarted returned. The EffectFinished functions run in the reverse
// sequence.
func JoinHooks[S comparable, C any](hs ...*Hooks[S, C]) *Hooks[S, C] {
	var set []*Hooks[S, C]
	for _, h := range hs {
		if h != nil {
			set = append(set, h)
		}
	}

	return &Hooks[S, C]{
		Transition: func(ctx context.Context, step Step[S, C]) {
			for _, h := range set {
				if h.Transition != nil {
					h.Transition(ctx, step)
				}
			}
		},
		Ignored: func(ctx context.Context, snap Snapshot[S, C], event any) {
			for _, h := range set {
				if h.Ignored != nil {
					h.Ignored(ctx, snap, event)
				}
			}
		},
		EffectStarted: func(ctx context.Context, effect Effect[C]) context.Context {
			own := make([]context.Context, len(set))
			for i, h := range set {
				if h.EffectStarted != nil {
					if started := h.EffectStarted(ctx, effect); started != nil {
						ctx = started
					}
				}
				own[i] = ctx
			}

			return context.WithValue(ctx, joinedContexts{}, own)
		},
		EffectFinished: func(ctx context.Context, effect Effect[C], elapsed time.Duration, err error) {
			own, _ := ctx.Value(joinedContexts{}).([]context.Context)
			for i, s := range slices.Backward(set) {
				if s.EffectFinished == nil {
					continue
				}

				hookCtx := ctx
				if i < len(own) {
					hookCtx = own[i]
				}
				s.EffectFinished(hookCtx, effect, elapsed, err)
			}
		},
	}
}
