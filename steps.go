package sc

import (
	"context"
	"fmt"
	"math/rand/v2"
	"reflect"
	"time"
)

// Steps declares what a state does when it is entered or exited. Get it from
// [StateBuilder.Entry] or [StateBuilder.Exit]. It has the same steps that a
// transition takes, and they run in the order that you declare them. An
// action sees an assignment that you declare before it. It does not see an
// assignment that you declare after it.
//
// The steps do not see the event that caused the entry or exit. A chart
// action that [Steps.Run] runs sees that event in its [Move].
type Steps[S comparable, C any] struct {
	t *Transition[S, C, struct{}]
}

// steps returns the step list kept in slot for n, creating it on first use.
func (b *build[S, C]) steps(n *node[S, C], slot **transition[S, C]) *Steps[S, C] {
	if *slot == nil {
		t := new(transition[S, C])
		t.source = n
		t.trigger = triggerNone
		t.sendTargets = map[int]string{}
		t.keys = map[int]func(C, any) string{}
		*slot = t
	}

	return &Steps[S, C]{t: &Transition[S, C, struct{}]{b: b, t: *slot}}
}

// Entry returns the steps that the state takes each time it is entered.
// They run after the entry steps of the states around it, and before the
// entry steps of the states inside it. For the chart itself, they run when
// the instance starts.
func (s *StateBuilder[S, C]) Entry() *Steps[S, C] { return s.b.steps(s.n, &s.n.onEntry) }

// Exit returns the steps that the state takes each time it is exited. They
// run after the exit steps of the states inside it. For the chart itself, and
// for the states that are active when the machine finishes, they run when the
// machine finishes.
func (s *StateBuilder[S, C]) Exit() *Steps[S, C] { return s.b.steps(s.n, &s.n.onExit) }

// Entry returns the steps that the final state takes when it is entered.
// They run before the core reports that the parent state is complete.
// Because of this, the completion transitions of the parent state see an
// assignment that these steps make.
func (f *FinalBuilder[S, C]) Entry() *Steps[S, C] { return f.b.steps(f.n, &f.n.onEntry) }

// Exit returns the steps that the final state takes when it is exited.
func (f *FinalBuilder[S, C]) Exit() *Steps[S, C] { return f.b.steps(f.n, &f.n.onExit) }

// Assign replaces the context with the value that f returns, as
// [Transition.Assign] does.
func (s *Steps[S, C]) Assign(name string, f func(C) C) *Steps[S, C] {
	s.t.Assign(name, func(c C, _ struct{}) C { return f(c) })
	return s
}

// AssignAt is [Steps.Assign] for an assignment that needs the current time.
func (s *Steps[S, C]) AssignAt(name string, f func(c C, now time.Time) C) *Steps[S, C] {
	s.t.AssignAt(name, func(c C, _ struct{}, now time.Time) C { return f(c, now) })
	return s
}

// AssignRand is [Steps.AssignAt] for an assignment that also uses random
// numbers, as [Transition.AssignRand] does.
func (s *Steps[S, C]) AssignRand(name string, f func(c C, now time.Time, rnd *rand.Rand) C) *Steps[S, C] {
	s.t.AssignRand(name, func(c C, _ struct{}, now time.Time, rnd *rand.Rand) C { return f(c, now, rnd) })
	return s
}

// Do adds an action, as [Transition.Do] does. The name must be unique among
// the actions of the state.
func (s *Steps[S, C]) Do(name string, f func(context.Context, C)) *Steps[S, C] {
	s.t.Do(name, func(ctx context.Context, c C, _ struct{}) { f(ctx, c) })
	return s
}

// Run runs chart actions, as [Transition.Run] does.
func (s *Steps[S, C]) Run(actions ...*Action[S, C]) *Steps[S, C] {
	s.t.Run(actions...)
	return s
}

// RunIf runs a chart action only if cond is true for the [Move], as
// [Transition.RunIf] does.
func (s *Steps[S, C]) RunIf(action *Action[S, C], cond func(Move[S]) bool) *Steps[S, C] {
	s.t.RunIf(action, cond)
	return s
}

// Raise puts the event that f returns in the queue of the machine itself, as
// [Transition.Raise] does.
func (s *Steps[S, C]) Raise[T any](name string, f func(C) T) *Steps[S, C] {
	s.t.Raise(name, func(c C, _ struct{}) T { return f(c) })
	return s
}

// RaiseAfter raises the event that f returns after a wait of d, as
// [Transition.RaiseAfter] does.
func (s *Steps[S, C]) RaiseAfter[T any](raise *Delayed[S, C], d time.Duration, f func(C) T) *Steps[S, C] {
	s.t.RaiseAfter(raise, d, func(c C, _ struct{}) T { return f(c) })
	return s
}

// CancelRaise cancels the events of raise that are not delivered yet, as
// [Transition.CancelRaise] does.
func (s *Steps[S, C]) CancelRaise(raise *Delayed[S, C]) *Steps[S, C] {
	s.t.CancelRaise(raise)
	return s
}

// CancelAfter cancels the pending delayed transitions of state, as
// [Transition.CancelAfter] does.
func (s *Steps[S, C]) CancelAfter(state S) *Steps[S, C] {
	s.t.CancelAfter(state)
	return s
}

// SendTo sends the message that f returns to the service that to addresses,
// while that service runs, as [Transition.SendTo] does.
//
// On entry, the message gets to a service that the state itself invokes. Use
// this to send a first message to the child machine of the state, or a first
// instruction to a service. On exit, a service that the state itself invokes
// gets the message before the core stops it.
func (s *Steps[S, C]) SendTo[M any](name string, to Mailbox[M], f func(C) M) *Steps[S, C] {
	s.t.SendTo(name, to, func(c C, _ struct{}) M { return f(c) })
	return s
}

// SendToKey sends to one run of a spawned service, as
// [Transition.SendToKey] does.
func (s *Steps[S, C]) SendToKey[M any](name string, to Mailbox[M], key func(C) string, f func(C) M) *Steps[S, C] {
	s.t.SendToKey(name, to, func(c C, _ struct{}) string { return key(c) }, func(c C, _ struct{}) M { return f(c) })
	return s
}

// Spawn starts a run of a spawned service, as [Transition.Spawn] does. On
// entry, the state is already active, so it can spawn its own services.
func (s *Steps[S, C]) Spawn(name string, service Spawnable[S, C], key func(C) string) *Steps[S, C] {
	s.t.Spawn(name, service, func(c C, _ struct{}) string { return key(c) })
	return s
}

// StopSpawned stops a run of a spawned service, as [Transition.StopSpawned]
// does.
func (s *Steps[S, C]) StopSpawned(name string, service Spawnable[S, C], key func(C) string) *Steps[S, C] {
	s.t.StopSpawned(name, service, func(c C, _ struct{}) string { return key(c) })
	return s
}

// Release releases a resource early, as [Transition.Release] does.
func (s *Steps[S, C]) Release(resource Releasable[S, C]) *Steps[S, C] {
	s.t.Release(resource)
	return s
}

// Emit gives the event that f returns to the code that runs the machine, as
// [Transition.Emit] does.
func (s *Steps[S, C]) Emit[T any](name string, f func(C) T) *Steps[S, C] {
	s.t.Emit(name, func(c C, _ struct{}) T { return f(c) })
	return s
}

// Action is a chart action that [Builder.Action] or [Builder.RequiresAction]
// declares. Transitions and step lists use this handle to run it.
type Action[S comparable, C any] struct {
	b    *build[S, C]
	name string
}

// Name returns the name of the action. [Binder.Action] uses this name to bind
// it.
func (a *Action[S, C]) Name() string { return a.name }

// Delayed is a delayed event that [Builder.Delayed] declares.
// [Transition.RaiseAfter] raises it, and [Transition.CancelRaise] cancels it.
type Delayed[S comparable, C any] struct {
	b    *build[S, C]
	name string
}

// Delayed declares a delayed event with a name. Use [Transition.RaiseAfter]
// or [Steps.RaiseAfter] to raise it. Use CancelRaise to cancel it. Both take
// the handle.
func (b *Builder[S, C]) Delayed(name string) *Delayed[S, C] {
	if name == "" {
		b.b.errorf("a delayed event has no name")
	}
	if b.b.raises[name] {
		b.b.errorf("delayed event %q is declared twice", name)
	}
	b.b.raises[name] = true

	return &Delayed[S, C]{b: b.b, name: name}
}

// Spawnable is a service that a chart can spawn runs of. It is the handle
// that [StateBuilder.SpawnMachine] returns, or that the declaration of a
// service with [Invocation.Spawned] returns.
type Spawnable[S comparable, C any] interface {
	invocation() *invocation[S, C]
}

func (i *Invocation[S, C, R]) invocation() *invocation[S, C] { return i.inv }

// Releasable is a resource that a transition can release early. It is the
// handle that [StateBuilder.Holds], [StateBuilder.HoldsRequired] or
// [StateBuilder.HoldsPermit] returns.
type Releasable[S comparable, C any] interface {
	resource() *invocation[S, C]
}

func (r *Resource[S, C, R]) resource() *invocation[S, C] { return r.inv }

// owns reports whether inv was declared on this chart.
func (b *build[S, C]) owns(inv *invocation[S, C]) bool {
	if inv == nil || inv.state == nil {
		return false
	}

	return inv.state == b.root || b.nodes[inv.state.id] == inv.state
}

// Retry is [Invocation.Retry], but it returns the [Receiving] handle.
func (r *Receiving[S, C, M, R]) Retry(policy Retry) *Receiving[S, C, M, R] {
	r.Invocation.Retry(policy)
	return r
}

// Spawned is [Invocation.Spawned], but it returns the [Receiving] handle.
func (r *Receiving[S, C, M, R]) Spawned() *Receiving[S, C, M, R] {
	r.Invocation.Spawned()
	return r
}

// Child is the handle of a child machine that [StateBuilder.InvokeMachine]
// or [StateBuilder.SpawnMachine] declares. The handle does these things:
//   - It routes the result of the child.
//   - It is the [Mailbox] that the parent uses to send events to the child.
//   - It reads the state of the child: [Child.Snapshot] in a driver, and
//     [Transition.WhenChildIn] in a guard.
type Child[S comparable, C any, S2 comparable, C2, R any] struct {
	*Receiving[S, C, any, R]

	machine *Machine[S2, C2]
}

// Retry is [Invocation.Retry], but it returns the handle of the child.
func (c *Child[S, C, S2, C2, R]) Retry(policy Retry) *Child[S, C, S2, C2, R] {
	c.Invocation.Retry(policy)
	return c
}

// Snapshot returns the current snapshot of the child, as the child last sent
// it up to parent. It also reports if there is a snapshot. It returns false
// if the child does not run, or if the child did not send up its first
// snapshot yet. For a spawned child, it returns the snapshot of the first run.
func (c *Child[S, C, S2, C2, R]) Snapshot(parent Snapshot[S, C]) (Snapshot[S2, C2], bool) {
	var none Snapshot[S2, C2]

	for _, inv := range parent.invocations {
		if inv.name != c.inv.name || inv.root != c.inv.state.root || (!inv.root && inv.state != c.inv.state.id) || len(inv.saved) == 0 {
			continue
		}

		snap, _, err := c.machine.Restore(inv.saved, time.Time{})
		if err != nil {
			return none, false
		}

		return snap, true
	}

	return none, false
}

// WhenChildIn limits the transition to the times when all of states are
// active in child, which is a child machine of this chart. The core reads the
// snapshot that the child last sent up. Because of this, the parent can make
// decisions from the state of its child, and the child does not have to report
// each move. The condition fails if the child does not run.
func (t *Transition[S, C, Ev]) WhenChildIn[S2 comparable, C2, R any](child *Child[S, C, S2, C2, R], states ...S2) *Transition[S, C, Ev] {
	switch {
	case child == nil || !t.b.owns(child.inv):
		t.b.errorf("state %q: WhenChildIn names a child machine of another chart", t.t.source.name)
		return t
	case len(states) == 0:
		t.b.errorf("state %q: WhenChildIn names no states", t.t.source.name)
	}

	cond := childCond[S, C]{inv: child.inv, states: nil}
	for _, state := range states {
		cond.states = append(cond.states, fmt.Sprint(state))
	}
	t.t.childConds = append(t.t.childConds, cond)

	return t
}

// Ending is the handle of a final state that declares an output with
// [FinalBuilder.Output]. It reads that output from a snapshot.
type Ending[S comparable, C, R any] struct {
	*FinalBuilder[S, C]

	output func(C) R
}

// Reached returns the output of the final state if that state is active in
// snap. It calculates the output from the context of snap. It also reports if
// the final state is active. For a top-level final state, this is the output
// of the finished machine.
func (e *Ending[S, C, R]) Reached(snap Snapshot[S, C]) (R, bool) {
	var zero R

	if !snap.In(e.n.id) {
		return zero, false
	}

	return e.output(snap.Context), true
}

// raisedType records the type of an event a raise produces, to be checked
// against the transitions that could take it.
func (b *build[S, C]) raisedType(t reflect.Type) {
	if t.Kind() != reflect.Interface {
		b.raisedTypes = append(b.raisedTypes, t)
	}
}

// hasSteps reports whether an entry or exit list has any steps, so that an
// empty one asked for and left empty declares the same chart as none.
func hasSteps[S comparable, C any](t *transition[S, C]) bool {
	return t != nil && len(t.steps) > 0
}
