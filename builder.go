package sc

import (
	"context"
	"fmt"
	"iter"
	"math/rand/v2"
	"reflect"
	"time"
)

// build accumulates the chart while the definition function runs.
type build[S comparable, C any] struct {
	// root is the implicit state containing every top-level state.
	root *node[S, C]

	// nodes indexes every declared state by its identifier.
	nodes map[S]*node[S, C]

	// ordered lists every declared state in document order.
	ordered []*node[S, C]

	// errs collects declaration errors for New to report together.
	errs []error

	// actions are the chart actions declared with Action.
	actions map[string]chartAction[S, C]

	// kinds records, for each service or resource name checked so far,
	// whether it names a resource.
	kinds map[string]bool

	// raises holds the names of the delayed events declared with Delayed,
	// and raisedTypes the types of the events raises produce.
	raises      map[string]bool
	raisedTypes []reflect.Type

	// sends holds the services sends address, and emits is set once any
	// transition emits.
	sends []sendTarget
	emits bool

	// emitTypes holds the types of the events Emit steps return, and
	// outputTypes those of the completion events final states report.
	emitTypes   []reflect.Type
	outputTypes []reflect.Type
}

func (b *build[S, C]) errorf(format string, args ...any) {
	b.errs = append(b.errs, fmt.Errorf(format, args...))
}

// Builder declares the child states of one state, the parent state. The
// Builder that [New] gives to your function declares the top-level states.
type Builder[S comparable, C any] struct {
	b      *build[S, C]
	parent *node[S, C]
}

// Action declares a chart action and returns its handle. You declare a chart
// action one time for the full chart. Any transition or step list can run it
// with the handle, through [Transition.Run] and [Steps.Run].
//
// The function fn receives the move that the action runs in. The move gives
// the state that runs the action, and the active states before and after the
// step. Thus one action can do work that depends on where the chart came from
// and where it went. For example, an action can record a status and then send
// a notification if the chart went across a boundary.
//
// The actions of a state must not use the name of a chart action. A chart
// action is like all actions: the driver can do its effect more than one
// time.
func (b *Builder[S, C]) Action(name string, fn func(ctx context.Context, c C, mv Move[S])) *Action[S, C] {
	switch _, declared := b.b.actions[name]; {
	case name == "":
		b.b.errorf("a chart action has no name")
	case declared:
		b.b.errorf("chart action %q is declared twice", name)
	default:
		b.b.actions[name] = fn
	}

	return &Action[S, C]{b: b.b, name: name}
}

// RequiresAction declares a chart action like [Builder.Action], but by name
// only. You give its implementation later with [Machine.Bind]. If the action
// runs before you bind it, it fails with [ErrNotBound].
func (b *Builder[S, C]) RequiresAction(name string) *Action[S, C] {
	return b.Action(name, nil)
}

// Initial names the child state that the chart enters when it enters the
// parent state without a more specific target. Each state that has children
// must have an initial state. Parallel states are different: they enter all
// of their children.
func (b *Builder[S, C]) Initial(s S) {
	if b.parent.kind == kindParallel {
		b.b.errorf("state %q: a parallel state has no initial state", b.parent.name)
		return
	}

	if b.parent.initialID != nil {
		b.b.errorf("state %q: initial state declared twice", b.parent.name)
		return
	}

	b.parent.initialID = &s
}

// Chart returns the builder for the chart itself. The chart is a state like
// all other states. It can do these things:
//   - Run entry and exit actions.
//   - Invoke services and child machines.
//   - Hold resources.
//   - Have timers and a budget.
//   - Take transitions.
//
// The declarations of the chart apply for the full life of the instance. Its
// entry actions run when the instance starts. Its exit actions run when the
// machine finishes. Its services run from the start to the finish. The chart
// takes its transitions in each state that does not handle the event first.
//
// A chart can have no other declarations. If you declare no states, the
// instance is always in the chart and in no other state.
//
// In snapshots, effects and hooks, the name of the chart is the name of the
// machine.
func (b *Builder[S, C]) Chart() *StateBuilder[S, C] {
	return &StateBuilder[S, C]{b: b.b, n: b.b.root}
}

// Regions makes the top-level states into regions that are all active at the
// same time. It does for the chart what [Builder.Parallel] does for the
// states in a parallel state. As a result, the chart has no initial state.
// The machine finishes when all regions finish. A region finishes when it is
// in one of its own final states, or when the region itself is a final state.
// Call Regions on the Builder that [New] gives to your function.
func (b *Builder[S, C]) Regions() {
	switch {
	case !b.parent.root:
		b.b.errorf("state %q: Regions applies to the chart itself; use Parallel for a state", b.parent.name)
	case b.parent.initialID != nil:
		b.b.errorf("a chart of regions has no initial state")
	default:
		b.parent.kind = kindParallel
	}
}

// On declares a transition on the parent state, as [StateBuilder.On] does. On
// the Builder that [New] gives, the parent state is the chart itself. Thus
// the chart takes the transition in all states, if no active state handles
// the event first. Use this for a cancellation or a shutdown that applies
// everywhere.
func (b *Builder[S, C]) On[Ev any]() *Transition[S, C, Ev] {
	return (&StateBuilder[S, C]{b: b.b, n: b.parent}).On[Ev]()
}

// OnAny declares a transition on the parent state for each event that the
// other transitions of that state do not take. It does what
// [StateBuilder.OnAny] does.
func (b *Builder[S, C]) OnAny() *Transition[S, C, any] {
	return (&StateBuilder[S, C]{b: b.b, n: b.parent}).OnAny()
}

// Always declares an eventless transition on the parent state, as
// [StateBuilder.Always] does.
func (b *Builder[S, C]) Always() *Transition[S, C, struct{}] {
	return (&StateBuilder[S, C]{b: b.b, n: b.parent}).Always()
}

// State declares a state. If you give a function, the state is a compound
// state, and you declare its children in that function. If you call State
// again with the same identifier under the same parent, State returns the
// state that exists. Thus you can declare the behaviour of a state across
// many statements.
func (b *Builder[S, C]) State(s S, children ...func(*Builder[S, C])) *StateBuilder[S, C] {
	n := b.declare(s, kindState)
	for _, fn := range children {
		fn(&Builder[S, C]{b: b.b, parent: n})
	}

	return &StateBuilder[S, C]{b: b.b, n: n}
}

// Parallel declares a state whose children are all active at the same time.
// Each state that the function regions declares directly is one region.
func (b *Builder[S, C]) Parallel(s S, regions func(*Builder[S, C])) *StateBuilder[S, C] {
	n := b.declare(s, kindParallel)
	regions(&Builder[S, C]{b: b.b, parent: n})

	return &StateBuilder[S, C]{b: b.b, n: n}
}

// Final declares a final state. When the chart enters a final state, the
// parent state completes. The chart then takes the [StateBuilder.OnDone]
// transitions of the parent state. When the chart enters a top-level final
// state, the machine completes. A final state that you declare directly in a
// parallel state is a region that is complete from the start.
//
// A chart whose top-level states are all final does not need an initial
// state. It starts in none of them. Its own transitions, services and timers
// do the work. The chart ends when one of them takes it to a final state.
func (b *Builder[S, C]) Final(s S) *FinalBuilder[S, C] {
	return &FinalBuilder[S, C]{b: b.b, n: b.declare(s, kindFinal)}
}

// History declares a history pseudo-state in the parent state. A transition
// that targets the history state enters again the states that were active
// when the chart last exited the parent state.
func (b *Builder[S, C]) History(s S, kind HistoryKind) *HistoryBuilder[S, C] {
	n := b.declare(s, kindHistory)
	n.historyKind = kind
	if kind != Shallow && kind != Deep {
		b.b.errorf("history state %q: unknown history kind %q", n.name, kind)
	}

	return &HistoryBuilder[S, C]{b: b.b, n: n}
}

func (b *Builder[S, C]) declare(s S, kind nodeKind) *node[S, C] {
	name := fmt.Sprint(s)

	if existing, ok := b.b.nodes[s]; ok {
		switch {
		case existing.parent != b.parent:
			b.b.errorf("state %q: declared under both %q and %q", name, existing.parent.name, b.parent.name)
		case existing.kind != kind:
			b.b.errorf("state %q: declared as both %s and %s", name, existing.kind, kind)
		case kind == kindHistory:
			b.b.errorf("history state %q: declared twice", name)
		}

		return existing
	}

	if b.parent.kind == kindFinal || b.parent.kind == kindHistory {
		b.b.errorf("state %q: %s state %q cannot have children", name, b.parent.kind, b.parent.name)
	}

	n := new(node[S, C])
	n.id = s
	n.name = name
	n.kind = kind
	n.parent = b.parent
	n.order = len(b.b.ordered) + 1
	b.parent.children = append(b.parent.children, n)
	b.b.nodes[s] = n
	b.b.ordered = append(b.b.ordered, n)

	return n
}

// FinalBuilder declares the behaviour of a final state.
type FinalBuilder[S comparable, C any] struct {
	b *build[S, C]
	n *node[S, C]
}

// Tag labels the final state, as [StateBuilder.Tag] does.
func (f *FinalBuilder[S, C]) Tag(tags ...string) *FinalBuilder[S, C] {
	f.n.tags = append(f.n.tags, tags...)
	return f
}

// Output declares the output that the final state reports when the chart
// enters it. The completion of the parent state carries this output. A
// transition that the parent state declares with [StateBuilder.OnDoneWith]
// receives a [Done] with these values:
//   - Its Result is the value that fn returns for the context. This is the
//     context after the entry assignments of the final state.
//   - Its ID is the name of the final state.
//
// For a top-level final state, the output is the output of the machine. The
// handle that Output returns reads the output from a snapshot with
// [Ending.Reached].
func (f *FinalBuilder[S, C]) Output[R any](fn func(C) R) *Ending[S, C, R] {
	if f.n.output != nil {
		f.b.errorf("final state %q: output declared twice", f.n.name)
	}

	name := f.n.name
	f.n.output = func(c C) any { return Done[R]{ID: name, Result: fn(c)} }
	f.n.outputValue = func(c C) any { return fn(c) }
	f.n.outputType = reflect.TypeFor[R]()
	f.b.outputTypes = append(f.b.outputTypes, reflect.TypeFor[Done[R]]())

	return &Ending[S, C, R]{FinalBuilder: f, output: fn}
}

// HistoryBuilder declares the behaviour of a history state.
type HistoryBuilder[S comparable, C any] struct {
	b *build[S, C]
	n *node[S, C]
}

// Default names the state that the chart enters if a transition targets the
// history state before the chart ever exited its parent. If there is no
// default, the chart uses the initial state of the parent. A history state of
// a parallel state can name a default in each of many regions with also. A
// deep history state above a parallel state can do this too.
func (h *HistoryBuilder[S, C]) Default(target S, also ...S) {
	if h.n.defaultID != nil {
		h.b.errorf("history state %q: default declared twice", h.n.name)
		return
	}

	h.n.defaultID = &target
	h.n.defaultAlso = also
}

// StateBuilder declares the behaviour of one state.
type StateBuilder[S comparable, C any] struct {
	b *build[S, C]
	n *node[S, C]
}

// On declares a transition that the chart takes when an event of type Ev
// arrives while the state is active. The chart tries the transitions for the
// same event in the order of their declaration. It takes the first transition
// whose guards hold. A transition of a descendant state has priority over a
// transition of its ancestor.
//
// Ev must be a concrete type. An event matches only a transition whose Ev is
// exactly the type of the event. Thus an interface type matches no event, and
// it is a declaration error.
func (s *StateBuilder[S, C]) On[Ev any]() *Transition[S, C, Ev] {
	t := newTransition[S, C, Ev](s.b, s.n, triggerEvent)
	t.t.eventType = reflect.TypeFor[Ev]()

	if t.t.eventType.Kind() == reflect.Interface {
		s.b.errorf("state %q: event type %s is an interface; events are matched by their concrete type", s.n.name, t.t.eventType)
	}

	return t
}

// OnAny declares a transition that the chart takes when an event arrives that
// no other transition of the state takes. This is one of these events:
//   - An event of a type for which the state has no On.
//   - An event whose On transitions all have guards that fail.
//
// The chart tries OnAny after the On transitions of the state. The order of
// declaration has no effect on this. The transitions of a descendant state
// still come first. Use OnAny for a state that must answer all events that it
// does not expect. For example, the transition can record the event, or go to
// an error state.
//
// Only two kinds of events count: events that a caller sends to the machine,
// and events that a stream service yields. Timers, service results and state
// completions never match.
//
// An action on the transition receives the event as it arrived. A driver that
// persists effects, such as pgdriver, can store an event only if the chart
// names its type in an On somewhere.
func (s *StateBuilder[S, C]) OnAny() *Transition[S, C, any] {
	t := newTransition[S, C, any](s.b, s.n, triggerEvent)
	t.t.wildcard = true

	return t
}

// Tag labels the state. A snapshot reports the tags of its active states with
// [Snapshot.HasTag]. Thus a caller can find what kind of state an instance is
// in, without a list of all the states of that kind. Examples of tags are
// "busy" and "terminal".
func (s *StateBuilder[S, C]) Tag(tags ...string) *StateBuilder[S, C] {
	s.n.tags = append(s.n.tags, tags...)
	return s
}

// Always declares an eventless transition. The chart takes it when the state
// is active and its guards hold, before the chart processes the next event.
//
// An eventless transition can change nothing: not the active states and not
// the context. The chart takes such a transition one time. Then the
// transition stays inactive until something else changes. Thus an action on a
// transition that stays in its state runs one time for each event, and not
// without end.
//
// Eventless transitions that continue to change things and never settle are a
// cycle. A cycle causes a panic.
func (s *StateBuilder[S, C]) Always() *Transition[S, C, struct{}] {
	return newTransition[S, C, struct{}](s.b, s.n, triggerAlways)
}

// After declares a transition that the chart takes after the state is active
// for the duration d. When the chart enters the state, the core returns an
// [EffectStartTimer] effect. When the chart leaves the state, the core
// returns an [EffectCancelTimer] effect. The driver delivers [TimerFired]
// when the timer is due.
func (s *StateBuilder[S, C]) After(d time.Duration) *Transition[S, C, struct{}] {
	if d < 0 {
		s.b.errorf("state %q: negative delay %s", s.n.name, d)
	}

	t := s.after()
	t.t.delay = d

	return t
}

// Delay declares a transition that the chart takes after the state is active
// for the duration that delay returns. The core calculates the duration from
// the context at the time when the chart enters the state. Thus a backoff can
// increase with a failure count. A negative duration fires the timer
// immediately. The function delay must be a pure function of the context,
// like a guard. To calculate the duration again after the context changes,
// enter the state again.
func (s *StateBuilder[S, C]) Delay(name string, delay func(C) time.Duration) *Transition[S, C, struct{}] {
	t := s.after()
	t.t.delayName = name
	t.t.delayFn = delay

	return t
}

// Deadline declares a transition that the chart takes at the time that
// deadline returns. For example, this can be a retry time that a server
// gives, or a renewal time in the context. The core calculates the time from
// the context at the time when the chart enters the state. If the time is
// already in the past, the timer fires immediately.
//
// The core never reads a clock. The timer effect has the time in [Effect.At],
// and the driver schedules the timer. The function deadline must be a pure
// function of the context.
func (s *StateBuilder[S, C]) Deadline(name string, deadline func(C) time.Time) *Transition[S, C, struct{}] {
	t := s.after()
	t.t.delayName = name
	t.t.deadlineFn = deadline

	return t
}

// after adds a delayed transition to the state.
func (s *StateBuilder[S, C]) after() *Transition[S, C, struct{}] {
	t := newTransition[S, C, struct{}](s.b, s.n, triggerAfter)
	t.t.afterIndex = s.n.afterCount
	s.n.afterCount++

	return t
}

// OnDone declares a transition that the chart takes when the state completes.
// A compound state completes when the chart enters one of its final children.
// A parallel state completes when all its regions complete.
func (s *StateBuilder[S, C]) OnDone() *Transition[S, C, struct{}] {
	return newTransition[S, C, struct{}](s.b, s.n, triggerStateDone)
}

// OnDoneWith declares a transition that the chart takes when the state
// completes, as [StateBuilder.OnDone] does. This transition also receives the
// output of the final state that the chart reached. The output is a [Done]
// with these values:
//   - Its Result is the value that the final state declares with
//     [FinalBuilder.Output].
//   - Its ID is the name of the final state.
//
// Thus one transition can identify which of many final states the chart
// reached. If that final state declares no output, the transition receives
// the zero value. If a final state of the state declares an output of a type
// other than R, it is a declaration error.
//
// A parallel state completes when all its regions complete, and it reports
// the outputs of all of them. R must then be map[string]any. The transition
// receives a Done with these values:
//   - Its ID is the name of the parallel state.
//   - Its Result maps the name of each region to the output of the final
//     state that the region ended in.
//   - The value for a region is nil if its final state declares no output.
//   - The value for a region that is itself parallel is a map of the same
//     kind.
func (s *StateBuilder[S, C]) OnDoneWith[R any]() *Transition[S, C, Done[R]] {
	t := newTransition[S, C, Done[R]](s.b, s.n, triggerStateDone)
	t.t.doneWith = reflect.TypeFor[R]()

	return t
}

// Within gives a shared time budget d to all services that run in the state
// or in a state inside it. The budget starts when the chart enters the state.
// When the budget ends, the driver cancels each service that still runs,
// through its context. That service fails with [context.DeadlineExceeded].
// This failure gets to the chart like all other failures. A service that
// starts after the budget ends fails immediately.
//
// If two states, one inside the other, both declare a budget, the earlier
// deadline applies. The deadline is part of the snapshot. Thus the deadline
// is not lost after a restart.
func (s *StateBuilder[S, C]) Within(d time.Duration) *StateBuilder[S, C] {
	if d <= 0 {
		s.b.errorf("state %q: budget %s is not positive", s.n.name, d)
	}

	if s.n.budget != 0 {
		s.b.errorf("state %q: budget declared twice", s.n.name)
	}

	s.n.budget = d

	return s
}

// sendTarget is a service a send addresses, with the type of message sent.
type sendTarget struct {
	service string
	message reflect.Type
}

// sendTo records a send to the service to names, and returns its name.
func (b *build[S, C]) sendTo[M any](to Mailbox[M]) string {
	if to == nil {
		b.errorf("a send has no service to send to")
		return ""
	}

	name := to.port().name
	b.sends = append(b.sends, sendTarget{service: name, message: reflect.TypeFor[M]()})

	return name
}

// Invoke declares a service that runs while the state is active. The service
// starts when the chart enters the state. It receives the context as it was
// at that time, and a [Run] for the data that the driver keeps for it. Its
// result arrives as a [Done] event and its error as a [Failed] event. The
// returned [Invocation] routes these events. If the chart leaves the state
// before the service finishes, the driver stops the service and discards its
// result.
//
// A driver can run a service more than one time. Thus fn must be idempotent:
// many runs must have the same effect as one run.
func (s *StateBuilder[S, C]) Invoke[R any](name string, fn func(ctx context.Context, run *Run, c C) (R, error)) *Invocation[S, C, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, adaptService(fn)))

	return &Invocation[S, C, R]{b: s.b, n: s.n, inv: inv}
}

// InvokeOn declares a service like [StateBuilder.Invoke]. This service also
// receives the event that the chart entered its state with, as an Ev. You
// write Ev, and Go infers R:
//
//	b.State("charging").InvokeOn[Checkout]("charge", func(ctx context.Context, run *sc.Run, o Order, ev Checkout) (Receipt, error) { ... })
//
// The chart must enter the state only with an event of type Ev. All other
// ways to enter the state are a declaration error:
//   - When the instance starts.
//   - As the initial state of the parent, or as a region of a parallel state.
//   - With a timer.
//   - With an event of a different type.
//
// A run that starts again receives the same event. This applies after a
// restart and after [Invocation.Retry]. If [Machine.StartIn] puts an instance
// in the state, the service gets the zero Ev.
func (s *StateBuilder[S, C]) InvokeOn[Ev, R any](name string, fn func(ctx context.Context, run *Run, c C, ev Ev) (R, error)) *Invocation[S, C, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, adaptService(func(ctx context.Context, run *Run, c C) (R, error) {
		return fn(ctx, run, c, triggerOf[Ev](ctx))
	})))
	inv.triggerType = reflect.TypeFor[Ev]()

	if inv.triggerType.Kind() == reflect.Interface {
		s.b.errorf("state %q: service %q is started by %s, an interface; use a concrete event type", s.n.name, name, inv.triggerType)
	}

	return &Invocation[S, C, R]{b: s.b, n: s.n, inv: inv}
}

// Receive declares a service like [StateBuilder.Invoke] that also receives
// messages of type M while it runs. The chart sends the messages with
// [Transition.SendTo] and the other send steps, to the handle that Receive
// returns. The service reads the messages from inbox. The inbox ends when the
// context of the service ends. The inbox still yields the messages from the
// step that stopped the service. If a service lets [MaxInbox] messages wait
// unread, the driver stops it, and it fails with [ErrInboxFull].
//
// Only the [Instance] and [Actor] drivers deliver messages. pgdriver rejects
// a chart that sends messages.
func (s *StateBuilder[S, C]) Receive[M, R any](name string, fn func(ctx context.Context, run *Run, c C, inbox iter.Seq[M]) (R, error)) *Receiving[S, C, M, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, adaptService(func(ctx context.Context, run *Run, c C) (R, error) {
		return fn(ctx, run, c, messages[M](ctx))
	})))
	inv.inboxType = reflect.TypeFor[M]()

	return &Receiving[S, C, M, R]{Invocation: &Invocation[S, C, R]{b: s.b, n: s.n, inv: inv}, Port: Port[M]{name: name}}
}

// ReceiveStream declares a service like [StateBuilder.Stream] that also
// receives messages of type M, as [StateBuilder.Receive] does. Use it for a
// service that receives instructions and also reports events while it runs.
func (s *StateBuilder[S, C]) ReceiveStream[M any](name string, fn func(ctx context.Context, run *Run, c C, inbox iter.Seq[M]) iter.Seq2[any, error]) *Receiving[S, C, M, struct{}] {
	inv := s.addInvocation(newInvocation[S, C, struct{}](name, adaptStream(func(ctx context.Context, run *Run, c C) iter.Seq2[any, error] {
		return fn(ctx, run, c, messages[M](ctx))
	})))
	inv.inboxType = reflect.TypeFor[M]()

	return &Receiving[S, C, M, struct{}]{Invocation: &Invocation[S, C, struct{}]{b: s.b, n: s.n, inv: inv}, Port: Port[M]{name: name}}
}

// Requires declares a service like [StateBuilder.Invoke], but only by its
// name and result type. You give its implementation later with
// [Machine.Bind]. Thus you can declare, export and explore the chart without
// the dependencies of the service. If the service runs before you bind it, it
// fails with [ErrNotBound].
func (s *StateBuilder[S, C]) Requires[R any](name string) *Invocation[S, C, R] {
	inv := s.addInvocation(newInvocation[S, C, R](name, nil))

	return &Invocation[S, C, R]{b: s.b, n: s.n, inv: inv}
}

// RequiresStream declares a service like [StateBuilder.Stream], but by name
// only. You give its implementation later with [Machine.Bind].
func (s *StateBuilder[S, C]) RequiresStream(name string) *Invocation[S, C, struct{}] {
	return s.Requires[struct{}](name)
}

// Holds declares a resource that the state holds while it is active. Examples
// are a connection, a listener, or a node that runs. The function acquire
// runs when the chart enters the state, like a service. The returned
// [Resource] routes the result: OnAcquired if acquire succeeds, OnError if it
// fails. The function release runs when the chart leaves the state, after the
// services in the state stop. The function release receives the reason why
// the chart left.
//
// The driver acquires the resources of a state one after the other, in the
// order of their declaration. Each acquisition starts after the driver holds
// the resource before it. Its acquire function can read the earlier resources
// with [Resource.Held]. If one acquisition fails, the driver does not acquire
// the resources after it.
//
// An acquisition does not stop other work. The services of the state, and of
// the child states that the chart enters with it, start at the same time as
// the first acquisition. These services can find that [Resource.Held] reports
// false. Put work that needs the resource in a child state that OnAcquired
// enters.
//
// The value never goes into the context or the snapshot. The driver keeps it.
// Services of the state, and of the states inside it, read the value with
// [Resource.Held]. Actions read it with [Resource.From]. The driver releases
// resources in the reverse order of their declaration. It releases the
// resources of inner states before the resources of outer states.
//
// If the chart leaves the state while acquire still runs, the driver lets
// acquire finish. Then the driver releases the value immediately. When the
// driver resumes a persisted instance that held the resource, it acquires the
// resource again before the services inside the state run. If that
// acquisition fails, the OnError transition decides what occurs.
//
// The function release cannot fail, like an action. Only the [Instance] and
// [Actor] drivers hold resources. pgdriver rejects a chart that declares a
// resource.
func (s *StateBuilder[S, C]) Holds[R any](name string, acquire func(ctx context.Context, run *Run, c C) (R, error), release func(ctx context.Context, value R, why Leave[S])) *Resource[S, C, R] {
	res := s.HoldsRequired[R](name)
	res.inv.run = adaptAcquire(acquire)
	res.inv.resource.release = adaptRelease(release)

	return res
}

// HoldsRequired declares a resource like [StateBuilder.Holds], but only by
// its name and type. You give its acquire and release functions later with
// [Machine.Bind].
func (s *StateBuilder[S, C]) HoldsRequired[R any](name string) *Resource[S, C, R] {
	inv := newInvocation[S, C, R](name, nil)
	inv.doneType = reflect.TypeFor[Acquired[R]]()
	inv.resource = &resourceSpec[S]{key: s.n.name + "\x00" + name, release: nil}
	s.addInvocation(inv)

	return &Resource[S, C, R]{b: s.b, n: s.n, inv: inv}
}

// Resource routes the result of the acquisition of a resource, and gives
// access to the value of the resource.
type Resource[S comparable, C, R any] struct {
	b   *build[S, C]
	n   *node[S, C]
	inv *invocation[S, C]
}

// OnAcquired declares a transition that the chart takes after the driver
// acquires the resource.
func (r *Resource[S, C, R]) OnAcquired() *Transition[S, C, Acquired[R]] {
	t := newTransition[S, C, Acquired[R]](r.b, r.n, triggerAcquired)
	t.t.eventType = reflect.TypeFor[Acquired[R]]()
	t.t.invocation = r.inv

	return t
}

// OnError declares a transition that the chart takes when the driver cannot
// acquire the resource. This applies to the first acquisition and to an
// acquisition after a restart. Each resource must have at least one OnError
// transition.
func (r *Resource[S, C, R]) OnError() *Transition[S, C, Failed] {
	t := newTransition[S, C, Failed](r.b, r.n, triggerInvokeError)
	t.t.eventType = reflect.TypeFor[Failed]()
	t.t.invocation = r.inv

	return t
}

// Stream declares a service that sends events to the machine while the state
// is active. The driver delivers each value that the sequence yields as an
// event. If the sequence yields a non-nil error, the service ends with a
// [Failed] event. If the sequence ends normally, the result is a [Done]
// event. When the chart exits the state, the driver stops the range over the
// sequence. Thus the deferred cleanup of the function runs.
func (s *StateBuilder[S, C]) Stream(name string, fn func(ctx context.Context, run *Run, c C) iter.Seq2[any, error]) *Invocation[S, C, struct{}] {
	inv := s.addInvocation(newInvocation[S, C, struct{}](name, adaptStream(fn)))

	return &Invocation[S, C, struct{}]{b: s.b, n: s.n, inv: inv}
}

// Invocation routes the result of an invoked service.
type Invocation[S comparable, C, R any] struct {
	b   *build[S, C]
	n   *node[S, C]
	inv *invocation[S, C]
}

// Retry makes the driver run the service again when it fails, after the waits
// that the policy gives. The driver does not report these failures to the
// chart. Meanwhile, the chart stays in the state. The chart sees a [Failed]
// event, through OnError, only for a failure that the policy does not cover.
// That is one of these failures:
//   - A failure that the If function of the policy rejects.
//   - The failure after Max retries.
//   - A failure whose retry cannot start before a [StateBuilder.Within]
//     budget around the state ends.
//
// If the chart leaves the state, the driver abandons the retry.
//
// The count of failures is in the snapshot, not in the context. Thus the
// count is not lost after a restart. The count goes back to zero each time
// the chart enters the state.
func (i *Invocation[S, C, R]) Retry(policy Retry) *Invocation[S, C, R] {
	switch {
	case i.inv.retry != nil:
		i.b.errorf("state %q: service %q has more than one retry policy", i.n.name, i.inv.name)
	case policy.Max != Forever && policy.Max <= 0:
		i.b.errorf("state %q: service %q retries %d times; use a positive number or Forever", i.n.name, i.inv.name, policy.Max)
	case policy.First < 0 || policy.Cap < 0:
		i.b.errorf("state %q: service %q has a negative retry wait", i.n.name, i.inv.name)
	case policy.Cap > 0 && policy.First == 0:
		i.b.errorf("state %q: service %q has a retry cap but no first wait to grow from", i.n.name, i.inv.name)
	case policy.Max == Forever && policy.First == 0:
		i.b.errorf("state %q: service %q retries forever without a wait", i.n.name, i.inv.name)
	case policy.Jitter < 0 || policy.Jitter > 1:
		i.b.errorf("state %q: service %q has retry jitter %v, which is not between 0 and 1", i.n.name, i.inv.name, policy.Jitter)
	}

	i.inv.retry = &policy

	return i
}

// Spawned makes the service one that can have many runs at the same time
// while its state is active. Each run has a key, for example one key for each
// upload, host, or job. No run starts when the chart enters the state. A
// transition starts a run with [Transition.Spawn] and stops a run with
// [Transition.StopSpawned]. When the chart leaves the state, the driver stops
// all the runs.
//
// The chart takes the OnDone and OnError transitions of the service for each
// run. These transitions identify the run with [SpawnKey] on the ID of the
// event. The service reads its own key with [Run.Key].
func (i *Invocation[S, C, R]) Spawned() *Invocation[S, C, R] {
	i.inv.spawned = true
	return i
}

// OnDone declares a transition that the chart takes when the service
// succeeds.
func (i *Invocation[S, C, R]) OnDone() *Transition[S, C, Done[R]] {
	t := newTransition[S, C, Done[R]](i.b, i.n, triggerInvokeDone)
	t.t.eventType = reflect.TypeFor[Done[R]]()
	t.t.invocation = i.inv

	return t
}

// OnError declares a transition that the chart takes when the service fails.
// Each invocation must have at least one OnError transition.
func (i *Invocation[S, C, R]) OnError() *Transition[S, C, Failed] {
	t := newTransition[S, C, Failed](i.b, i.n, triggerInvokeError)
	t.t.eventType = reflect.TypeFor[Failed]()
	t.t.invocation = i.inv

	return t
}

// Transition declares what one transition does. A transition without a target
// changes the context or runs actions. It does not change the active states.
type Transition[S comparable, C, Ev any] struct {
	b *build[S, C]
	t *transition[S, C]
}

// newTransition adds a transition with the given trigger to source.
func newTransition[S comparable, C, Ev any](b *build[S, C], source *node[S, C], trigger triggerKind) *Transition[S, C, Ev] {
	t := new(transition[S, C])
	t.source = source
	t.trigger = trigger
	t.sendTargets = map[int]string{}
	t.keys = map[int]func(C, any) string{}
	source.transitions = append(source.transitions, t)

	return &Transition[S, C, Ev]{b: b, t: t}
}

// event recovers the typed event from the value the core passes around.
// Triggers without an event value pass struct{}, which is also their Ev.
func event[Ev any](v any) Ev {
	ev, _ := v.(Ev)
	return ev
}

// When limits the transition to the events and contexts for which g holds. A
// transition can have many guards, and each guard has its own name. All the
// guards must hold. The chart checks them in the order of their declaration.
func (t *Transition[S, C, Ev]) When(name string, g func(C, Ev) bool) *Transition[S, C, Ev] {
	t.t.guards = append(t.t.guards, guard[C]{
		name: name,
		fn:   func(c C, ev any, _ time.Time) bool { return g(c, event[Ev](ev)) },
	})

	return t
}

// WhenAt is [Transition.When] for a guard that needs the current time. For
// example, the guard can find if a retry time is in the past. For an event,
// now is the time that the driver gives. When a timer fires, now is the due
// time of that timer. Like all guards, g must be a pure function of its
// arguments.
func (t *Transition[S, C, Ev]) WhenAt(name string, g func(c C, ev Ev, now time.Time) bool) *Transition[S, C, Ev] {
	t.t.guards = append(t.t.guards, guard[C]{
		name: name,
		fn:   func(c C, ev any, now time.Time) bool { return g(c, event[Ev](ev), now) },
	})

	return t
}

// WhenEvent is [Transition.When] for a guard that needs the current event, of
// any type. The value of ev depends on the transition:
//   - On an eventless transition, which you declare with Always, ev is the
//     event that started the step, or the raised event that the chart
//     processes. A typed guard on that transition cannot see this event. The
//     value is nil if a timer or the start of the instance started the step.
//   - On other transitions, ev is the event of the transition.
//
// An action on an eventless transition also has the event, in its effect.
// This is true only if the chart names the type of the event in an On
// somewhere.
func (t *Transition[S, C, Ev]) WhenEvent(name string, g func(c C, ev any) bool) *Transition[S, C, Ev] {
	t.t.guards = append(t.t.guards, guard[C]{
		name: name,
		fn:   func(c C, ev any, _ time.Time) bool { return g(c, ev) },
	})

	return t
}

// WhenIn limits the transition to the times when all the given states are
// active. Thus one parallel region can react to the state of a different
// region, which a guard cannot see. WhenIn and [Transition.When] work
// together: both must hold.
func (t *Transition[S, C, Ev]) WhenIn(states ...S) *Transition[S, C, Ev] {
	if len(states) == 0 {
		t.b.errorf("state %q: WhenIn names no states", t.t.source.name)
	}

	t.t.inIDs = append(t.t.inIDs, states...)

	return t
}

// Jitter makes the driver change the wait of a delayed transition by a random
// amount. The wait can become shorter or longer, by a maximum of fraction of
// the wait. Thus many instances that start at the same time do not fire at
// the same time. Jitter applies only to transitions that you declare with
// After, Delay, or Deadline. The value fraction must be between 0 and 1. The
// core stays deterministic: the effect has the fraction in [Effect.Jitter],
// and the driver selects the random amount.
func (t *Transition[S, C, Ev]) Jitter(fraction float64) *Transition[S, C, Ev] {
	if t.t.trigger != triggerAfter {
		t.b.errorf("state %q: Jitter applies only to delayed transitions", t.t.source.name)
	}
	if fraction < 0 || fraction > 1 {
		t.b.errorf("state %q: jitter %v is not between 0 and 1", t.t.source.name, fraction)
	}

	t.t.jitter = fraction

	return t
}

// WhenNotIn limits the transition to the times when none of the given states
// is active. It works together with [Transition.When] and
// [Transition.WhenIn]: all of them must hold.
func (t *Transition[S, C, Ev]) WhenNotIn(states ...S) *Transition[S, C, Ev] {
	if len(states) == 0 {
		t.b.errorf("state %q: WhenNotIn names no states", t.t.source.name)
	}

	t.t.notInIDs = append(t.t.notInIDs, states...)

	return t
}

// Assign replaces the context with the value that f returns. Assignments,
// actions, and raises run in the order of their declaration. Thus a later
// step sees the context that an earlier step made.
func (t *Transition[S, C, Ev]) Assign(name string, f func(C, Ev) C) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepAssign,
		name:   name,
		assign: func(c C, ev any, _ time.Time, _ *rand.Rand) C { return f(c, event[Ev](ev)) },
		do:     nil,
		raise:  nil,
		random: false,
	})

	return t
}

// AssignAt is [Transition.Assign] for an assignment that needs the current
// time. For example, the assignment can record when something occurred, or
// calculate the next retry from that time. The value now is the same time
// that [Transition.WhenAt] sees.
func (t *Transition[S, C, Ev]) AssignAt(name string, f func(c C, ev Ev, now time.Time) C) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepAssign,
		name:   name,
		assign: func(c C, ev any, now time.Time, _ *rand.Rand) C { return f(c, event[Ev](ev), now) },
		do:     nil,
		raise:  nil,
		random: false,
	})

	return t
}

// AssignRand is [Transition.AssignAt] for an assignment that also needs
// random numbers. For example, the assignment can spread a refresh time, so
// that instances that start at the same time do not all act at the same time.
//
// The core makes the source rnd from the seed of the instance and the number
// of events that the instance processed. Thus the assignment stays a pure
// function of its arguments. If you replay the same events, you get the same
// random numbers, and exploration stays deterministic. For a timer, use
// [Transition.Jitter] instead.
func (t *Transition[S, C, Ev]) AssignRand(name string, f func(c C, ev Ev, now time.Time, rnd *rand.Rand) C) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepAssign,
		name:   name,
		assign: func(c C, ev any, now time.Time, rnd *rand.Rand) C { return f(c, event[Ev](ev), now, rnd) },
		do:     nil,
		raise:  nil,
		random: true,
	})

	return t
}

// Do adds an action. The core does not run the action. It returns an
// [EffectAction] that has the context and the event that the action must see.
// The name must be unique among the actions of the source state.
func (t *Transition[S, C, Ev]) Do(name string, f func(context.Context, C, Ev)) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepDo,
		name:   name,
		assign: nil,
		do:     func(ctx context.Context, c C, ev any) { f(ctx, c, event[Ev](ev)) },
		raise:  nil,
		random: false,
	})

	return t
}

// Run runs chart actions as steps of the transition. You declare these
// actions with [Builder.Action]. They run in sequence with the other steps of
// the transition.
func (t *Transition[S, C, Ev]) Run(actions ...*Action[S, C]) *Transition[S, C, Ev] {
	for _, action := range actions {
		t.t.steps = append(t.t.steps, step[C]{kind: stepRun, name: t.action(action), assign: nil, do: nil, raise: nil, random: false})
	}

	return t
}

// action returns the name of a chart action, reporting one of another chart.
func (t *Transition[S, C, Ev]) action(a *Action[S, C]) string {
	if a == nil || a.b != t.b {
		t.b.errorf("state %q: runs a chart action of another chart", t.t.source.name)
		return ""
	}

	return a.name
}

// RunIf runs a chart action as a step of the transition, as Run does. But the
// action runs only if cond holds for the [Move] of the step. The core makes
// this decision. Thus the action is in the effects of the step only when it
// runs. The function cond must be a pure function of the move.
func (t *Transition[S, C, Ev]) RunIf(action *Action[S, C], cond func(Move[S]) bool) *Transition[S, C, Ev] {
	if t.t.runConds == nil {
		t.t.runConds = map[int]func(Move[S]) bool{}
	}
	t.t.runConds[len(t.t.steps)] = cond
	t.t.steps = append(t.t.steps, step[C]{kind: stepRun, name: t.action(action), assign: nil, do: nil, raise: nil, random: false})

	return t
}

// Raise puts the event that f returns in a queue for the machine itself. The
// machine processes the event in the same call to [Machine.Transition], after
// the current transitions complete. Thus one parallel region can send a
// signal to a different region. If no transition of the chart takes the type
// T, it is a declaration error.
func (t *Transition[S, C, Ev]) Raise[T any](name string, f func(C, Ev) T) *Transition[S, C, Ev] {
	t.b.raisedType(reflect.TypeFor[T]())
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepRaise,
		name:   name,
		assign: nil,
		do:     nil,
		raise:  func(c C, ev any) any { return f(c, event[Ev](ev)) },
		random: false,
	})

	return t
}

// Release releases the resource immediately, but the state that holds it
// stays active. Use it, for example, for a secret that only the first step in
// the state needs, or for a lock that later steps must not keep. The release
// function runs as it does when the chart leaves the state. After that,
// [Resource.Held] reports false. The driver does not acquire the resource
// again until the chart enters the state again.
//
// Release does nothing if the driver does not hold the resource. If an
// acquisition is not complete, the driver releases the resource when it
// arrives. The chart does not take OnAcquired for that acquisition.
func (t *Transition[S, C, Ev]) Release(resource Releasable[S, C]) *Transition[S, C, Ev] {
	var inv *invocation[S, C]
	if resource != nil {
		inv = resource.resource()
	}

	name := ""
	if !t.b.owns(inv) {
		t.b.errorf("state %q: releases a resource of another chart", t.t.source.name)
	} else {
		name = inv.name
	}

	t.t.steps = append(t.t.steps, step[C]{kind: stepRelease, name: name, assign: nil, do: nil, raise: nil, random: false})

	return t
}

// Spawn starts a run of the spawned service that the handle names. The run
// has the key that key returns, and it gets the context as it is at this
// step. Spawn does nothing in these conditions:
//   - A run with that key already runs.
//   - The state of the service is not active. Thus a transition that enters
//     the state cannot spawn in the same step.
func (t *Transition[S, C, Ev]) Spawn(name string, service Spawnable[S, C], key func(C, Ev) string) *Transition[S, C, Ev] {
	return t.keyed(stepSpawn, name, service, key)
}

// StopSpawned stops the run of the spawned service that the handle names and
// that has the key that key returns. After that, the chart drops all the
// reports of that run. StopSpawned does nothing if there is no such run.
func (t *Transition[S, C, Ev]) StopSpawned(name string, service Spawnable[S, C], key func(C, Ev) string) *Transition[S, C, Ev] {
	return t.keyed(stepStop, name, service, key)
}

// SendToKey is [Transition.SendTo] for one run of a spawned service: the run
// with the key that key returns.
func (t *Transition[S, C, Ev]) SendToKey[M any](name string, to Mailbox[M], key func(C, Ev) string, f func(C, Ev) M) *Transition[S, C, Ev] {
	t.SendTo(name, to, f)
	t.t.keys[len(t.t.steps)-1] = func(c C, ev any) string { return key(c, event[Ev](ev)) }

	return t
}

// keyed adds a step about one run of a spawned service.
func (t *Transition[S, C, Ev]) keyed(kind stepKind, name string, service Spawnable[S, C], key func(C, Ev) string) *Transition[S, C, Ev] {
	var inv *invocation[S, C]
	if service != nil {
		inv = service.invocation()
	}

	switch {
	case !t.b.owns(inv):
		t.b.errorf("state %q: %s names a service of another chart", t.t.source.name, kind)
	case !inv.spawned:
		t.b.errorf("state %q: %s names service %q, which is not declared Spawned", t.t.source.name, kind, inv.name)
	}

	serviceName := ""
	if inv != nil {
		serviceName = inv.name
	}
	t.t.sendTargets[len(t.t.steps)] = serviceName
	t.t.keys[len(t.t.steps)] = func(c C, ev any) string { return key(c, event[Ev](ev)) }
	t.t.steps = append(t.t.steps, step[C]{kind: kind, name: name, assign: nil, do: nil, raise: nil, random: false})

	return t
}

// SendTo sends the message that f returns to the service that to addresses,
// while that service runs. The handle to is a handle that one of these
// functions returned:
//   - [StateBuilder.Receive]
//   - [StateBuilder.ReceiveStream]
//   - [StateBuilder.InvokeMachine]
//   - [StateBuilder.SpawnMachine]
//
// Go infers M from the handle. Thus f must make the message type that the
// service reads. A chart uses SendTo to communicate with work that is in
// progress.
//
// The message goes to each run of that service that runs after the step
// settles. This includes a run that the step itself starts. Thus a transition
// into a state can greet the service that the state invokes. A service that
// the step stops gets the message first: the message is still in its inbox
// after its context ends. If no such service runs, the driver drops the
// message. This occurs if the state of the service is not active, or if the
// service is finished.
//
// Only the [Instance] and [Actor] drivers deliver messages. pgdriver rejects
// a chart that sends messages, because the service runs in a different
// process from the process of the event.
func (t *Transition[S, C, Ev]) SendTo[M any](name string, to Mailbox[M], f func(C, Ev) M) *Transition[S, C, Ev] {
	service := t.b.sendTo(to)
	t.t.sendTargets[len(t.t.steps)] = service

	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepSend,
		name:   name,
		assign: nil,
		do:     nil,
		raise:  func(c C, ev any) any { return f(c, event[Ev](ev)) },
		random: false,
	})

	return t
}

// Emit gives the event that f returns to the code that runs the machine. The
// receiver is one of these:
//   - The listeners for T that the driver received with [Listen].
//   - The parent of a machine that runs through [StateBuilder.InvokeMachine].
//     The parent receives the event as one of its own events.
//   - The listeners that pgdriver received with its Listen option. These
//     listeners receive the event after the step commits.
//
// A machine uses Emit to report progress, and it does not need to know who
// listens. If there is no listener, the driver drops the event.
//
// T must be a concrete type. Thus a driver that stores effects can name the
// event and load it again.
func (t *Transition[S, C, Ev]) Emit[T any](name string, f func(C, Ev) T) *Transition[S, C, Ev] {
	emitted := reflect.TypeFor[T]()
	if emitted.Kind() == reflect.Interface {
		t.b.errorf("state %q: emit %q returns %s, an interface; emit a concrete type", t.t.source.name, name, emitted)
	} else {
		t.b.emitTypes = append(t.b.emitTypes, emitted)
	}

	t.b.emits = true
	t.t.steps = append(t.t.steps, step[C]{
		kind:   stepEmit,
		name:   name,
		assign: nil,
		do:     nil,
		raise:  func(c C, ev any) any { return f(c, event[Ev](ev)) },
		random: false,
	})

	return t
}

// RaiseAfter puts the event that f returns in a queue for the machine itself.
// The machine gets the event after the duration d, as if a caller sent it at
// that time. This is different from a delayed transition: the event is not
// part of a state. The chart gets the event in the state that it is in at
// that time. Two things stop the delivery:
//   - A [Transition.CancelRaise] of the same delayed event runs first.
//   - The machine finishes.
//
// The driver keeps the timer, and the event waits in the snapshot. Thus the
// event is not lost after a restart. The handle raise is the handle that
// [Builder.Delayed] returned.
//
// If no transition of the chart takes the type T, it is a declaration error.
// The reason is that a stored snapshot must name the event that waits.
func (t *Transition[S, C, Ev]) RaiseAfter[T any](raise *Delayed[S, C], d time.Duration, f func(C, Ev) T) *Transition[S, C, Ev] {
	name := t.delayed(raise)
	if d < 0 {
		t.b.errorf("state %q: raise %q has a negative delay %s", t.t.source.name, name, d)
	}

	if t.t.raiseDelays == nil {
		t.t.raiseDelays = map[int]time.Duration{}
	}
	t.t.raiseDelays[len(t.t.steps)] = d

	return t.Raise(name, f)
}

// CancelRaise cancels the events that raise still waits to deliver. It does
// nothing if there are no such events.
func (t *Transition[S, C, Ev]) CancelRaise(raise *Delayed[S, C]) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{kind: stepCancel, name: t.delayed(raise), assign: nil, do: nil, raise: nil, random: false})

	return t
}

// delayed returns the name of a delayed event, reporting one of another
// chart.
func (t *Transition[S, C, Ev]) delayed(raise *Delayed[S, C]) string {
	if raise == nil || raise.b != t.b {
		t.b.errorf("state %q: uses a delayed event of another chart", t.t.source.name)
		return ""
	}

	return raise.name
}

// To sets the state that the transition enters. If the target is a history
// state, the chart enters again the states that were last active in the
// parent of the history state.
//
// If the target is the source state or is inside it, the chart does not exit
// and enter the source state again. To change this, also call
// [Transition.Reenter].
//
// You can give more states in also. The chart then enters each of them too.
// Thus one transition can put many regions of a parallel state where it wants
// them. The targets must be in different regions of one parallel state.
func (t *Transition[S, C, Ev]) To(target S, also ...S) *Transition[S, C, Ev] {
	if t.t.targetID != nil {
		t.b.errorf("state %q: To is called more than once on a transition", t.t.source.name)
		return t
	}

	t.t.targetID = &target
	t.t.alsoIDs = also

	return t
}

// CancelAfter cancels the timers that still wait for the delayed transitions
// that state declares. These are the transitions that [StateBuilder.After],
// [StateBuilder.Delay] and [StateBuilder.Deadline] declare. The chart does
// not leave state. The chart does not take these transitions while the state
// stays active. The timers start again the next time that the chart enters
// the state. CancelAfter does nothing if state is not active.
func (t *Transition[S, C, Ev]) CancelAfter(state S) *Transition[S, C, Ev] {
	t.t.steps = append(t.t.steps, step[C]{kind: stepCancelAfter, name: fmt.Sprint(state), assign: nil, do: nil, raise: nil, random: false})

	return t
}

// ToChart makes the chart itself the target of the transition. The chart
// exits all the active states. Then it enters its initial states again, as
// when the instance started, with the current context. The chart itself stays
// entered. Thus its own services and timers continue. To start them again
// too, add [Transition.Reenter]. This also runs the exit and entry actions of
// the chart.
func (t *Transition[S, C, Ev]) ToChart() *Transition[S, C, Ev] {
	t.t.toChart = true
	return t
}

// Stay marks a transition that consumes its event and does nothing else. It
// has no target and no steps. A chart uses Stay to show that it expects an
// event and drops it on purpose. An example is the error of a service whose
// failure is not important. Without Stay, a transition with no target and no
// steps is a declaration error.
//
// The chart never gets to a transition that you declare after the Stay
// transition for the same trigger. This is not true if the Stay transition
// has a guard that fails.
func (t *Transition[S, C, Ev]) Stay() *Transition[S, C, Ev] {
	t.t.stay = true
	return t
}

// Reenter makes the transition exit and enter its source state again, also
// when the target is the source or one of its descendants. Exit and entry
// actions run again, timers start again, and services start again.
func (t *Transition[S, C, Ev]) Reenter() *Transition[S, C, Ev] {
	t.t.reenter = true
	return t
}
