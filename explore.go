package sc

import (
	"context"
	"encoding/json"
	"fmt"
	"iter"
	"reflect"
	"slices"
	"strings"
	"time"
)

// defaultExploreStart is the time an exploration starts at when it sets none.
// Its value carries no meaning; a fixed one keeps reports reproducible.
var defaultExploreStart = time.Date(2000, time.January, 1, 0, 0, 0, 0, time.UTC)

// defaultMaxStates bounds an exploration that sets no limit of its own. A
// chart meant to be explored has a few hundred states at most; a count this
// large means the context is unbounded, and stopping there reports that
// instead of exhausting memory.
const defaultMaxStates = 100_000

// Outcome makes an event for one run of an invoked service. It gets the ID
// of the run and the exploration time when the result arrives. Thus, a
// result can contain times relative to that time, for example a certificate
// that is valid for 90 days. [Succeeds], [Fails] and [Emits] make the usual
// outcomes.
type Outcome func(id string, now time.Time) any

// Succeeds is the outcome of a service that returns result.
func Succeeds[R any](result R) Outcome {
	return func(id string, _ time.Time) any { return Done[R]{ID: id, Result: result} }
}

// Fails is the outcome of a service that returns err.
func Fails(err error) Outcome {
	return func(id string, _ time.Time) any { return Failed{ID: id, Err: err} }
}

// Acquires is the outcome of a resource acquisition that gets value. Put it
// in Outcomes, below the name of the resource. Put [Fails] with it for an
// acquisition that fails.
func Acquires[R any](value R) Outcome {
	return func(id string, _ time.Time) any { return Acquired[R]{ID: id, Value: value} }
}

// Emits is the outcome of a [StateBuilder.Stream] service that yields ev.
func Emits(ev any) Outcome {
	return func(id string, _ time.Time) any { return Emitted{ID: id, Event: ev} }
}

// Invariant is a named property that must be true in all reachable states.
// Set Holds. For a property that changes with time, set HoldsAt.
type Invariant[S comparable, C any] struct {
	// Name identifies the invariant in the report.
	Name string

	// Holds reports if the property is true for snap.
	Holds func(snap Snapshot[S, C]) bool

	// HoldsAt reports if the property is true for snap at now. now is the
	// exploration time in that state. An example property is "ready only
	// while the certificate is not expired".
	HoldsAt func(snap Snapshot[S, C], now time.Time) bool
}

// holds reports whether the invariant is true of snap at now.
func (inv Invariant[S, C]) holds(snap Snapshot[S, C], now time.Time) bool {
	if inv.Holds != nil && !inv.Holds(snap) {
		return false
	}

	return inv.HoldsAt == nil || inv.HoldsAt(snap, now)
}

// Step is one transition. An exploration makes steps, and a driver reports
// steps to [Hooks.Transition].
type Step[S comparable, C any] struct {
	// At is the time of the event.
	At time.Time

	// From is the snapshot before the event.
	From Snapshot[S, C]

	// Event is the event that the machine processed.
	Event any

	// To is the snapshot after the event.
	To Snapshot[S, C]

	// Effects are the effects that the transition made.
	Effects []Effect[C]
}

// Accepted reports if the step changed the instance. It is false for an
// event that [Machine.Transition] ignored.
func (s Step[S, C]) Accepted() bool { return s.To.seq != s.From.seq }

// Service returns the service run that sent the result or stream event of
// the step. The returned value is the run as From lists it. For all other
// events, Service returns false.
func (s Step[S, C]) Service() (RunningService[S], bool) {
	var none RunningService[S]

	var id string
	switch e := s.Event.(type) {
	case invocationResult:
		id = e.invocationID()
	case Emitted:
		id = e.ID
	default:
		return none, false
	}

	for _, run := range s.From.Services() {
		if run.ID == id {
			return run, true
		}
	}

	return none, false
}

// StepInvariant is a named property that must be true for all transitions.
// An example property is "the chart emits this action only from these
// states".
type StepInvariant[S comparable, C any] struct {
	// Name identifies the invariant in the report.
	Name string

	// Holds reports if the property is true for step.
	Holds func(step Step[S, C]) bool
}

// Goal is a condition that the chart must always be able to get to again.
// An example is "the gateway is online". From each reachable state, a
// sequence of events, service outcomes and timers must go to a state where
// the condition is true.
//
// A goal is weaker than "the condition is true at some time in the future".
// That property also needs help from the environment. To make a model of
// the environment, limit Events and Outcomes to the things that you think
// the environment does.
type Goal[S comparable, C any] struct {
	// Name identifies the goal in the report.
	Name string

	// Holds reports if the goal is true in snap.
	Holds func(snap Snapshot[S, C]) bool

	// Ignoring lists events with types that the goal fully ignores. An
	// example is an event from an operator who closes the instance. The
	// exploration checks the goal only on the states that it can reach
	// without these events. The paths back to the goal cannot use these
	// events.
	Ignoring []any
}

// Exploration describes the environment of an exploration and the
// properties to check.
type Exploration[S comparable, C any] struct {
	// Events are the external events that the exploration tries in all
	// reachable states.
	Events []any

	// Outcomes lists the outcomes for each service name. The exploration
	// tries these outcomes each time that the service is active. A service
	// with no outcomes never reports. This is a model of a service that
	// hangs.
	Outcomes map[string][]Outcome

	// Invariants are the properties that the exploration checks in all
	// reachable states.
	Invariants []Invariant[S, C]

	// Steps are the properties that the exploration checks on all
	// transitions.
	Steps []StepInvariant[S, C]

	// Goals are the goals that the exploration checks after it knows all
	// of the state space. A goal fails if a reachable state can never get
	// to the goal again. The report shows each failed goal as a violation
	// with the name "goal: " and then the goal name. The violation has a
	// shortest trace to such a state. If the exploration is truncated, it
	// does not check goals.
	Goals []Goal[S, C]

	// MaxStates stops the exploration when it finds this number of
	// different states. If MaxStates is zero, the limit is 100,000.
	MaxStates int

	// Key returns the part of the context that makes one state different
	// from a different state. Two snapshots are the same state if these
	// items are equal:
	//   - The active states.
	//   - The history.
	//   - The services that did not report.
	//   - The time that remains on each pending timer.
	//   - The results of Key.
	//
	// Use Key if the context contains values that do not marshal to JSON,
	// for example a TLS certificate. Also use it for values that have no
	// limit but do not change what the chart can do, for example
	// timestamps. now is the exploration clock in that state. Thus, a key
	// can keep the distance from a stored time to now, instead of the time.
	// The result must marshal to JSON. If Key is nil, the exploration uses
	// all of the context.
	//
	// Be careful: a Key must keep all the data that a guard or delay reads.
	// If not, the exploration merges states that are different, and it
	// does not find some of them.
	Key func(c C, now time.Time) any

	// Start is the start time of the exploration. If Start is zero, the
	// exploration uses a fixed time in the year 2000.
	Start time.Time

	// Starts are more contexts that the exploration starts instances
	// from. It also uses the context that you give to Explore. Use Starts
	// for a chart that operates differently for different inputs. The
	// exploration covers the states that it can reach from all of them.
	Starts []C

	// CheckKey checks that Key merges only the states that operate the
	// same. Sometimes the key of a new state is equal to the key of a known
	// state, but the snapshots are different. Then, the exploration tries
	// all candidates from the two states. If the keys of their next states
	// are different, it reports a violation with the name "key". The cost
	// is a second expansion of each merged state.
	CheckKey bool

	// CheckDeterminism runs each transition two times. If the two results
	// are different, it reports a violation with the name "deterministic".
	// It finds guards, assignments and delays that do one of these things:
	//   - Read the clock.
	//   - Make random numbers.
	//   - Iterate over a map.
	//   - Keep their own state.
	//
	// These things make explorations and replays incorrect. This check
	// makes the cost of the exploration two times larger.
	CheckDeterminism bool

	// Waits are lengths of time that the exploration lets go by in all
	// reachable states, with no event. During a wait, the timers that
	// become due fire. Events, service outcomes and timers already occur at
	// their own times. Use waits for guards that compare the context with
	// the current time. An example is an expiry time, where an event can
	// arrive before or after it.
	Waits []time.Duration
}

// Trace is a reachable state and the shortest sequence of events that goes
// to it from the start. To get Snapshot again, do these steps:
//  1. Call [Machine.Start] with Initial at Start to get a snapshot.
//  2. Give that snapshot and Events to [Machine.Fold].
type Trace[S comparable, C any] struct {
	// Start is the time at which the exploration started the instance.
	Start time.Time

	// Initial is the start context of the instance.
	Initial C

	// Events are the events in the order of processing. Each event has
	// its time. A nil event only lets time go by.
	Events []Timed

	// Snapshot is the state at the end of the events.
	Snapshot Snapshot[S, C]
}

// Violation is an invariant that is not true. It contains a shortest trace
// to a state or transition that breaks the invariant.
type Violation[S comparable, C any] struct {
	// Invariant is the name of the broken invariant.
	Invariant string

	// Trace goes to the state that breaks the invariant. For a step
	// invariant, the last event is the transition that breaks it.
	Trace Trace[S, C]
}

// Report is the result of an exploration.
type Report[S comparable, C any] struct {
	// States is the number of different states that the exploration
	// found.
	States int

	// Violations contains one item for each broken invariant.
	Violations []Violation[S, C]

	// Stuck lists the states that are not done and that no event, timer or
	// service outcome can exit. A stuck state is not always a bug. This
	// changes with the chart. For example, a state that waits for an event
	// that is not in Events is always stuck, because the exploration never
	// sends that event.
	Stuck []Trace[S, C]

	// Truncated is true if the exploration stopped at MaxStates. In that
	// case, the exploration did not check the properties on all reachable
	// states.
	Truncated bool

	// Trapped lists the states that are not done and that cannot get to a
	// top-level final state. No sequence of events, timers and service
	// outcomes goes from them to such a final state. An example is a retry
	// loop with no exit. All Stuck states are also Trapped states.
	//
	// Trapped is nil for a chart with no top-level final state, because
	// such a chart does not end. Trapped is also nil for a truncated
	// exploration, because the exploration cannot know the result.
	Trapped []Trace[S, C]

	reached map[S]bool

	// paths yields the shortest trace to every explored state.
	paths iter.Seq[Trace[S, C]]
}

// Paths yields a shortest trace to each state that the exploration found.
// The nearest state comes first. You can use each trace as a test:
//  1. Start an instance with the Initial context of the trace, at its Start
//     time.
//  2. Give the events of the trace to the instance. Use [Machine.Fold], or
//     use a driver with fake services.
//  3. Make sure that the instance gets to the Snapshot of the trace.
//
// All the traces together go through all the things that the chart can do
// in the limits of the exploration.
func (r Report[S, C]) Paths() iter.Seq[Trace[S, C]] {
	if r.paths == nil {
		return func(func(Trace[S, C]) bool) {}
	}

	return r.paths
}

// PathTo returns a shortest trace to a state where cond is true. It also
// reports if the exploration found such a state.
func (r Report[S, C]) PathTo(cond func(Snapshot[S, C]) bool) (Trace[S, C], bool) {
	for tr := range r.Paths() {
		if cond(tr.Snapshot) {
			return tr, true
		}
	}

	var none Trace[S, C]

	return none, false
}

// FormatTrace describes a trace as numbered steps, one step for each event.
// It names each event in the same way as [Machine.EventName]. For an
// internal event, it shows the related timer or service run. At the end, it
// shows the last states of the trace. Use it in the messages of failed
// tests.
func (m *Machine[S, C]) FormatTrace(tr Trace[S, C]) string {
	var b strings.Builder
	for i, ev := range tr.Events {
		fmt.Fprintf(&b, "%d. +%s %s\n", i+1, ev.At.Sub(tr.Start), m.describeEvent(ev.Event))
	}
	fmt.Fprintf(&b, "ends in %v", tr.Snapshot.Active())

	return b.String()
}

// describeEvent names an event, with the ID of the timer or service run an
// internal event belongs to.
func (m *Machine[S, C]) describeEvent(ev any) string {
	switch e := ev.(type) {
	case nil:
		return "time passes"
	case TimerFired:
		return "timer " + e.ID
	case Failed:
		return fmt.Sprintf("service %s failed: %v", e.ID, e.Err)
	case Emitted:
		return fmt.Sprintf("service %s emitted %s", e.ID, m.EventName(e.Event))
	case invocationResult:
		return fmt.Sprintf("service %s done: %v", e.invocationID(), reflect.ValueOf(ev).FieldByName("Result").Interface())
	default:
		return m.EventName(ev)
	}
}

// Reached reports if the exploration entered state one or more times. This
// includes a state that an eventless transition exited in the same step.
func (r Report[S, C]) Reached(state S) bool { return r.reached[state] }

// explored is one discovered state with the way it was reached.
type explored[S comparable, C any] struct {
	snap   Snapshot[S, C]
	now    time.Time
	key    string
	parent int
	event  Timed

	// initial is the context a start state was created with.
	initial C
}

// edge is a step between two explored states, kept for the reachability
// checks.
type edge struct {
	from  int
	event reflect.Type
}

// Explore goes to all the states that the machine can reach from a new
// instance with context c. It checks the invariants of x in each state. A
// model checker does the same with a specification. Explore runs the guards
// and assignments of the chart. It never runs actions or services. In each
// state, it does these things:
//   - It tries each external event at the time of the state.
//   - It tries each outcome in x for each active service, at the time of
//     the state.
//   - It fires each pending timer at its due time.
//   - It lets each wait in x go by.
//
// The exploration has its own clock. Thus, timers fire in the order of
// their due times, as they do with a driver. A timer with jitter fires at
// its nominal due time.
//
// Two snapshots are the same state if these items are equal:
//   - The active states.
//   - The history.
//   - The services that did not report.
//   - The time that remains on each pending timer and Within budget.
//   - The context.
//
// Thus, the context must marshal to JSON. Also, the context must have a
// finite number of values, or the exploration does not end. A guard must
// set a limit on a counter that only increases. Stored times need a Key.
//
// The search is breadth-first, so each trace in the report is a shortest
// trace. The search continues after violations. It reports each invariant
// one time.
func (m *Machine[S, C]) Explore(c C, x Exploration[S, C]) (Report[S, C], error) {
	report := Report[S, C]{States: 0, Violations: nil, Stuck: nil, Truncated: false, Trapped: nil, reached: map[S]bool{}, paths: nil}

	limit := x.MaxStates
	if limit <= 0 {
		limit = defaultMaxStates
	}

	begin := x.Start
	if begin.IsZero() {
		begin = defaultExploreStart
	}

	var (
		found []explored[S, C]
		seen  = map[string]int{}

		// preds lists, for each found state, the steps that lead to it.
		preds [][]edge
	)

	for _, initial := range append([]C{c}, x.Starts...) {
		start, _, passed := m.start(initial, begin, seedFor(begin, nil))
		for _, n := range passed {
			report.reached[n.id] = true
		}

		startKey, err := m.stateKey(start, begin, x.Key)
		if err != nil {
			return report, err
		}
		if _, ok := seen[startKey]; ok {
			continue
		}
		if len(found) >= limit {
			report.Truncated = true
			continue
		}

		seen[startKey] = len(found)
		found = append(found, explored[S, C]{snap: start, now: begin, key: startKey, parent: -1, event: Timed{At: begin, Event: nil}, initial: initial})
		preds = append(preds, nil)
	}
	roots := len(found)
	broken := map[string]bool{}

	trace := func(i int) Trace[S, C] {
		var events []Timed
		at := i
		for ; found[at].parent >= 0; at = found[at].parent {
			events = append(events, found[at].event)
		}
		slices.Reverse(events)

		return Trace[S, C]{Start: begin, Initial: found[at].initial, Events: events, Snapshot: found[i].snap}
	}

	// violate reports the first violation of each invariant, and builds a
	// trace only for that one.
	violate := func(name string, tr func() Trace[S, C]) {
		if !broken[name] {
			broken[name] = true
			report.Violations = append(report.Violations, Violation[S, C]{Invariant: name, Trace: tr()})
		}
	}

	// through is the trace to found[i] followed by one more step.
	through := func(i int, ev Timed, next Snapshot[S, C]) func() Trace[S, C] {
		return func() Trace[S, C] {
			tr := trace(i)

			return Trace[S, C]{Start: begin, Initial: tr.Initial, Events: append(tr.Events, ev), Snapshot: next}
		}
	}

	check := func(i int) {
		for _, s := range found[i].snap.active {
			report.reached[s] = true
		}

		for _, inv := range x.Invariants {
			if !inv.holds(found[i].snap, found[i].now) {
				violate(inv.Name, func() Trace[S, C] { return trace(i) })
			}
		}
	}

	for i := range roots {
		check(i)
	}

	for i := 0; i < len(found); i++ {
		from := found[i]
		if from.snap.done {
			continue
		}

		moved := false

		for _, cand := range m.candidates(from.snap, from.now, x) {
			ev := cand.Timed
			next, effects, passed := m.transition(from.snap, ev.Event, ev.At)
			for _, n := range passed {
				report.reached[n.id] = true
			}

			key, err := m.stateKey(next, ev.At, x.Key)
			if err != nil {
				return report, err
			}

			if x.CheckDeterminism {
				again, againEffects, _ := m.transition(from.snap, ev.Event, ev.At)
				againKey, err := m.stateKey(again, ev.At, x.Key)
				if err != nil {
					return report, err
				}
				if againKey != key || !sameEffects(effects, againEffects) {
					violate("deterministic", through(i, ev, next))
				}
			}
			moved = moved || next.seq != from.snap.seq

			for _, inv := range x.Steps {
				if !inv.Holds(Step[S, C]{At: ev.At, From: from.snap, Event: ev.Event, To: next, Effects: effects}) {
					violate(inv.Name, through(i, ev, next))
				}
			}

			if j, ok := seen[key]; ok {
				preds[j] = append(preds[j], edge{from: i, event: reflect.TypeOf(ev.Event)})

				if x.CheckKey && !sameSnapshot(found[j].snap, next) {
					same, err := m.sameSuccessors(found[j].snap, found[j].now, next, ev.At, x)
					if err != nil {
						return report, err
					}
					if !same {
						violate("key", through(i, ev, next))
					}
				}

				continue
			}

			if len(found) >= limit {
				report.Truncated = true
				continue
			}

			seen[key] = len(found)
			var notStart C
			found = append(found, explored[S, C]{snap: next, now: ev.At, key: key, parent: i, event: ev, initial: notStart})
			preds = append(preds, []edge{{from: i, event: reflect.TypeOf(ev.Event)}})
			check(len(found) - 1)
		}

		if !moved {
			report.Stuck = append(report.Stuck, trace(i))
		}
	}

	report.States = len(found)
	report.paths = func(yield func(Trace[S, C]) bool) {
		for i := range found {
			if !yield(trace(i)) {
				return
			}
		}
	}

	// canReach marks the states from which some path, using no event of an
	// ignored type, leads to a state where target holds.
	canReach := func(target func(Snapshot[S, C]) bool, ignoring []any) []bool {
		ignored := map[reflect.Type]bool{}
		for _, ev := range ignoring {
			ignored[reflect.TypeOf(ev)] = true
		}

		reaches := make([]bool, len(found))
		var queue []int
		for i, f := range found {
			if target(f.snap) {
				reaches[i] = true
				queue = append(queue, i)
			}
		}

		for len(queue) > 0 {
			j := queue[0]
			queue = queue[1:]
			for _, e := range preds[j] {
				if !reaches[e.from] && !ignored[e.event] {
					reaches[e.from] = true
					queue = append(queue, e.from)
				}
			}
		}

		return reaches
	}

	if !report.Truncated && m.canFinish() {
		finishes := canReach(func(s Snapshot[S, C]) bool { return s.done }, nil)
		for i := range found {
			if !finishes[i] {
				report.Trapped = append(report.Trapped, trace(i))
			}
		}
	}

	if !report.Truncated {
		for _, goal := range x.Goals {
			reaches := canReach(goal.Holds, goal.Ignoring)
			inScope := reachableWithout(preds, roots, goal.Ignoring)

			// found is in breadth-first order, so the first state that
			// cannot reach the goal has a shortest trace.
			for i := range found {
				if inScope[i] && !reaches[i] {
					violate("goal: "+goal.Name, func() Trace[S, C] { return trace(i) })
					break
				}
			}
		}
	}

	return report, nil
}

// canFinish reports whether the chart has a top-level final state.
func (m *Machine[S, C]) canFinish() bool {
	finishes := func(n *node[S, C]) bool {
		return n.kind == kindFinal || slices.ContainsFunc(n.children, func(c *node[S, C]) bool { return c.kind == kindFinal })
	}

	// A chart of regions finishes when every region can.
	if m.root.kind == kindParallel {
		return !slices.ContainsFunc(m.root.states(), func(n *node[S, C]) bool { return !finishes(n) })
	}

	return slices.ContainsFunc(m.root.children, func(n *node[S, C]) bool { return n.kind == kindFinal })
}

// sameEffects reports whether two effect lists ask for the same work. The
// contexts they carry are compared through the state key instead.
func sameEffects[C any](a, b []Effect[C]) bool {
	return slices.EqualFunc(a, b, func(x, y Effect[C]) bool {
		return x.Kind == y.Kind && x.ID == y.ID && x.State == y.State && x.Name == y.Name &&
			x.Delay == y.Delay && x.At.Equal(y.At) && x.Jitter == y.Jitter
	})
}

// candidate is one thing the environment can do in a state.
type candidate struct {
	Timed

	// label names the candidate by what it is rather than by a run ID, so
	// two states that share a key name their candidates alike.
	label string
}

// candidates lists everything the environment can do in snap at now: each
// external event and service outcome now, each pending timer at its due
// time, and each wait.
func (m *Machine[S, C]) candidates(snap Snapshot[S, C], now time.Time, x Exploration[S, C]) []candidate {
	events := make([]candidate, 0, len(x.Events)+len(snap.timers)+len(x.Waits))
	add := func(at time.Time, ev any, label string) {
		events = append(events, candidate{Timed: Timed{At: at, Event: ev}, label: label})
	}

	for i, ev := range x.Events {
		add(now, ev, fmt.Sprintf("event %d", i))
	}

	for _, t := range snap.timers {
		add(later(now, t.due), TimerFired{ID: t.id}, "timer "+m.timerLabel(t))
	}

	for _, inv := range snap.invocations {
		// A held resource has nothing more to report.
		if inv.acquired {
			continue
		}

		service := m.at(inv.state, inv.root).name + "." + inv.name
		if key, spawned := SpawnKey(inv.id); spawned {
			service += fmt.Sprintf("[%q]", key)
		}

		// A service under a Within budget can only report before the
		// deadline, and fails with the deadline at it.
		deadline := m.serviceDeadline(snap, m.at(inv.state, inv.root))
		if deadline.IsZero() || now.Before(deadline) {
			for i, outcome := range x.Outcomes[inv.name] {
				add(now, outcome(inv.id, now), fmt.Sprintf("service %s outcome %d", service, i))
			}
		}
		if !deadline.IsZero() {
			add(later(now, deadline), Failed{ID: inv.id, Err: context.DeadlineExceeded}, "service "+service+" deadline")
		}
	}

	for _, d := range x.Waits {
		add(now.Add(d), nil, "wait "+d.String())
	}

	return events
}

// reachableWithout marks the states reachable from the start without any
// event of an ignored type.
func reachableWithout(preds [][]edge, roots int, ignoring []any) []bool {
	ignored := map[reflect.Type]bool{}
	for _, ev := range ignoring {
		ignored[reflect.TypeOf(ev)] = true
	}

	succs := make([][]int, len(preds))
	for j, in := range preds {
		for _, e := range in {
			if !ignored[e.event] {
				succs[e.from] = append(succs[e.from], j)
			}
		}
	}

	reached := make([]bool, len(preds))
	var queue []int
	for i := range roots {
		reached[i] = true
		queue = append(queue, i)
	}
	for len(queue) > 0 {
		i := queue[0]
		queue = queue[1:]
		for _, j := range succs[i] {
			if !reached[j] {
				reached[j] = true
				queue = append(queue, j)
			}
		}
	}

	return reached
}

// serviceDeadline is the earliest Within deadline around state, or zero.
func (m *Machine[S, C]) serviceDeadline(snap Snapshot[S, C], state *node[S, C]) time.Time {
	deadline := snap.rootScope
	for n := state; n != nil && !n.root; n = n.parent {
		if d, ok := snap.scopes[n.id]; ok && (deadline.IsZero() || d.Before(deadline)) {
			deadline = d
		}
	}

	return deadline
}

// sameSnapshot reports whether two snapshots hold the same state apart from
// their sequence numbers and run IDs.
func sameSnapshot[S comparable, C any](a, b Snapshot[S, C]) bool {
	ja, errA := json.Marshal(a.Context)
	jb, errB := json.Marshal(b.Context)

	return errA == nil && errB == nil && string(ja) == string(jb) &&
		slices.Equal(a.active, b.active) && len(a.timers) == len(b.timers) && len(a.invocations) == len(b.invocations)
}

// sameSuccessors reports whether two states that share a key lead, through
// each candidate, to states that share a key too. Successors are matched by
// candidate, so two states whose candidates lead to each other's successors
// do not pass.
func (m *Machine[S, C]) sameSuccessors(a Snapshot[S, C], aNow time.Time, b Snapshot[S, C], bNow time.Time, x Exploration[S, C]) (bool, error) {
	keys := func(snap Snapshot[S, C], now time.Time) ([]string, error) {
		var out []string
		for _, ev := range m.candidates(snap, now, x) {
			next, _, _ := m.transition(snap, ev.Event, ev.At)
			key, err := m.stateKey(next, ev.At, x.Key)
			if err != nil {
				return nil, err
			}
			out = append(out, ev.label+" -> "+key)
		}
		slices.Sort(out)

		return out, nil
	}

	ka, err := keys(a, aNow)
	if err != nil {
		return false, err
	}
	kb, err := keys(b, bNow)
	if err != nil {
		return false, err
	}

	return slices.Equal(ka, kb), nil
}

func later(a, b time.Time) time.Time {
	if b.After(a) {
		return b
	}

	return a
}

// stateIdentity is what distinguishes one explored state from another. It
// leaves out the event count and the numbers inside timer and service run
// IDs, which grow on every visit to a state without changing what the
// machine can do next.
type stateIdentity struct {
	// Done is set once the machine reached a top-level final state.
	Done bool `json:"done"`

	// Active lists the active states.
	Active []string `json:"active"`

	// History maps each history state to the states it recorded.
	History map[string][]string `json:"history"`

	// Timers lists the delayed transitions whose timers are running, with
	// the time each has left.
	Timers []string `json:"timers"`

	// Services lists the services that have not finished.
	Services []string `json:"services"`

	// Budgets lists the active states with a Within budget, with the time
	// each has left, which decides when the services inside them fail.
	Budgets []string `json:"budgets"`

	// Context is the chart's extended state, or what the exploration's Key
	// projects it onto.
	Context any `json:"context"`
}

// timerLabel names a pending timer by what it is for rather than by its ID.
func (m *Machine[S, C]) timerLabel(t pendingTimer[S]) string {
	state := m.at(t.state, t.root).name

	switch t.kind {
	case timerAfter:
	case timerRetry:
		return fmt.Sprintf("%q retry %q %q attempt=%d", state, t.name, t.key, t.attempt)
	case timerRaise:
		// A raised event that does not marshal is caught when the
		// snapshot is stored; here its text form is enough.
		ev, _ := json.Marshal(t.raised)

		return fmt.Sprintf("%q raise %q %s %s", state, t.name, t.eventName, ev)
	}

	return fmt.Sprintf("%q.%d", state, t.index)
}

func (m *Machine[S, C]) stateKey(snap Snapshot[S, C], now time.Time, key func(C, time.Time) any) (string, error) {
	var ctx any = snap.Context
	if key != nil {
		ctx = key(snap.Context, now)
	}

	id := stateIdentity{
		Done:     snap.done,
		Active:   make([]string, 0, len(snap.active)),
		History:  make(map[string][]string, len(snap.history)),
		Timers:   make([]string, 0, len(snap.timers)),
		Services: make([]string, 0, len(snap.invocations)),
		Budgets:  make([]string, 0, len(snap.scopes)),
		Context:  ctx,
	}

	for _, s := range snap.active {
		id.Active = append(id.Active, m.nodes[s].name)
	}

	for h, recorded := range snap.history {
		names := make([]string, 0, len(recorded))
		for _, s := range recorded {
			names = append(names, m.nodes[s].name)
		}
		id.History[m.nodes[h].name] = names
	}

	for _, t := range snap.timers {
		id.Timers = append(id.Timers, fmt.Sprintf("%s in %s", m.timerLabel(t), t.due.Sub(now)))
	}
	slices.Sort(id.Timers)

	for _, inv := range snap.invocations {
		// Quoting keeps a name that ends like the marker apart from it.
		id.Services = append(id.Services, fmt.Sprintf("%q %q %q held=%t attempt=%d", m.at(inv.state, inv.root).name, inv.name, keyOf(inv.id), inv.acquired, inv.attempt))
	}
	slices.Sort(id.Services)

	// A budget that has run out behaves the same however long ago it did.
	for state, deadline := range snap.scopes {
		id.Budgets = append(id.Budgets, fmt.Sprintf("%s in %s", m.nodes[state].name, max(deadline.Sub(now), 0)))
	}
	if !snap.rootScope.IsZero() {
		id.Budgets = append(id.Budgets, fmt.Sprintf("%s in %s", m.root.name, max(snap.rootScope.Sub(now), 0)))
	}
	slices.Sort(id.Budgets)

	data, err := json.Marshal(id)
	if err != nil {
		return "", fmt.Errorf("marshal state for exploration: %w", err)
	}

	return string(data), nil
}
