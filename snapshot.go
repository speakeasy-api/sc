package sc

import (
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"slices"
	"time"
)

// Snapshot is the complete state of one machine instance. It contains:
//
//   - the active states
//   - the context
//   - the states that the history states remember
//   - the timers and services that are outstanding
//
// A Snapshot is a value. [Machine.Transition] returns a new Snapshot and does
// not change its input.
//
// A Snapshot marshals to JSON. To load a Snapshot again, you need the
// machine. Thus, use [Machine.Restore], not json.Unmarshal.
type Snapshot[S comparable, C any] struct {
	// Context is the extended state of the chart.
	Context C

	machine     string
	version     int
	hash        string
	seq         uint64
	done        bool
	active      []S
	history     map[S][]S
	timers      []pendingTimer[S]
	invocations []runningInvocation[S]
	scopes      map[S]time.Time
	counter     uint64
	seed        uint64

	// tags are the tags of the active states, sorted. They follow from
	// active and are not stored.
	tags []string

	// config and nodes are the active states again, in the forms the core
	// works with: a set indexed by position in document order, and a list
	// in that order. Both are shared between snapshots and never changed.
	config []bool
	nodes  []*node[S, C]

	// rootScope is the deadline of the chart's own budget, or zero.
	rootScope time.Time
}

// pendingTimer is a delayed transition whose timer has not fired.
type pendingTimer[S comparable] struct {
	id     string
	kind   timerKind
	state  S
	index  int
	name   string
	delay  time.Duration
	due    time.Time
	seq    uint64
	jitter float64

	// attempt is, for a retry, how many times the service has failed.
	attempt int

	// root is set for a timer of the chart itself, whose state is then
	// the zero value.
	root bool

	// raised is a delayed raise's event, and eventName its name.
	raised    any
	eventName string

	// key and saved are what a retried run keeps from the one that failed.
	key   string
	saved json.RawMessage
}

// runningInvocation is a service run that has not reported a result.
type runningInvocation[S comparable] struct {
	id    string
	state S
	name  string

	// root is set for a service of the chart itself, whose state is then
	// the zero value.
	root bool

	// resource marks a resource; acquired is set once it is held.
	resource bool
	acquired bool

	// attempt is how many times a retried service failed before this run.
	attempt int

	// saved is the progress the run last recorded with Run.Checkpoint.
	saved json.RawMessage

	// cause is the event the run was started for, and eventName its name.
	cause     any
	eventName string
}

// PendingTimer is a timer that started, and that did not fire and was not
// cancelled.
type PendingTimer[S comparable] struct {
	// ID is the ID of the effect that started the timer. The TimerFired
	// event for the timer contains this ID.
	ID string

	// State is one of these states:
	//
	//   - the state that has the delayed transition of the timer
	//   - the state that has the service that waits for a retry
	//   - the state that has the transition that raised a delayed event
	State S

	// Name is the name of a calculated delay or deadline. For a fixed delay,
	// Name is empty. For a retry, Name is the service name. For a delayed
	// event, Name is the name of the raise.
	Name string

	// Retry is true for the wait before a failed service runs again. Then
	// Attempt is the number of times that the service failed.
	Retry   bool
	Attempt int

	// Raised is the event that a delayed raise will deliver. For other
	// timers, Raised is nil.
	Raised any

	// Delay is the duration of the timer. For a deadline, Delay is zero.
	Delay time.Duration

	// Due is the time when the timer fires. [Actor.Timers] reports a
	// different time: the time for which the actor scheduled the timer.
	// These two times are different for a timer that has jitter.
	Due time.Time

	// Jitter is the fraction by which the driver changes the wait at random.
	Jitter float64
}

// RunningService is a service run that started, and that did not report a
// result and was not stopped.
type RunningService[S comparable] struct {
	// ID is the ID of the run. The Done or Failed event of the run contains
	// this ID.
	ID string

	// State is the state that invoked the service.
	State S

	// Name is the service name.
	Name string
}

// Timers returns the timers that started and did not fire, in the order
// that they started.
func (s Snapshot[S, C]) Timers() []PendingTimer[S] {
	out := make([]PendingTimer[S], 0, len(s.timers))
	for _, t := range s.timers {
		out = append(out, PendingTimer[S]{ID: t.id, State: t.state, Name: t.name, Retry: t.kind == timerRetry, Attempt: t.attempt, Raised: t.raised, Delay: t.delay, Due: t.due, Jitter: t.jitter})
	}

	return out
}

// Services returns the service runs that started and did not finish, in the
// order that they started.
func (s Snapshot[S, C]) Services() []RunningService[S] {
	out := make([]RunningService[S], 0, len(s.invocations))
	for _, inv := range s.invocations {
		if !inv.resource {
			out = append(out, RunningService[S]{ID: inv.id, State: inv.state, Name: inv.name})
		}
	}

	return out
}

// Checkpoint returns the last progress that the service run id recorded with
// [Run.Checkpoint]. It returns nil if the run recorded no progress or if the
// run is not active.
func (s Snapshot[S, C]) Checkpoint(id string) json.RawMessage {
	for _, inv := range s.invocations {
		if inv.id == id {
			return inv.saved
		}
	}

	return nil
}

// HeldResource is a resource of an active state.
type HeldResource[S comparable] struct {
	// ID is the ID of the effect that acquires the resource.
	ID string

	// State is the state that holds the resource.
	State S

	// Name is the resource name.
	Name string

	// Acquired is false while the acquisition is not complete.
	Acquired bool
}

// Resources returns the resources of the active states, in the order that
// their acquisition started.
func (s Snapshot[S, C]) Resources() []HeldResource[S] {
	var out []HeldResource[S]
	for _, inv := range s.invocations {
		if inv.resource {
			out = append(out, HeldResource[S]{ID: inv.id, State: inv.state, Name: inv.name, Acquired: inv.acquired})
		}
	}

	return out
}

// Active returns all active states in document order. These are each active
// leaf and all of the ancestors of that leaf.
func (s Snapshot[S, C]) Active() []S { return slices.Clone(s.active) }

// In reports if state is active, as a leaf or as an ancestor of a leaf.
func (s Snapshot[S, C]) In(state S) bool { return slices.Contains(s.active, state) }

// WithSeed returns the snapshot with the seed that opt gives. The random
// numbers that [Transition.AssignRand] receives come from this seed. Use
// WithSeed to give an instance from [Machine.StartIn] its own random draws:
//
//	snap = snap.WithSeed(sc.RandomSeed())
func (s Snapshot[S, C]) WithSeed(opt StartOption) Snapshot[S, C] {
	s.seed = seedFor(time.Time{}, []StartOption{opt})
	return s
}

// stateName is the stored name of a state a timer or service belongs to:
// the machine's own for the chart itself.
func (s Snapshot[S, C]) stateName(state S, root bool) string {
	if root {
		return s.machine
	}

	return fmt.Sprint(state)
}

// HasTag reports if an active state has tag. You declare tags with
// [StateBuilder.Tag].
func (s Snapshot[S, C]) HasTag(tag string) bool { return slices.Contains(s.tags, tag) }

// Tags returns the tags of the active states. The tags are sorted, and each
// tag occurs one time.
func (s Snapshot[S, C]) Tags() []string { return slices.Clone(s.tags) }

// Done reports if the machine is in a top-level final state.
func (s Snapshot[S, C]) Done() bool { return s.done }

// Seq returns the number of events that the instance processed. When a
// driver saves a snapshot, it uses Seq to find a concurrent update.
func (s Snapshot[S, C]) Seq() uint64 { return s.seq }

// RawSnapshot is the stored form of a [Snapshot]. In a RawSnapshot, each
// state is the text form of its identifier. A [Migrate] hook reads and
// returns a RawSnapshot.
type RawSnapshot struct {
	// Machine is the ID of the machine that made the snapshot.
	Machine string `json:"machine"`

	// Version is the chart version of the snapshot.
	Version int `json:"version"`

	// Hash identifies the chart of the snapshot.
	Hash string `json:"hash"`

	// Seq is the number of events that the instance processed.
	Seq uint64 `json:"seq"`

	// Done is true after the machine gets to a top-level final state.
	Done bool `json:"done"`

	// Active lists the active states.
	Active []string `json:"active"`

	// History maps each history state to the states it recorded.
	History map[string][]string `json:"history,omitempty"`

	// Timers lists the delayed transitions that have active timers.
	Timers []RawTimer `json:"timers,omitempty"`

	// Invocations lists the service runs that did not finish.
	Invocations []RawInvocation `json:"invocations,omitempty"`

	// Scopes maps each active state that declared a budget to the deadline
	// of that state.
	Scopes map[string]time.Time `json:"scopes,omitempty"`

	// Counter is the last number that a timer ID or a service run ID used.
	Counter uint64 `json:"counter"`

	// Seed is the seed of the random numbers of the instance.
	Seed uint64 `json:"seed,omitempty"`

	// Context is the extended state of the chart.
	Context json.RawMessage `json:"context"`
}

// RawTimer is the stored form of an active timer.
type RawTimer struct {
	// ID is the ID of the effect that started the timer.
	ID string `json:"id"`

	// State is the state that has the delayed transition of the timer.
	State string `json:"state"`

	// Index is the position of that transition in the delayed transitions
	// of the state.
	Index int `json:"index"`

	// DelayNanos is the duration of the timer, in nanoseconds.
	DelayNanos int64 `json:"delay_ns,omitempty"`

	// Due is the time when the timer fires. Old snapshots, from before sc
	// recorded due times, do not have Due. When you restore such a snapshot,
	// the timer starts again with its full delay.
	Due time.Time `json:"due,omitzero"`

	// Seq puts timers that are due at the same time in order. The timer with
	// the lower Seq started first.
	Seq uint64 `json:"seq,omitempty"`

	// Kind shows the type of timer:
	//
	//   - empty for a delayed transition
	//   - "retry" for the wait before a failed service runs again
	//   - "raise" for a delayed event
	Kind string `json:"kind,omitempty"`

	// Name is the name of the service that runs again, or the name of the
	// raise.
	Name string `json:"name,omitempty"`

	// Attempt is the number of times that a service with retries failed.
	Attempt int `json:"attempt,omitempty"`

	// Jitter is the fraction by which the driver changes the wait of a retry
	// at random.
	Jitter float64 `json:"jitter,omitempty"`

	// EventName and Event are the event of a delayed raise.
	EventName string          `json:"event_name,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`

	// For a retry, Key is the spawn key of the run that failed, and Saved is
	// the progress that the run recorded.
	Key   string          `json:"key,omitempty"`
	Saved json.RawMessage `json:"saved,omitempty"`
}

// RawInvocation is the stored form of a service run that did not finish.
type RawInvocation struct {
	// ID is the ID of the effect that started the run.
	ID string `json:"id"`

	// State is the state that has the service.
	State string `json:"state"`

	// Name is the service name.
	Name string `json:"name"`

	// Resource is true for a resource that you declare with Holds.
	Resource bool `json:"resource,omitempty"`

	// Acquired is true for a resource that the driver held.
	Acquired bool `json:"acquired,omitempty"`

	// Attempt is the number of times that a service with retries failed
	// before this run.
	Attempt int `json:"attempt,omitempty"`

	// Saved is the last progress that the run recorded with Run.Checkpoint.
	Saved json.RawMessage `json:"saved,omitempty"`

	// EventName and Event are the event that caused the run to start.
	EventName string          `json:"event_name,omitempty"`
	Event     json.RawMessage `json:"event,omitempty"`
}

// MarshalJSON encodes the snapshot in the [RawSnapshot] format.
func (s Snapshot[S, C]) MarshalJSON() ([]byte, error) {
	ctx, err := json.Marshal(s.Context)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot context: %w", err)
	}

	raw := RawSnapshot{
		Machine:     s.machine,
		Version:     s.version,
		Hash:        s.hash,
		Seq:         s.seq,
		Done:        s.done,
		Active:      make([]string, 0, len(s.active)),
		History:     make(map[string][]string, len(s.history)),
		Timers:      make([]RawTimer, 0, len(s.timers)),
		Invocations: make([]RawInvocation, 0, len(s.invocations)),
		Scopes:      make(map[string]time.Time, len(s.scopes)),
		Counter:     s.counter,
		Seed:        s.seed,
		Context:     ctx,
	}

	for _, st := range s.active {
		raw.Active = append(raw.Active, fmt.Sprint(st))
	}

	for h, recorded := range s.history {
		names := make([]string, 0, len(recorded))
		for _, st := range recorded {
			names = append(names, fmt.Sprint(st))
		}
		raw.History[fmt.Sprint(h)] = names
	}

	for _, t := range s.timers {
		rt := RawTimer{ID: t.id, State: s.stateName(t.state, t.root), Index: t.index, DelayNanos: int64(t.delay), Due: t.due, Seq: t.seq, Kind: string(t.kind), Name: "", Attempt: t.attempt, Jitter: 0, EventName: "", Event: nil, Key: t.key, Saved: t.saved}

		switch t.kind {
		case timerAfter:
		case timerRetry:
			rt.Name, rt.Jitter = t.name, t.jitter

			// A retry keeps the event its service was started for.
			if t.raised != nil {
				ev, err := json.Marshal(t.raised)
				if err != nil {
					return nil, fmt.Errorf("marshal the event of retry %q: %w", t.name, err)
				}
				rt.EventName, rt.Event = t.eventName, ev
			}
		case timerRaise:
			ev, err := json.Marshal(t.raised)
			if err != nil {
				return nil, fmt.Errorf("marshal raised event %q: %w", t.eventName, err)
			}
			rt.Name, rt.EventName, rt.Event = t.name, t.eventName, ev
		}

		raw.Timers = append(raw.Timers, rt)
	}

	for _, inv := range s.invocations {
		ri := RawInvocation{ID: inv.id, State: s.stateName(inv.state, inv.root), Name: inv.name, Resource: inv.resource, Acquired: inv.acquired, Attempt: inv.attempt, Saved: inv.saved, EventName: "", Event: nil}
		if inv.cause != nil {
			ev, err := json.Marshal(inv.cause)
			if err != nil {
				return nil, fmt.Errorf("marshal the event of service %q: %w", inv.name, err)
			}
			ri.EventName, ri.Event = inv.eventName, ev
		}

		raw.Invocations = append(raw.Invocations, ri)
	}

	for state, deadline := range s.scopes {
		raw.Scopes[fmt.Sprint(state)] = deadline
	}
	if !s.rootScope.IsZero() {
		raw.Scopes[s.machine] = s.rootScope
	}

	data, err := json.Marshal(raw)
	if err != nil {
		return nil, fmt.Errorf("marshal snapshot: %w", err)
	}

	return data, nil
}

var (
	// ErrVersionMismatch is the error that [Machine.Restore] returns for a
	// snapshot from a different chart version, if the machine has no
	// [Migrate] hook.
	ErrVersionMismatch = errors.New("snapshot was taken at another chart version and no migration is configured")

	// ErrChartChanged is the error that [Machine.Restore] returns for a
	// snapshot that has the same version as the machine but a different
	// chart. This shows that someone changed the chart but did not increase
	// its [Version].
	ErrChartChanged = errors.New("chart changed without a version change")

	// ErrInvalidSnapshot is the error that [Machine.Restore] returns for a
	// snapshot that does not show a legal state of the chart.
	ErrInvalidSnapshot = errors.New("invalid snapshot")
)

// Restore loads a snapshot from the data that you get when you marshal a
// [Snapshot].
//
// If the snapshot has a different [Version], Restore first sends it through
// the [Migrate] hook. Then Restore makes sure that the result is a legal
// state of the chart.
//
// The effects that Restore returns make the outstanding timers and services
// agree with the active states:
//
//   - They cancel the timers and services of states that are not active now.
//   - They start the timers and services of states that became active.
//
// For a snapshot that this chart stored with no changes, there are no
// effects. There are effects if a migration changes the active states. There
// are also effects if the timers or services of a stored snapshot are not
// part of its active states.
//
// now is the start time for a timer that a migration starts. It is also the
// start time for a timer that has no recorded due time. Restore does not fire
// timers. To bring the snapshot up to date, give it to [Machine.Advance].
func (m *Machine[S, C]) Restore(data []byte, now time.Time) (Snapshot[S, C], []Effect[C], error) {
	var zero Snapshot[S, C]

	var raw RawSnapshot
	if err := json.Unmarshal(data, &raw); err != nil {
		return zero, nil, fmt.Errorf("%w: unmarshal snapshot: %w", ErrInvalidSnapshot, err)
	}

	if raw.Machine != m.id {
		return zero, nil, fmt.Errorf("%w: taken by machine %q, not %q", ErrInvalidSnapshot, raw.Machine, m.id)
	}

	// previously holds the states that were active before a migration. It
	// stays nil when the snapshot is not migrated.
	var previously map[string]bool

	switch {
	case raw.Version != m.version && m.migrate == nil:
		return zero, nil, fmt.Errorf("%w: snapshot version %d, chart version %d", ErrVersionMismatch, raw.Version, m.version)
	case raw.Version != m.version:
		// A stored snapshot may name only leaf states; restoring it
		// activates their ancestors too, so those count as active before.
		previously = map[string]bool{}
		for _, name := range raw.Active {
			previously[name] = true
			for n := m.names[name]; n != nil; n = n.parent {
				previously[n.name] = true
			}
		}

		migrated, err := m.migrate(raw)
		if err != nil {
			return zero, nil, fmt.Errorf("migrate snapshot from version %d: %w", raw.Version, err)
		}
		raw = migrated
	case raw.Hash != m.hash:
		return zero, nil, fmt.Errorf("%w: version %d", ErrChartChanged, m.version)
	}

	r := &run[S, C]{
		m:          m,
		config:     m.rootOnly(),
		history:    nil,
		timers:     nil,
		services:   nil,
		counter:    raw.Counter,
		ctx:        zero.Context,
		done:       raw.Done,
		now:        now,
		fired:      nil,
		seed:       raw.Seed,
		stream:     raw.Seq + 1,
		rng:        nil,
		scopes:     nil,
		acquired:   nil,
		pending:    nil,
		effects:    nil,
		queue:      nil,
		entered:    nil,
		passed:     nil,
		changed:    false,
		cause:      nil,
		sends:      nil,
		starting:   false,
		ended:      nil,
		entryCause: nil,
		list:       nil,
		listed:     false,
	}

	if err := json.Unmarshal(raw.Context, &r.ctx); err != nil {
		return zero, nil, fmt.Errorf("%w: unmarshal snapshot context: %w", ErrInvalidSnapshot, err)
	}

	if err := r.restoreStates(raw); err != nil {
		return zero, nil, err
	}

	for name, deadline := range raw.Scopes {
		if n, ok := m.owner(name); ok && r.config[n.order] && n.budget > 0 {
			r.setScope(n, deadline)
		}
	}

	if err := r.restoreOutstanding(raw, previously); err != nil {
		return zero, nil, err
	}

	return r.snapshot(raw.Seq), r.effects, nil
}

// restoreStates loads and checks the active and remembered states.
func (r *run[S, C]) restoreStates(raw RawSnapshot) error {
	for _, name := range raw.Active {
		n, ok := r.m.names[name]
		if !ok || n.kind == kindHistory {
			return fmt.Errorf("%w: %q is not a state of the chart", ErrInvalidSnapshot, name)
		}

		for a := n; a != nil; a = a.parent {
			r.activate(a)
		}
	}

	if err := r.checkConfig(); err != nil {
		return err
	}

	for name, recorded := range raw.History {
		h, ok := r.m.names[name]
		if !ok || h.kind != kindHistory {
			return fmt.Errorf("%w: %q is not a history state of the chart", ErrInvalidSnapshot, name)
		}

		nodes := make([]*node[S, C], 0, len(recorded))
		for _, rn := range recorded {
			n, ok := r.m.names[rn]
			if !ok || n.kind == kindHistory || !n.below(h.parent) {
				return fmt.Errorf("%w: history state %q remembers %q, which is not a state inside %q", ErrInvalidSnapshot, name, rn, h.parent.name)
			}
			nodes = append(nodes, n)
		}

		if len(nodes) > 0 {
			r.setHistory(h, nodes)
		}
	}

	return nil
}

// checkConfig reports whether the active states are a legal configuration:
// one active child under each active compound state, and every region active
// under each active parallel state.
func (r *run[S, C]) checkConfig() error {
	for _, n := range append([]*node[S, C]{r.m.root}, r.active()...) {
		activeChildren := 0
		for _, c := range n.states() {
			if r.config[c.order] {
				activeChildren++
			}
		}

		switch {
		case n.kind == kindParallel && activeChildren != len(n.states()):
			return fmt.Errorf("%w: parallel state %q does not have every region active", ErrInvalidSnapshot, n.name)
		case n.root && n.initial == nil && activeChildren == 0:
			// A chart of final states starts in none of them.
		case n.kind == kindState && !n.atomic() && activeChildren != 1:
			return fmt.Errorf("%w: state %q has %d active child states, want 1", ErrInvalidSnapshot, n.name, activeChildren)
		}
	}

	return nil
}

// StartIn returns a new instance with context c that is already in the given
// states at now. The instance is as if it went to those states. These rules
// apply:
//
//   - states names one state for each active region.
//   - The ancestors of those states also become active.
//   - A state that has children is the same as the states that the chart
//     goes to when it enters that state.
//   - A region that states does not name gets its default entry.
//
// No entry action runs. But the timers and services of each active state
// start. Also, if the guard of an Always transition holds, StartIn takes the
// transition, as on arrival. Thus the effects that StartIn returns are the
// effects that a driver must do to keep the instance active.
//
// Use StartIn for tests that need an instance in a specified state. Also use
// it to adopt work that already started, for example an existing row whose
// status names its state. If the states are not a legal configuration of the
// chart, StartIn returns an error that wraps [ErrInvalidSnapshot].
//
// The random seed of the instance comes from now. Thus instances that start
// at the same instant make the same [Transition.AssignRand] draws. If they
// must be different, give each instance its own seed with
// [Snapshot.WithSeed].
func (m *Machine[S, C]) StartIn(c C, now time.Time, states ...S) (Snapshot[S, C], []Effect[C], error) {
	var zero Snapshot[S, C]

	r := &run[S, C]{
		m:          m,
		config:     m.rootOnly(),
		history:    nil,
		timers:     nil,
		services:   nil,
		counter:    0,
		ctx:        c,
		done:       false,
		now:        now,
		fired:      nil,
		seed:       seedFor(now, nil),
		stream:     0,
		rng:        nil,
		scopes:     nil,
		acquired:   nil,
		pending:    nil,
		effects:    nil,
		queue:      nil,
		entered:    nil,
		passed:     nil,
		changed:    false,
		cause:      nil,
		sends:      nil,
		starting:   false,
		ended:      nil,
		entryCause: nil,
		list:       nil,
		listed:     false,
	}

	// A state that has children stands for the states entering it leads
	// to, and regions nobody names get their default entry.
	set := &nodeSet[S, C]{in: make([]bool, len(m.ordered)+1), list: nil}

	named := make([]*node[S, C], 0, len(states))
	for _, s := range states {
		n, ok := m.nodes[s]
		if !ok || n.kind == kindHistory {
			return zero, nil, fmt.Errorf("%w: %q is not a state of the chart", ErrInvalidSnapshot, fmt.Sprint(s))
		}
		named = append(named, n)
	}

	for _, n := range named {
		r.addDescendants(set, n)
	}
	for _, n := range named {
		r.addAncestors(set, n, m.root)
	}
	r.fillRegions(set, m.root)

	for _, n := range set.list {
		if set.in[n.order] {
			r.activate(n)
		}
	}

	if err := r.checkConfig(); err != nil {
		return zero, nil, err
	}

	r.done = r.completed(m.root)

	if !r.done {
		everything := append([]*node[S, C]{m.root}, r.active()...)

		for _, n := range everything {
			if n.budget > 0 {
				r.setScope(n, now.Add(n.budget))
			}
		}

		for _, n := range everything {
			for _, t := range n.transitions {
				if t.trigger == triggerAfter {
					r.startTimer(t)
				}
			}
			r.entered = append(r.entered, n)
		}
		r.startServices()
		r.settle()
		r.fireDue()
	}

	return r.snapshot(0), r.effects, nil
}

// restoreOutstanding loads the timers and service runs that still belong to
// an active state and cancels the rest. When the snapshot was migrated,
// previously is non-nil and names the states active before the migration;
// states the migration made active get the timers, services and budget that
// entering them would have started.
func (r *run[S, C]) restoreOutstanding(raw RawSnapshot, previously map[string]bool) error {
	for _, rt := range raw.Timers {
		n, ok := r.m.owner(rt.State)
		kept := false

		switch {
		case timerKind(rt.Kind) == timerRaise && ok && !r.done:
			// A raised event waits whatever state the chart is in.
			t, known := r.m.eventTypes[rt.EventName]
			if !known {
				return fmt.Errorf("%w: delayed event %q is not one the chart handles", ErrInvalidSnapshot, rt.EventName)
			}

			ev := reflect.New(t)
			if err := json.Unmarshal(rt.Event, ev.Interface()); err != nil {
				return fmt.Errorf("unmarshal delayed event %q: %w", rt.EventName, err)
			}

			r.timers = append(r.timers, timer[S, C]{id: rt.ID, kind: timerRaise, t: nil, retry: nil, attempt: 0, source: n, label: rt.Name, raised: ev.Elem().Interface(), delay: time.Duration(rt.DelayNanos), due: rt.Due, seq: rt.Seq, jitter: 0, key: "", saved: nil})
			kept = true
		case timerKind(rt.Kind) == timerRetry && ok && r.config[n.order] && !r.done:
			for _, inv := range n.invocations {
				if inv.name == rt.Name && inv.retry != nil {
					cause, err := r.m.decodeEvent(rt.EventName, rt.Event)
					if err != nil {
						return err
					}

					r.timers = append(r.timers, timer[S, C]{id: rt.ID, kind: timerRetry, t: nil, retry: inv, attempt: rt.Attempt, source: nil, label: "", raised: cause, delay: time.Duration(rt.DelayNanos), due: rt.Due, seq: rt.Seq, jitter: rt.Jitter, key: rt.Key, saved: rt.Saved})
					kept = true
				}
			}
		case timerKind(rt.Kind) == timerAfter && ok && r.config[n.order] && !r.done:
			for _, t := range n.transitions {
				if t.trigger == triggerAfter && t.afterIndex == rt.Index {
					r.timers = append(r.timers, restoredTimer(rt.ID, t, time.Duration(rt.DelayNanos), rt.Due, rt.Seq, r.now))
					kept = true
				}
			}
		}

		if !kept {
			var zero C
			r.effects = append(r.effects, Effect[C]{Kind: EffectCancelTimer, ID: rt.ID, State: rt.State, Name: "", Delay: 0, At: time.Time{}, Jitter: 0, Context: zero, Event: nil, From: nil, To: nil, Needs: nil, Saved: nil})
		}
	}

	for _, ri := range raw.Invocations {
		n, ok := r.m.owner(ri.State)
		kept := false

		if ok && r.config[n.order] && !r.done {
			for _, inv := range n.invocations {
				if inv.name == ri.Name && (inv.resource != nil) == ri.Resource {
					cause, err := r.m.decodeEvent(ri.EventName, ri.Event)
					if err != nil {
						return err
					}

					r.services = append(r.services, service[S, C]{id: ri.ID, inv: inv, acquired: ri.Acquired, attempt: ri.Attempt, saved: ri.Saved, cause: cause})
					kept = true
				}
			}
		}

		// A resource that is dropped needs no release: whatever process
		// held its value is gone.
		if !kept && !ri.Resource {
			var zero C
			r.effects = append(r.effects, Effect[C]{Kind: EffectStopInvocation, ID: ri.ID, State: ri.State, Name: ri.Name, Delay: 0, At: time.Time{}, Jitter: 0, Context: zero, Event: nil, From: nil, To: nil, Needs: nil, Saved: nil})
		}
	}

	if r.done || previously == nil {
		return nil
	}

	for _, n := range r.active() {
		if previously[n.name] {
			continue
		}

		if _, ok := r.scopes[n]; !ok && n.budget > 0 {
			r.setScope(n, r.now.Add(n.budget))
		}

		for _, t := range n.transitions {
			if t.trigger == triggerAfter {
				r.startTimer(t)
			}
		}

		for _, inv := range n.invocations {
			if !inv.spawned {
				r.startService(inv)
			}
		}
	}

	return nil
}

// Resume returns the effects that start again all items that are
// outstanding in snap:
//
//   - one [EffectStartTimer] for each pending timer. It contains the due time
//     of the timer, which can be in the past.
//   - one [EffectAcquire] for each resource of an active state.
//   - one [EffectStartInvocation] for each service run that did not finish.
//
// Drivers that keep timers and services in memory call Resume after they
// load a snapshot in a new process. Drivers that keep timers and services in
// durable storage do not need Resume for them. But a resource value never
// continues after its process stops. Thus each driver that holds resources
// must acquire them again through Resume.
func (m *Machine[S, C]) Resume(snap Snapshot[S, C]) []Effect[C] {
	r := m.load(snap, time.Time{})

	effects := make([]Effect[C], 0, len(r.timers)+len(r.services))

	for _, t := range r.timers {
		effects = append(effects, timerEffect(t))
	}

	// Resources are acquired again before the services that run inside
	// them.
	for _, resources := range []bool{true, false} {
		for _, s := range r.services {
			if (s.inv.resource != nil) == resources {
				effects = append(effects, r.serviceEffect(s.id, s.inv))
			}
		}
	}

	return effects
}

// decodeEvent loads a stored event by the name the chart gave its type. An
// empty name is no event.
func (m *Machine[S, C]) decodeEvent(name string, data json.RawMessage) (any, error) {
	if name == "" {
		return nil, nil //nolint:nilnil // no event is a valid result, not an error.
	}

	t, known := m.eventTypes[name]
	if !known {
		return nil, fmt.Errorf("%w: stored event %q is not one the chart handles", ErrInvalidSnapshot, name)
	}

	ev := reflect.New(t)
	if err := json.Unmarshal(data, ev.Interface()); err != nil {
		return nil, fmt.Errorf("%w: unmarshal stored event %q: %w", ErrInvalidSnapshot, name, err)
	}

	return ev.Elem().Interface(), nil
}
