package sc

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"sync"
	"time"

	"golang.org/x/sync/semaphore"
)

// ErrInstanceClosed is the error that [Instance.Send] and [Instance.Advance]
// return after a call to [Instance.Close].
var ErrInstanceClosed = errors.New("instance is closed")

// ErrInstanceDone is the error that [Instance.Await] returns when the machine
// is done and the condition is not true. After the machine is done, nothing
// changes again.
var ErrInstanceDone = errors.New("instance is done")

// Instance drives one machine instance from the goroutines of its callers. An
// Instance has no goroutine and no mailbox. The Instance processes an event
// on the goroutine that sends the event. That goroutine is a caller of
// [Instance.Send], or a service run that sends its result. The Instance
// processes one event at a time, together with the actions of the event. As a
// result, when Send returns, the caller sees all the results of its event.
//
// Use an Instance for a state machine in a loop or request handler that
// exists. For example:
//
//   - A poll loop that must know that a step is complete before it continues.
//   - A request that must know if the instance accepted its command.
//   - A hot path that cannot use one goroutine for each instance.
//
// Actions run on the goroutine that processes the event. An action must not
// send events to the same instance. Use [Transition.Raise] instead. Each
// service runs on its own goroutine and sends its results through the
// instance.
type Instance[S comparable, C any] struct {
	m        *Machine[S, C]
	hooks    *Hooks[S, C]
	services *semaphore.Weighted
	now      func() time.Time
	manual   bool
	emit     func(context.Context, any)
	throttle Waiter

	// base carries the values of the context the instance was started
	// with, for the steps a timer starts, which have no sender.
	base context.Context //nolint:containedctx // only its values are used, as the context of timer wake-ups.

	// turn holds one token, the right to process an event. A channel
	// rather than a mutex lets a stopped service give up waiting for it.
	turn chan struct{}

	// res holds the values of the resources the instance has acquired.
	res *resourceTable[S, C]

	// runs tracks service goroutines so Close can wait for them.
	runs sync.WaitGroup

	mu sync.Mutex

	// snap is the current snapshot, guarded by mu.
	snap Snapshot[S, C]

	// changed is closed and replaced whenever snap is replaced, guarded
	// by mu.
	changed chan struct{}

	// running maps each running service run's ID to how to stop it,
	// guarded by mu.
	running map[string]serviceRun

	// wakeups maps each scheduled timer's ID to its wake-up, guarded by mu.
	wakeups map[string]*time.Timer

	// unsettled counts, per settle token, the service runs still going,
	// guarded by mu. A token's entry is removed when its count reaches
	// zero.
	unsettled map[uint64]int

	// lastToken is the last settle token handed out, guarded by mu.
	lastToken uint64

	// ended is closed and replaced whenever a service run ends, guarded
	// by mu.
	ended chan struct{}

	// closed is set by Close, guarded by mu.
	closed bool
}

// deliveringRun marks the context of an event a service run delivered,
// with the run's ID, so that stopping that same run does not wait for the
// goroutine doing the stopping.
type deliveringRun struct{}

// serviceRun is a service goroutine an Instance started.
type serviceRun struct {
	// stop cancels the run and drops what it reports from then on.
	stop context.CancelFunc

	// cancel cancels the service's work but still delivers its result.
	cancel context.CancelFunc

	// token is the settle token of the send that started the run, zero
	// for none.
	token uint64

	// done is closed when the goroutine returns.
	done chan struct{}

	// inbox receives the events the chart sends to the run.
	inbox *inbox
}

// settleToken marks, through the context values services inherit, the runs
// that a SendAndSettle call is waiting for.
type settleToken struct{}

// Instance starts a new instance with context c at the time of the clock.
// Instance does the effects of the entry into the initial states. The
// actions of these effects use ctx. The services of the instance keep the
// values of ctx, but not its cancellation. A service runs until one of
// these occurs:
//
//   - The chart exits the state of the service.
//   - A caller cancels the service.
//   - A caller closes the instance.
//
// If an initial action fails, Instance closes the instance and returns only
// the error.
func (m *Machine[S, C]) Instance(ctx context.Context, c C, opts ...RunOption) (*Instance[S, C], error) {
	o := runSettings(opts)
	snap, effects := m.Start(c, o.now(), RandomSeed())

	return m.newInstance(ctx, snap, effects, o)
}

// InstanceFrom continues an instance from snap, for example after a restart.
// InstanceFrom sets each pending timer for the time when it is due. The
// services that did not end run again.
func (m *Machine[S, C]) InstanceFrom(ctx context.Context, snap Snapshot[S, C], opts ...RunOption) (*Instance[S, C], error) {
	return m.newInstance(ctx, snap, m.Resume(snap), runSettings(opts))
}

func (m *Machine[S, C]) newInstance(ctx context.Context, snap Snapshot[S, C], effects []Effect[C], o runOptions) (*Instance[S, C], error) {
	if err := m.checkListeners(o); err != nil {
		return nil, err
	}

	i := &Instance[S, C]{
		m:         m,
		hooks:     hooksFor[S, C](o, m.id),
		services:  o.services,
		now:       o.now,
		manual:    o.manualTimers,
		emit:      o.listener(),
		throttle:  o.throttle,
		base:      context.WithoutCancel(ctx),
		turn:      make(chan struct{}, 1),
		res:       nil,
		runs:      sync.WaitGroup{},
		mu:        sync.Mutex{},
		snap:      snap,
		changed:   make(chan struct{}),
		running:   map[string]serviceRun{},
		wakeups:   map[string]*time.Timer{},
		unsettled: map[uint64]int{},
		lastToken: 0,
		ended:     make(chan struct{}),
		closed:    false,
	}
	i.res = newResourceTable(m, i.hooks)
	i.turn <- struct{}{}

	<-i.turn
	defer func() { i.turn <- struct{}{} }()

	if err := i.apply(ctx, effects); err != nil {
		i.turn <- struct{}{}
		i.Close()
		<-i.turn

		return nil, err
	}

	return i, nil
}

// Snapshot returns the current snapshot of the instance. This is the state
// after the last step for which the instance did all the effects. If an
// action reads the snapshot while its own step runs, the action sees the
// state before that step.
func (i *Instance[S, C]) Snapshot() Snapshot[S, C] {
	i.mu.Lock()
	defer i.mu.Unlock()

	return i.snap
}

// Send processes ev at the time of the clock. Before Send returns, it does
// the actions of the event with ctx. If the instance processes a different
// event, Send waits. The wait stops when ctx ends. The step that Send returns
// tells if the instance accepted the event. The event changed the instance
// if step.To.Seq() is not equal to step.From.Seq().
//
// Each service that the event starts runs on its own goroutine. The service
// keeps the values of ctx, but not its cancellation. If the event stops a
// service, the service stops and its goroutine returns before the next
// effect. The error is the error of the first action that failed. Even
// then, the instance did the transition.
func (i *Instance[S, C]) Send(ctx context.Context, ev any) (Step[S, C], error) {
	if err := i.m.CheckEvent(ev); err != nil {
		snap := i.Snapshot()

		return Step[S, C]{At: time.Time{}, From: snap, Event: ev, To: snap, Effects: nil}, fmt.Errorf("send to instance: %w", err)
	}

	select {
	case <-i.turn:
	case <-ctx.Done():
		// One read serves both ends of the step, so a step another send
		// publishes meanwhile cannot make this event look accepted.
		snap := i.Snapshot()

		return Step[S, C]{At: time.Time{}, From: snap, Event: ev, To: snap, Effects: nil}, fmt.Errorf("send to instance: %w", context.Cause(ctx))
	}
	defer func() { i.turn <- struct{}{} }()

	return i.process(ctx, ev)
}

// SendAndSettle sends ev as [Instance.Send] does. Then it waits until each
// service run that the step started is complete and the instance processed
// its result. This includes the runs that these results start, and so on.
// Use SendAndSettle for a caller that must not continue while its work
// continues, for example one pass of a poll loop. The wait stops when ctx
// ends. If a service never ends, for example a stream, the wait continues
// until ctx ends.
//
// An error from an action does not stop the wait. The instance did the
// transition and started its services. As a result, SendAndSettle first waits
// for these services. Then it returns the step and the error.
func (i *Instance[S, C]) SendAndSettle(ctx context.Context, ev any) (Step[S, C], error) {
	i.mu.Lock()
	i.lastToken++
	token := i.lastToken
	i.mu.Unlock()

	step, sendErr := i.Send(context.WithValue(ctx, settleToken{}, token), ev)

	for {
		i.mu.Lock()
		pending, ended := i.unsettled[token], i.ended
		i.mu.Unlock()

		if pending == 0 {
			return step, sendErr
		}

		select {
		case <-ended:
		case <-ctx.Done():
			return step, errors.Join(sendErr, fmt.Errorf("settle instance: %w", context.Cause(ctx)))
		}
	}
}

// Cancel cancels the context of the service run id, and tells if that run was
// active. When the chart exits the state of a run, the run stops and the
// instance discards its results. Cancel is different. The result of a
// cancelled run, usually the error of its context, goes to the chart as its
// Done or Failed event. As a result, the chart decides what occurs next. Use
// Cancel to interrupt work when you still want its result, for example when
// you drain the instance.
func (i *Instance[S, C]) Cancel(id string) bool {
	i.mu.Lock()
	defer i.mu.Unlock()

	run, ok := i.running[id]
	if ok {
		run.cancel()
	}

	return ok
}

// CancelAll cancels all active service runs, as Cancel does for one run.
func (i *Instance[S, C]) CancelAll() {
	i.mu.Lock()
	defer i.mu.Unlock()

	for _, run := range i.running {
		run.cancel()
	}
}

// Advance fires the timers that are due at the time of the clock, as Send
// does before an event. Then Advance does the effects of these timers. Use
// Advance to move the instance forward when you control the time, with
// [ManualTimers] or with your own clock.
func (i *Instance[S, C]) Advance(ctx context.Context) (Step[S, C], error) {
	step, err := i.Send(ctx, nil)

	// Child machines run on the same clock, and under manual timers they
	// are moved on with their parent.
	if i.manual {
		i.mu.Lock()
		for _, run := range i.running {
			run.inbox.put(advance{})
		}
		i.mu.Unlock()
	}

	return step, err
}

// Await blocks until cond is true for the current snapshot, and returns that
// snapshot. Await first examines the current snapshot. Then it examines the
// snapshot after each change. The wait stops when ctx ends. If the instance
// is done and cond is not true, Await returns [ErrInstanceDone]. If the
// instance is closed and cond is not true, Await returns [ErrInstanceClosed].
func (i *Instance[S, C]) Await(ctx context.Context, cond func(Snapshot[S, C]) bool) (Snapshot[S, C], error) {
	for {
		i.mu.Lock()
		snap, changed := i.snap, i.changed
		i.mu.Unlock()

		if cond(snap) {
			return snap, nil
		}

		// A finished or closed instance never changes again.
		i.mu.Lock()
		closed := i.closed
		i.mu.Unlock()

		switch {
		case snap.done:
			return snap, ErrInstanceDone
		case closed:
			return snap, ErrInstanceClosed
		}

		select {
		case <-changed:
		case <-ctx.Done():
			return snap, fmt.Errorf("await instance: %w", context.Cause(ctx))
		}
	}
}

// Close does these steps:
//
//  1. It stops all active services and timers.
//  2. It waits until the goroutines of the services return.
//  3. It releases all the resources that the instance holds. It releases
//     the last resource that it acquired first.
//
// After Close, Send and Advance return [ErrInstanceClosed]. The caller can
// read the snapshot, and can persist it. The snapshot continues to record the
// resources as held. As a result, an instance that starts again from the
// snapshot acquires them again.
func (i *Instance[S, C]) Close() {
	i.mu.Lock()
	i.closed = true
	for _, w := range i.wakeups {
		w.Stop()
	}
	i.res.abandon()
	for _, run := range i.running {
		run.stop()
	}
	i.mu.Unlock()

	i.runs.Wait()
	i.res.releaseAll(context.Background())
}

// process takes one step. The caller holds the turn.
func (i *Instance[S, C]) process(ctx context.Context, ev any) (Step[S, C], error) {
	from := i.Snapshot()

	i.mu.Lock()
	closed := i.closed
	i.mu.Unlock()
	if closed {
		return Step[S, C]{At: time.Time{}, From: from, Event: ev, To: from, Effects: nil}, ErrInstanceClosed
	}

	at := i.now()
	next, effects, _ := i.m.transition(from, ev, at)
	step := Step[S, C]{At: at, From: from, Event: ev, To: next, Effects: effects}

	// The new snapshot is published once its effects have been carried
	// out, so a reader never sees a state whose actions are still to run.
	// It is published even when an action failed, and even when a hook
	// panics: the transition was taken, and a snapshot left behind would
	// have the driver stop services the chart still counts as running.
	defer func() {
		i.mu.Lock()
		defer i.mu.Unlock()

		if next.seq != from.seq {
			i.snap = next
			close(i.changed)
			i.changed = make(chan struct{})
		}
	}()

	// A hook that panics is the caller's to hear about, but only once the
	// step's effects have been carried out: the chart has taken the step,
	// and counts the work those effects start.
	var failed any
	func() {
		defer func() { failed = recover() }()

		i.hooks.Observe(ctx, step)
	}()

	err := i.apply(ctx, effects)
	if failed != nil {
		panic(failed)
	}

	return step, err
}

// apply carries out effects in order. The caller holds the turn.
func (i *Instance[S, C]) apply(ctx context.Context, effects []Effect[C]) error {
	var first error

	ctx = i.res.attach(ctx)

	for _, e := range effects {
		switch e.Kind {
		case EffectAcquire:
			i.res.begin(e)
			i.startService(ctx, e)
		case EffectRelease:
			i.res.releaseFor(ctx, e)
		case EffectAction:
			if err := i.hooks.Exec(ctx, i.m, e, i.now(), func(context.Context, any) {}); err != nil && first == nil {
				first = err
			}
		case EffectStartInvocation:
			i.startService(ctx, e)
		case EffectStopInvocation:
			i.mu.Lock()
			run, ok := i.running[e.ID]
			delete(i.running, e.ID)
			i.mu.Unlock()

			if ok {
				run.stop()
				if self, _ := ctx.Value(deliveringRun{}).(string); self != e.ID {
					<-run.done
				}
			}
		case EffectSend:
			i.mu.Lock()
			run, ok := i.running[e.ID]
			i.mu.Unlock()

			if ok && !run.inbox.put(e.Event) {
				i.overflow(e.ID, run)
			}
		case EffectEmit:
			if err := listen(ctx, i.emit, e); err != nil && first == nil {
				first = err
			}
		case EffectStartTimer:
			i.schedule(e)
		case EffectCancelTimer:
			i.mu.Lock()
			if w, ok := i.wakeups[e.ID]; ok {
				w.Stop()
				delete(i.wakeups, e.ID)
			}
			i.mu.Unlock()
		}
	}

	return first
}

// startService runs a service on a goroutine of its own. Its events are
// processed when the run can take the turn, and dropped once the run has
// been stopped.
func (i *Instance[S, C]) startService(ctx context.Context, e Effect[C]) {
	box := newInbox(MaxInbox)
	runCtx, stop := context.WithCancel(serviceContext(context.WithoutCancel(ctx), box, i.now, i.manual))
	execCtx, cancel := context.WithCancel(runCtx)
	token, _ := ctx.Value(settleToken{}).(uint64)
	run := serviceRun{stop: stop, cancel: cancel, token: token, done: make(chan struct{}), inbox: box}

	deliver := func(evCtx context.Context, ev any) {
		// An acquired value is kept before the chart hears of it. One
		// that arrives after its state was left has been released, and
		// its event goes no further.
		if a, ok := ev.(acquisition); ok {
			if id, value := a.acquisition(); !i.res.store(evCtx, id, value) {
				return
			}
		}

		select {
		case <-i.turn:
		case <-runCtx.Done():
			return
		}
		defer func() { i.turn <- struct{}{} }()

		if runCtx.Err() != nil {
			return
		}

		processCtx := context.WithValue(context.WithoutCancel(evCtx), deliveringRun{}, e.ID)
		if _, err := i.process(processCtx, ev); err != nil && !errors.Is(err, ErrInstanceClosed) {
			// An action failed after the service's result was
			// accepted; there is no caller to return it to, so it
			// reaches the hooks only.
			return
		}
	}

	i.mu.Lock()
	if i.closed {
		i.mu.Unlock()
		cancel()
		stop()
		close(run.done)

		return
	}
	i.running[e.ID] = run
	if token != 0 {
		i.unsettled[token]++
	}

	// The goroutine is registered before mu is released: Close sets closed
	// under mu and then waits for runs, so it either sees this run or this
	// run sees closed.
	defer i.mu.Unlock()

	i.runs.Go(func() {
		defer close(run.done)
		defer stop()
		defer cancel()
		defer func() {
			i.mu.Lock()
			if current, ok := i.running[e.ID]; ok && current.done == run.done {
				delete(i.running, e.ID)
			}
			if token != 0 {
				// A run its result started is counted before this one
				// ends, so a count of zero means the token has settled.
				i.unsettled[token]--
				if i.unsettled[token] == 0 {
					delete(i.unsettled, token)
				}
			}
			close(i.ended)
			i.ended = make(chan struct{})
			i.mu.Unlock()
		}()

		if e.Kind == EffectAcquire {
			defer i.res.settle(e.ID)
		}

		if err := i.res.wait(execCtx, e.Needs); err != nil {
			deliver(execCtx, Failed{ID: e.ID, Err: err})
			return
		}

		if i.services != nil && e.Kind == EffectStartInvocation {
			if err := i.services.Acquire(execCtx, 1); err != nil {
				return
			}
			defer i.services.Release(1)
		}

		if i.throttle != nil && e.Kind == EffectStartInvocation {
			if err := i.throttle.Wait(execCtx); err != nil {
				return
			}
		}

		if err := i.hooks.Exec(execCtx, i.m, e, i.now(), deliver); err != nil {
			deliver(execCtx, Failed{ID: e.ID, Err: err})
		}
	})
}

// overflow stops a service run that fell too far behind the events sent to
// it, and tells the chart it failed.
func (i *Instance[S, C]) overflow(id string, run serviceRun) {
	i.mu.Lock()
	if current, ok := i.running[id]; ok && current.done == run.done {
		delete(i.running, id)
	}
	if !i.closed {
		i.runs.Go(func() {
			// The failure has no caller to report an action's error to.
			_, _ = i.Send(i.base, Failed{ID: id, Err: ErrInboxFull})
		})
	}
	i.mu.Unlock()

	run.stop()
}

// schedule wakes the instance when a timer is due, unless timers are driven
// by the caller.
func (i *Instance[S, C]) schedule(e Effect[C]) {
	if i.manual {
		return
	}

	wait := e.Wait(i.now(), rand.Float64)

	i.mu.Lock()
	defer i.mu.Unlock()

	if i.closed {
		return
	}

	i.wakeups[e.ID] = time.AfterFunc(wait, func() {
		i.mu.Lock()
		delete(i.wakeups, e.ID)
		closed := i.closed
		i.mu.Unlock()

		if !closed {
			// A wake-up has no caller to report an action's error to;
			// the hooks see it.
			_, _ = i.Send(i.base, TimerFired{ID: e.ID})
		}
	})
}
