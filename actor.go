package sc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"math/rand/v2"
	"slices"
	"sync"
	"sync/atomic"
	"time"

	"golang.org/x/sync/semaphore"
)

var (
	// ErrActorStopped is the error that [Actor.Send] returns after
	// [Actor.Run] returns.
	ErrActorStopped = errors.New("actor has stopped")

	// ErrActorStarted is the error that [Actor.Run] returns when you call
	// it a second time.
	ErrActorStarted = errors.New("actor was already run")
)

// Actor runs one machine instance in memory. An Actor does these tasks:
//   - It processes one event at a time.
//   - It runs actions on the goroutine that called [Actor.Run].
//   - It runs each service on its own goroutine.
//   - It keeps timers with the time package.
//
// The data that an Actor holds does not survive the process. To continue
// after a restart, persist [Actor.Snapshot]. Then start again with
// [Machine.ActorFrom].
type Actor[S comparable, C any] struct {
	m *Machine[S, C]

	// mailbox carries events to the Run loop.
	mailbox chan envelope[S, C]

	// hooks are the functions the actor reports to; nil for none.
	hooks *Hooks[S, C]

	// services, when set, is acquired by each service run before it starts.
	services *semaphore.Weighted

	// now is the actor's clock.
	now func() time.Time

	// emit receives the events the chart emits, when set.
	emit func(context.Context, any)

	// invalid is why the actor cannot run, such as a listener for an event
	// the chart never emits; Run returns it.
	invalid error

	// throttle is what each service run waits on before it starts, when
	// set.
	throttle Waiter

	// stopped is closed when Run returns.
	stopped chan struct{}

	started atomic.Bool

	// initial holds the effects Run carries out before its first event.
	initial []Effect[C]

	mu sync.Mutex

	// snap is the current snapshot, guarded by mu.
	snap Snapshot[S, C]

	// changed is closed and replaced whenever snap is replaced, guarded
	// by mu.
	changed chan struct{}

	// due maps each scheduled timer's ID to when it fires, guarded by mu.
	due map[string]time.Time
}

// envelope is an event on its way to the Run loop, with the context it was
// sent with.
type envelope[S comparable, C any] struct {
	ctx context.Context //nolint:containedctx // carries the sender's trace context across the mailbox to the hooks.
	ev  any

	// reply, when set, receives the step once its effects have been
	// started.
	reply chan<- Step[S, C]
}

// Actor returns an actor for a new instance with context c. The instance
// starts at the time of the actor's clock. The actor does nothing until you
// call [Actor.Run].
func (m *Machine[S, C]) Actor(c C, opts ...RunOption) *Actor[S, C] {
	o := runSettings(opts)
	snap, effects := m.Start(c, o.now(), RandomSeed())

	return m.newActor(snap, effects, o)
}

// ActorFrom returns an actor that continues from snap. The actor schedules
// each pending timer for the time that it is due. The services that did not
// finish run again.
func (m *Machine[S, C]) ActorFrom(snap Snapshot[S, C], opts ...RunOption) *Actor[S, C] {
	return m.newActor(snap, m.Resume(snap), runSettings(opts))
}

func (m *Machine[S, C]) newActor(snap Snapshot[S, C], effects []Effect[C], o runOptions) *Actor[S, C] {
	hooks := hooksFor[S, C](o, m.id)

	return &Actor[S, C]{
		m:        m,
		mailbox:  make(chan envelope[S, C]),
		hooks:    hooks,
		services: o.services,
		now:      o.now,
		emit:     o.listener(),
		invalid:  m.checkListeners(o),
		throttle: o.throttle,
		stopped:  make(chan struct{}),
		started:  atomic.Bool{},
		initial:  effects,
		mu:       sync.Mutex{},
		snap:     snap,
		changed:  make(chan struct{}),
		due:      map[string]time.Time{},
	}
}

// Run processes events until one of these conditions occurs:
//   - The machine is done. Run returns nil.
//   - ctx is cancelled. Run returns the error of the context.
//
// Before Run returns, it stops all services and timers that are active. An
// actor runs only one time.
func (a *Actor[S, C]) Run(ctx context.Context) error {
	if !a.started.CompareAndSwap(false, true) {
		return ErrActorStarted
	}

	if a.invalid != nil {
		close(a.stopped)
		return a.invalid
	}
	defer close(a.stopped)

	ctx, cancel := context.WithCancel(ctx)

	res := newResourceTable(a.m, a.hooks)
	ctx = res.attach(ctx)

	// On the way out: acquisitions still in flight release what they get,
	// every service and acquisition is cancelled and waited for, and then
	// the held resources are released, the last acquired first.
	var services sync.WaitGroup
	defer res.releaseAll(context.WithoutCancel(ctx))
	defer services.Wait()
	defer cancel()
	defer res.abandon()

	// deliver sends an event from a service or timer to the loop, giving
	// up when the actor stops.
	deliver := func(evCtx context.Context, ev any) {
		select {
		case a.mailbox <- envelope[S, C]{ctx: context.WithoutCancel(evCtx), ev: ev, reply: nil}:
		case <-ctx.Done():
		}
	}

	stops := map[string]context.CancelFunc{}

	// inboxes holds, for each service run, where the chart's events for it
	// go.
	inboxes := map[string]*inbox{}

	// stopping holds the stopped service runs that have not returned yet;
	// a resource is released only once they have.
	var stopping []chan struct{}

	timers := map[string]*time.Timer{}
	defer func() {
		for _, t := range timers {
			t.Stop()
		}
	}()

	apply := func(effects []Effect[C]) error {
		var first error

		for _, e := range effects {
			switch e.Kind {
			case EffectAction:
				// A failed action does not hold up the rest of the step,
				// which the chart has already taken.
				if err := a.hooks.Exec(ctx, a.m, e, a.now(), deliver); err != nil && first == nil {
					first = err
				}
			case EffectStartInvocation:
				box := newInbox(MaxInbox)
				inboxes[e.ID] = box
				serviceCtx, stop := context.WithCancel(serviceContext(ctx, box, a.now, false))
				done := make(chan struct{})
				stops[e.ID] = func() {
					stop()

					// Runs that have already returned need no waiting.
					stopping = slices.DeleteFunc(stopping, func(d chan struct{}) bool {
						select {
						case <-d:
							return true
						default:
							return false
						}
					})
					stopping = append(stopping, done)
				}

				// A stopped service must not wait on the loop, which may
				// be waiting for it before releasing a resource.
				send := func(evCtx context.Context, ev any) {
					// Once stopped, nothing more is reported, even if
					// the loop happens to be ready for it.
					if serviceCtx.Err() != nil {
						return
					}

					select {
					case a.mailbox <- envelope[S, C]{ctx: context.WithoutCancel(evCtx), ev: ev, reply: nil}:
					case <-serviceCtx.Done():
					}
				}

				services.Go(func() {
					defer close(done)

					if err := res.wait(serviceCtx, e.Needs); err != nil {
						send(serviceCtx, Failed{ID: e.ID, Err: err})
						return
					}

					if a.services != nil {
						if err := a.services.Acquire(serviceCtx, 1); err != nil {
							return
						}
						defer a.services.Release(1)
					}

					if a.throttle != nil {
						if err := a.throttle.Wait(serviceCtx); err != nil {
							return
						}
					}

					if err := a.hooks.Exec(serviceCtx, a.m, e, a.now(), send); err != nil {
						send(serviceCtx, Failed{ID: e.ID, Err: err})
					}
				})
			case EffectStopInvocation:
				if stop, ok := stops[e.ID]; ok {
					stop()
					delete(stops, e.ID)
				}
				delete(inboxes, e.ID)
			case EffectSend:
				if box, ok := inboxes[e.ID]; ok && !box.put(e.Event) {
					// The run fell too far behind: it is stopped, and
					// the chart is told it failed.
					if stop, ok := stops[e.ID]; ok {
						stop()
						delete(stops, e.ID)
					}
					delete(inboxes, e.ID)

					services.Go(func() { deliver(ctx, Failed{ID: e.ID, Err: ErrInboxFull}) })
				}
			case EffectEmit:
				if err := listen(ctx, a.emit, e); err != nil && first == nil {
					first = err
				}
			case EffectAcquire:
				res.begin(e)

				// An acquired value is kept before the chart hears of
				// it. One that arrives after its state was left has been
				// released, and its event goes no further.
				keep := func(evCtx context.Context, ev any) {
					if acq, ok := ev.(acquisition); ok {
						if id, value := acq.acquisition(); !res.store(evCtx, id, value) {
							return
						}
					}

					deliver(evCtx, ev)
				}

				services.Go(func() {
					defer res.settle(e.ID)

					if err := res.wait(ctx, e.Needs); err != nil {
						deliver(ctx, Failed{ID: e.ID, Err: err})
						return
					}

					if err := a.hooks.Exec(ctx, a.m, e, a.now(), keep); err != nil {
						deliver(ctx, Failed{ID: e.ID, Err: err})
					}
				})
			case EffectRelease:
				for _, done := range stopping {
					<-done
				}
				stopping = nil

				res.releaseFor(ctx, e)
			case EffectStartTimer:
				now := a.now()
				wait := e.Wait(now, rand.Float64)
				a.setDue(e.ID, now.Add(wait))
				timers[e.ID] = time.AfterFunc(wait, func() {
					a.setDue(e.ID, time.Time{})
					deliver(ctx, TimerFired{ID: e.ID})
				})
			case EffectCancelTimer:
				if t, ok := timers[e.ID]; ok {
					t.Stop()
					delete(timers, e.ID)
				}
				a.setDue(e.ID, time.Time{})
			}
		}

		return first
	}

	if err := apply(a.initial); err != nil {
		return err
	}

	for {
		if a.Snapshot().Done() {
			return nil
		}

		select {
		case <-ctx.Done():
			return fmt.Errorf("run actor: %w", context.Cause(ctx))
		case env := <-a.mailbox:
			from := a.Snapshot()
			at := a.now()
			next, effects, _ := a.m.transition(from, env.ev, at)
			step := Step[S, C]{At: at, From: from, Event: env.ev, To: next, Effects: effects}
			a.hooks.Observe(env.ctx, step)

			// The new snapshot is published once its effects have been
			// carried out, so a reader never sees a state whose actions
			// are still to run. It is published even when an action
			// failed: the transition was taken, and the error goes to
			// the sender.
			// An action's error has no caller to go to under an actor;
			// the hooks see it, and the actor carries on.
			_ = apply(effects)
			a.publish(next)
			if env.reply != nil {
				env.reply <- step
			}
		}
	}
}

func (a *Actor[S, C]) publish(next Snapshot[S, C]) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if next.seq == a.snap.seq {
		return
	}

	a.snap = next
	close(a.changed)
	a.changed = make(chan struct{})
}

// Send delivers an event to the actor. Send returns after the actor takes
// the event. Send blocks while the actor is busy or before the actor starts.
// This gives backpressure to the sender. ctx sets the limit of the wait.
// The hooks of the actor see the event with the values of ctx, for example
// the span of the sender.
func (a *Actor[S, C]) Send(ctx context.Context, ev any) error {
	if err := a.m.CheckEvent(ev); err != nil {
		return fmt.Errorf("send to actor: %w", err)
	}

	select {
	case a.mailbox <- envelope[S, C]{ctx: context.WithoutCancel(ctx), ev: ev, reply: nil}:
		return nil
	case <-a.stopped:
		return ErrActorStopped
	case <-ctx.Done():
		return fmt.Errorf("send to actor: %w", context.Cause(ctx))
	}
}

// setDue records when a timer fires; the zero time forgets it.
func (a *Actor[S, C]) setDue(id string, at time.Time) {
	a.mu.Lock()
	defer a.mu.Unlock()

	if at.IsZero() {
		delete(a.due, id)
		return
	}
	a.due[id] = at
}

// Timers returns the pending timers of the current snapshot, the soonest
// first. Due is the time at which the actor scheduled the timer to fire,
// with jitter. Run can be slow to get to a timer. Until the actor schedules
// that timer, the timer keeps the Due of its snapshot entry.
func (a *Actor[S, C]) Timers() []PendingTimer[S] {
	a.mu.Lock()
	defer a.mu.Unlock()

	timers := a.snap.Timers()
	for i := range timers {
		if at, ok := a.due[timers[i].ID]; ok {
			timers[i].Due = at
		}
	}

	slices.SortStableFunc(timers, func(x, y PendingTimer[S]) int { return x.Due.Compare(y.Due) })

	return timers
}

// Advance tells the actor to fire the timers that are due now, by the clock
// of the actor. Advance sends no other event. Advance returns after the
// actor takes the request.
func (a *Actor[S, C]) Advance(ctx context.Context) error {
	return a.Send(ctx, nil)
}

// Call sends ev and waits until the actor processes it. When Call returns,
// the actor took the transition, ran its actions, and started its services
// and timers. The step that Call returns shows if the actor accepted the
// event. If step.To.Seq() is different from step.From.Seq(), the event
// changed the instance. ctx sets the limit of the two waits.
func (a *Actor[S, C]) Call(ctx context.Context, ev any) (Step[S, C], error) {
	if err := a.m.CheckEvent(ev); err != nil {
		return Step[S, C]{}, fmt.Errorf("call actor: %w", err)
	}

	reply := make(chan Step[S, C], 1)

	select {
	case a.mailbox <- envelope[S, C]{ctx: context.WithoutCancel(ctx), ev: ev, reply: reply}:
	case <-a.stopped:
		return Step[S, C]{}, ErrActorStopped
	case <-ctx.Done():
		return Step[S, C]{}, fmt.Errorf("call actor: %w", context.Cause(ctx))
	}

	select {
	case step := <-reply:
		return step, nil
	case <-ctx.Done():
		return Step[S, C]{}, fmt.Errorf("call actor: %w", context.Cause(ctx))
	}
}

// Await blocks until cond is true for the current snapshot of the actor.
// Then Await returns that snapshot. Await examines the snapshot first, and
// again after each change. Await fails in these conditions:
//   - The actor stops and cond is not true. Await returns [ErrActorStopped].
//   - ctx ends first. Await returns the error of the context.
func (a *Actor[S, C]) Await(ctx context.Context, cond func(Snapshot[S, C]) bool) (Snapshot[S, C], error) {
	for {
		a.mu.Lock()
		snap, changed := a.snap, a.changed
		a.mu.Unlock()

		if cond(snap) {
			return snap, nil
		}

		select {
		case <-changed:
		case <-a.stopped:
			if final := a.Snapshot(); cond(final) {
				return final, nil
			}

			return a.Snapshot(), ErrActorStopped
		case <-ctx.Done():
			return snap, fmt.Errorf("await actor: %w", context.Cause(ctx))
		}
	}
}

// Snapshot returns the current snapshot of the actor.
func (a *Actor[S, C]) Snapshot() Snapshot[S, C] {
	a.mu.Lock()
	defer a.mu.Unlock()

	return a.snap
}

// Snapshots yields the current snapshot and then each snapshot after it.
// A consumer can be slower than the actor. In that case, the consumer gets
// only the latest snapshot when it asks for the next one. Thus the consumer
// never stops the actor. The sequence ends in these conditions:
//   - The consumer breaks the loop.
//   - ctx is cancelled.
//   - The actor stopped and the consumer got its last snapshot.
func (a *Actor[S, C]) Snapshots(ctx context.Context) iter.Seq[Snapshot[S, C]] {
	return func(yield func(Snapshot[S, C]) bool) {
		first := true
		var seen uint64

		for {
			a.mu.Lock()
			snap, changed := a.snap, a.changed
			a.mu.Unlock()

			if first || snap.seq != seen {
				first, seen = false, snap.seq
				if !yield(snap) {
					return
				}

				continue
			}

			select {
			case <-changed:
			case <-ctx.Done():
				return
			case <-a.stopped:
				// Run publishes its last snapshot before it
				// returns, so one more look is enough.
				if a.Snapshot().seq == seen {
					return
				}
			}
		}
	}
}
