// Package pgdriver runs statechart instances and keeps their state in
// PostgreSQL.
//
// [Driver.SendTx] applies one event in the transaction of the caller. It
// does these steps:
//
//   - It locks the instance through its [Store].
//   - It applies the event with the pure transition function of the machine,
//     at the time of the database.
//   - It saves the new snapshot.
//   - It records the effects of the step through its [Sink].
//
// As a result, the state change and its effects commit together with all
// other data that the caller writes. They also come after all locks that the
// caller holds. [Driver.Send] does the same in a transaction of its own.
//
// By default, the driver keeps instances in its own table ([TableStore]) and
// effects in its own outbox ([Outbox]). [Driver.Work] does the effects in the
// outbox. A store can keep the snapshot on the row of the domain instead, and
// copy the state into indexed columns. A sink can give effects to an outbox
// that already exists. The consumer of that outbox calls [Driver.Perform].
//
// The driver delivers each effect one or more times. If a worker stops after
// it runs an effect but before it removes the effect, the next worker will
// run the effect again. Thus, actions and services must be idempotent: two
// runs must have the same result as one run. The machine ignores an event
// that it gets twice for this reason, because the related timer or service
// run is not pending any more.
//
// [Schema] defines the default tables. The driver does not make them.
package pgdriver

import (
	"context"
	_ "embed"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/speakeasy-api/sc"
)

// Schema is the SQL that makes the tables and indexes of the default store
// and the default outbox. You can run it more than one time with the same
// result.
//
//go:embed schema.sql
var Schema string

// defaultPollInterval is how long an idle worker waits before it looks for
// due effects again. It bounds how late a timer can fire and how long an
// effect waits when every worker was idle, so it trades latency for load on
// an empty outbox.
const defaultPollInterval = time.Second

var (
	// ErrNotFound is the error that the driver returns if no instance has
	// the given ID.
	ErrNotFound = errors.New("statechart instance not found")

	// ErrExists is the error that [Driver.Create] returns if an instance
	// with the ID exists already.
	ErrExists = errors.New("statechart instance already exists")

	// ErrNoOutbox is the error that [Driver.Work] and [Driver.Step] return
	// if the sink of the driver is not the built-in [Outbox].
	ErrNoOutbox = errors.New("driver does not use the built-in outbox")

	// ErrResources is the error that [New] returns for a chart that
	// declares resources with Holds. A resource stays in one process. But
	// any worker process can do the effects of an instance, thus this
	// driver cannot hold a resource. Run such a chart with sc.Instance or
	// sc.Actor.
	ErrResources = errors.New("chart declares resources, which the Postgres driver cannot hold")

	// ErrMessaging is the error that [New] returns for a chart that sends
	// events to its services while they run. A service runs in the worker
	// process that claimed it. The event goes to a different process, thus
	// the event cannot get to the service. Run such a chart with
	// sc.Instance or sc.Actor.
	ErrMessaging = errors.New("chart sends events to its services, which the Postgres driver cannot deliver")
)

// Driver stores and runs the instances of one machine. It is safe for
// concurrent use. Many processes can run drivers for the same machine on the
// same database at the same time.
type Driver[S comparable, C any] struct {
	pool      *pgxpool.Pool
	m         *sc.Machine[S, C]
	poll      time.Duration
	hooks     *sc.Hooks[S, C]
	store     Store[S, C]
	sink      Sink[C]
	outbox    bool
	listeners map[reflect.Type][]func(ctx context.Context, instance string, ev any) error
}

// config holds what the Option values passed to New configure.
type config struct {
	poll time.Duration

	// hooks, store and sink hold values typed for the driver's S and C,
	// checked by New.
	hooks any
	store any
	sink  any

	listeners map[reflect.Type][]func(ctx context.Context, instance string, ev any) error
}

// Option configures a [Driver].
type Option func(*config)

// WithPollInterval sets the time that an idle worker waits before it looks
// for due effects again. If the duration is zero or less, the driver keeps
// the default. Thus, a worker always pauses between two polls.
func WithPollInterval(d time.Duration) Option {
	return func(c *config) {
		if d > 0 {
			c.poll = d
		}
	}
}

// Listen makes the driver call fn with each event of type T that a chart
// emits. The chart emits an event with [sc.Transition.Emit] or with an entry
// or exit step. The driver also gives fn the ID of the instance that emitted
// the event. Go infers T from fn. Use one Listen for each type of event. New
// fails for a listener of a type that the chart never emits.
//
// The driver records an emitted event with the step that made it. It
// delivers the event after that step commits. Thus, fn never gets an event
// from a step that the database rolled back. The driver delivers each event
// one or more times. If fn returns an error, the driver delivers the event
// again later. Thus, fn must be idempotent.
//
// The events of one instance arrive in the order that the instance emitted
// them, in sequence with its actions. The driver discards an event if no
// listener is set for its type.
func Listen[T any](fn func(ctx context.Context, instance string, ev T) error) Option {
	return func(c *config) {
		t := reflect.TypeFor[T]()
		c.listeners[t] = append(c.listeners[t], func(ctx context.Context, instance string, ev any) error {
			typed, ok := ev.(T)
			if !ok {
				return nil
			}

			return fn(ctx, instance, typed)
		})
	}
}

// WithHooks makes the driver report to h. [Driver.Send] reports a step after
// the transaction that saved the step commits. Thus, the driver never
// reports a step that the database rolled back. [Driver.SendTx] cannot know
// when the caller commits. Call [Driver.Report] after the commit. To create
// an instance is not a step, and the driver does not report it. New panics
// if h is for a different state type or context type.
func WithHooks[S comparable, C any](h *sc.Hooks[S, C]) Option {
	return func(c *config) { c.hooks = h }
}

// WithStore makes the driver keep snapshots in store instead of its own
// table. New panics if store is for a different state type or context type.
func WithStore[S comparable, C any](store Store[S, C]) Option {
	return func(c *config) { c.store = store }
}

// WithSink makes the driver record effects in sink instead of its own
// outbox. You cannot then use [Driver.Work]. The consumer of the sink does
// the effects with [Driver.Perform]. New panics if sink is for a different
// context type.
func WithSink[C any](sink Sink[C]) Option {
	return func(c *config) { c.sink = sink }
}

// New returns a driver for the instances of m. If the chart declares
// resources, New fails with an error that wraps [ErrResources]. If the chart
// sends events to its services, New fails with [ErrMessaging].
func New[S comparable, C any](pool *pgxpool.Pool, m *sc.Machine[S, C], opts ...Option) (*Driver[S, C], error) {
	if resources := m.Resources(); len(resources) > 0 {
		return nil, fmt.Errorf("%w: %s", ErrResources, strings.Join(resources, ", "))
	}

	if m.Sends() {
		return nil, ErrMessaging
	}

	cfg := config{poll: defaultPollInterval, hooks: nil, store: nil, sink: nil, listeners: map[reflect.Type][]func(context.Context, string, any) error{}}
	for _, opt := range opts {
		opt(&cfg)
	}

	d := &Driver[S, C]{
		pool:      pool,
		m:         m,
		poll:      cfg.poll,
		hooks:     typed[*sc.Hooks[S, C]](cfg.hooks, "WithHooks", m.ID()),
		store:     typed[Store[S, C]](cfg.store, "WithStore", m.ID()),
		sink:      typed[Sink[C]](cfg.sink, "WithSink", m.ID()),
		outbox:    false,
		listeners: cfg.listeners,
	}

	for t := range cfg.listeners {
		if !m.EmitsType(t) {
			return nil, fmt.Errorf("%w: %s", sc.ErrNoSuchEmit, t)
		}
	}

	if d.store == nil {
		d.store = TableStore[S, C]{Machine: m.ID()}
	}
	if d.sink == nil {
		d.sink = Outbox[S, C]{Machine: m}
		d.outbox = true
	}

	return d, nil
}

// typed asserts an option's value to the type the driver needs, panicking
// with the option's name when it was built for another machine.
func typed[T any](v any, option, machine string) T {
	var zero T
	if v == nil {
		return zero
	}

	t, ok := v.(T)
	if !ok {
		panic(fmt.Sprintf("pgdriver: %s was given %T, which is not for machine %q", option, v, machine))
	}

	return t
}

// Machine returns the machine the driver runs.
func (d *Driver[S, C]) Machine() *sc.Machine[S, C] { return d.m }

// Create stores a new instance with context c in a transaction of its own.
// It returns the first snapshot of the instance.
func (d *Driver[S, C]) Create(ctx context.Context, id string, c C) (sc.Snapshot[S, C], error) {
	var snap sc.Snapshot[S, C]

	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var err error
		snap, err = d.CreateTx(ctx, tx, id, c)

		return err
	})
	if err != nil {
		return snap, fmt.Errorf("create transaction: %w", err)
	}

	return snap, nil
}

// CreateTx stores a new instance with context c in tx. The instance starts at
// the time of the database. CreateTx also records the effects that occur
// when the instance enters its initial states.
func (d *Driver[S, C]) CreateTx(ctx context.Context, tx pgx.Tx, id string, c C) (sc.Snapshot[S, C], error) {
	var zero sc.Snapshot[S, C]

	now, err := dbNow(ctx, tx)
	if err != nil {
		return zero, fmt.Errorf("create instance %q: %w", id, err)
	}

	snap, effects := d.m.Start(c, now, sc.RandomSeed())

	data, err := json.Marshal(snap)
	if err != nil {
		return snap, fmt.Errorf("create instance %q: marshal snapshot: %w", id, err)
	}

	if err := d.store.Insert(ctx, tx, id, snap, data); err != nil {
		return snap, fmt.Errorf("create instance %q: %w", id, err)
	}

	if err := d.sink.Enqueue(ctx, tx, id, now, effects); err != nil {
		return snap, fmt.Errorf("create instance %q: %w", id, err)
	}

	return snap, nil
}

// Load returns an instance as it is now, by the clock of the database. It
// returns the stored snapshot after it fires all timers that are due, as
// [sc.Machine.Advance] fires them. Load writes nothing. Thus, a read shows
// the result of a due timer, for example an expiry, before a worker processes
// that timer.
func (d *Driver[S, C]) Load(ctx context.Context, id string) (sc.Snapshot[S, C], error) {
	var zero sc.Snapshot[S, C]

	data, err := d.store.Read(ctx, d.pool, id)
	if err != nil {
		return zero, fmt.Errorf("load instance %q: %w", id, err)
	}

	now, err := dbNow(ctx, d.pool)
	if err != nil {
		return zero, fmt.Errorf("load instance %q: %w", id, err)
	}

	snap, _, err := d.m.Restore(data, now)
	if err != nil {
		return zero, fmt.Errorf("load instance %q: %w", id, err)
	}

	current, _ := d.m.Advance(snap, now)

	return current, nil
}

// Send applies one event to an instance in a transaction of its own. After
// the transaction commits, Send reports the step to the hooks of the driver.
// If many calls send to the same instance at the same time, the driver
// applies them one after the other.
func (d *Driver[S, C]) Send(ctx context.Context, id string, ev any) (sc.Step[S, C], error) {
	var step sc.Step[S, C]

	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var err error
		step, err = d.SendTx(ctx, tx, id, ev)

		return err
	})
	if err != nil {
		return step, fmt.Errorf("send transaction: %w", err)
	}

	d.Report(ctx, step)

	return step, nil
}

// SendTx applies one event to an instance in tx, at the time of the
// database. It locks the instance through the store. Thus, get all locks
// that must come first before you call SendTx.
//
// For an accepted step, SendTx writes the snapshot and effects in tx, and
// they commit with tx. For an ignored step, SendTx writes nothing, with one
// exception: the restore of the stored snapshot can make effects, for
// example after a migration. In that case, SendTx writes the restored
// snapshot and those effects. Use [sc.Step.Accepted] to know which type of
// step occurred. Call [Driver.Report] after the commit to send the step to
// the hooks.
func (d *Driver[S, C]) SendTx(ctx context.Context, tx pgx.Tx, id string, ev any) (sc.Step[S, C], error) {
	var step sc.Step[S, C]

	if err := d.m.CheckEvent(ev); err != nil {
		return step, fmt.Errorf("send to instance %q: %w", id, err)
	}

	data, err := d.store.Lock(ctx, tx, id)
	if err != nil {
		return step, fmt.Errorf("send to instance %q: %w", id, err)
	}

	now, err := dbNow(ctx, tx)
	if err != nil {
		return step, fmt.Errorf("send to instance %q: %w", id, err)
	}

	snap, restored, err := d.m.Restore(data, now)
	if err != nil {
		return step, fmt.Errorf("send to instance %q: restore snapshot: %w", id, err)
	}

	next, effects := d.m.Transition(snap, ev, now)
	step = sc.Step[S, C]{At: now, From: snap, Event: ev, To: next, Effects: effects}

	if !step.Accepted() && len(restored) == 0 {
		return step, nil
	}

	encoded, err := json.Marshal(next)
	if err != nil {
		return step, fmt.Errorf("send to instance %q: marshal snapshot: %w", id, err)
	}

	if err := d.store.Update(ctx, tx, id, step, encoded); err != nil {
		return step, fmt.Errorf("send to instance %q: %w", id, err)
	}

	if err := d.sink.Enqueue(ctx, tx, id, now, append(restored, effects...)); err != nil {
		return step, fmt.Errorf("send to instance %q: %w", id, err)
	}

	return step, nil
}

// Report gives a committed step to the hooks of the driver. [Driver.Send]
// calls Report itself. If you call [Driver.SendTx], call Report after your
// commit.
func (d *Driver[S, C]) Report(ctx context.Context, step sc.Step[S, C]) {
	d.hooks.Observe(ctx, step)
}

// dbNow reads the database's clock, which every decision the driver makes
// is measured by, so the chart and the queries that read its rows agree.
func dbNow(ctx context.Context, q Querier) (time.Time, error) {
	var now time.Time
	if err := q.QueryRow(ctx, `SELECT clock_timestamp()`).Scan(&now); err != nil {
		return time.Time{}, fmt.Errorf("read database clock: %w", err)
	}

	return now, nil
}

// Perform does one effect that a sink recorded. The effect has the encoding
// of [sc.Machine.EncodeEffect]. Perform sends the events that the effect
// makes to the instance: the TimerFired of a timer, or the results of a
// service. The consumer of a custom [Sink] uses Perform to run effects.
// [Driver.Work] uses Perform for the built-in outbox.
//
// For an action that the chart does not declare any more, Perform returns an
// error that wraps [sc.ErrUnknownEffect], because that action can never
// succeed. A service that the chart does not declare any more, or a service
// with no bound implementation, does not cause an error. Perform reports it
// to the instance as failed.
func (d *Driver[S, C]) Perform(ctx context.Context, instance string, data []byte) error {
	e, err := d.m.DecodeEffect(data)
	if err != nil {
		return fmt.Errorf("decode effect: %w", err)
	}

	if e.Kind == sc.EffectStartTimer {
		_, err := d.Send(ctx, instance, sc.TimerFired{ID: e.ID})
		return err
	}

	if e.Kind == sc.EffectEmit {
		// Every listener hears the event; the first error has it delivered
		// again, to all of them, which is why each must be idempotent.
		for _, listen := range d.listeners[reflect.TypeOf(e.Event)] {
			if err := listen(ctx, instance, e.Event); err != nil {
				return fmt.Errorf("deliver emitted event: %w", err)
			}
		}

		return nil
	}

	now, err := dbNow(ctx, d.pool)
	if err != nil {
		return fmt.Errorf("perform effect: %w", err)
	}

	// A service that is run again starts from the progress it has recorded
	// since the effect was written.
	if e.Kind == sc.EffectStartInvocation {
		snap, err := d.Load(ctx, instance)
		if err != nil {
			return fmt.Errorf("perform effect: %w", err)
		}
		if saved := snap.Checkpoint(e.ID); saved != nil {
			e.Saved = saved
		}
	}

	var sendErr error
	err = d.hooks.Exec(ctx, d.m, e, now, func(evCtx context.Context, ev any) {
		if sendErr == nil {
			_, sendErr = d.Send(evCtx, instance, ev)
		}
	})

	// A service the chart no longer declares fails like any other service,
	// which hands the decision to the chart's own error transition.
	if e.Kind == sc.EffectStartInvocation && (errors.Is(err, sc.ErrUnknownEffect) || errors.Is(err, sc.ErrNotBound)) {
		_, err = d.Send(ctx, instance, sc.Failed{ID: e.ID, Err: err})
	}
	if err != nil {
		return err
	}

	return sendErr
}

// Work does the due effects of the built-in outbox until ctx is cancelled or
// until it cannot process an effect. Then it returns the cause. Run Work on
// the number of goroutines or processes that the workload needs. A service
// keeps its worker busy until the service ends.
func (d *Driver[S, C]) Work(ctx context.Context) error {
	if !d.outbox {
		return ErrNoOutbox
	}

	for {
		worked, err := d.Step(ctx)
		if err != nil {
			return err
		}
		if worked {
			continue
		}

		idle := time.NewTimer(d.poll)
		select {
		case <-ctx.Done():
			idle.Stop()
			return fmt.Errorf("stop worker: %w", context.Cause(ctx))
		case <-idle.C:
		}
	}
}

// Step does a maximum of one due effect of the built-in outbox. It reports
// if it found an effect. The row of the effect stays locked while the effect
// runs. Step removes the row in the same transaction. Thus, no other worker
// gets the effect. If this worker stops, the effect is available again.
//
// Step does the actions and emitted events of one instance in the order that
// the machine made them. The services and timers of one instance do not
// wait for each other or for an action that runs.
//
// Step keeps one pool connection for the claim while the effect runs. The
// delivery of a timer or of a service result uses a second connection for
// its own transaction. Make the pool larger than the number of workers that
// call Step. If not, the workers can hold all connections, and each worker
// waits for a different one.
func (d *Driver[S, C]) Step(ctx context.Context) (bool, error) {
	if !d.outbox {
		return false, ErrNoOutbox
	}

	worked := false

	// dropped is set when the claimed effect no longer matches the chart.
	// Such an effect can never run, so its row is removed like a completed
	// one and the error is reported after the removal commits.
	var dropped error

	err := pgx.BeginFunc(ctx, d.pool, func(tx pgx.Tx) error {
		var (
			rowID    int64
			instance string
			kind     string
			data     []byte
		)

		err := tx.QueryRow(ctx, `
			SELECT e.id, e.instance_id, e.kind, e.effect
			FROM statechart_effects e
			WHERE e.machine = $1
			  AND e.run_after <= clock_timestamp()
			  AND (NOT (e.kind = ANY($2)) OR NOT EXISTS (
				SELECT 1 FROM statechart_effects earlier
				WHERE earlier.machine = e.machine
				  AND earlier.instance_id = e.instance_id
				  AND earlier.kind = ANY($2)
				  AND earlier.id < e.id
			  ))
			ORDER BY e.id
			LIMIT 1
			FOR UPDATE OF e SKIP LOCKED`,
			d.m.ID(), []string{string(sc.EffectAction), string(sc.EffectEmit)},
		).Scan(&rowID, &instance, &kind, &data)
		switch {
		case errors.Is(err, pgx.ErrNoRows):
			return nil
		case err != nil:
			return fmt.Errorf("claim effect: %w", err)
		}

		worked = true

		err = d.Perform(ctx, instance, data)
		switch {
		case errors.Is(err, sc.ErrUnknownEffect):
			dropped = fmt.Errorf("drop %s effect of instance %q: %w", kind, instance, err)
		case err != nil:
			return fmt.Errorf("run %s effect of instance %q: %w", kind, instance, err)
		}

		if _, err := tx.Exec(ctx, `DELETE FROM statechart_effects WHERE id = $1`, rowID); err != nil {
			return fmt.Errorf("remove effect: %w", err)
		}

		return nil
	})
	if err != nil {
		return worked, fmt.Errorf("process effect: %w", err)
	}

	return worked, dropped
}
