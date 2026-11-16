package pgdriver

import (
	"context"
	"fmt"
	"math/rand/v2"
	"time"

	"github.com/jackc/pgx/v5"

	"github.com/speakeasy-api/sc"
)

// Sink records the effects of a step in the transaction that saves the step.
// Thus, the sink records an effect only if the database also records the
// state change that caused the effect. The consumer of the sink does each
// effect with [Driver.Perform].
//
// Effects arrive in the order that they must occur. If a sink cannot obey
// cancellations (timer.cancel and invocation.stop effects), it can ignore
// them. The machine ignores the events that a cancelled timer or a stopped
// service makes later.
//
// The default sink, [Outbox], keeps effects in the table of the driver.
// [Driver.Work] consumes that sink. A sink can also give effects to an outbox
// that exists already, for example an outbox that publishes to a message bus.
type Sink[C any] interface {
	// Enqueue records effects for the instance with the given ID. now is
	// the database time of the step. Timers start from this time.
	Enqueue(ctx context.Context, tx pgx.Tx, instance string, now time.Time, effects []sc.Effect[C]) error
}

// Outbox keeps effects in the statechart_effects table that [Schema]
// makes. [Driver.Work] does these effects.
type Outbox[S comparable, C any] struct {
	// Machine encodes the effects and identifies the machine that owns
	// them.
	Machine *sc.Machine[S, C]
}

// Enqueue inserts one row for each effect to do. The row for an action, an
// emitted event or a service is due immediately. The row for a timer is due
// at the time of the timer. Enqueue also deletes the rows that a
// cancellation refers to.
func (o Outbox[S, C]) Enqueue(ctx context.Context, tx pgx.Tx, instance string, now time.Time, effects []sc.Effect[C]) error {
	for _, e := range effects {
		switch e.Kind {
		case sc.EffectCancelTimer, sc.EffectStopInvocation:
			// A row a worker has locked is being carried out right
			// now. It is skipped, not waited for: the machine
			// ignores the event it produces.
			_, err := tx.Exec(ctx, `
				DELETE FROM statechart_effects
				WHERE id IN (
					SELECT id FROM statechart_effects
					WHERE machine = $1 AND instance_id = $2 AND effect_id = $3
					FOR UPDATE SKIP LOCKED
				)`,
				o.Machine.ID(), instance, e.ID,
			)
			if err != nil {
				return fmt.Errorf("cancel effect %q: %w", e.ID, err)
			}
		case sc.EffectAction, sc.EffectStartInvocation, sc.EffectStartTimer, sc.EffectEmit:
			data, err := o.Machine.EncodeEffect(e)
			if err != nil {
				return fmt.Errorf("encode %s effect: %w", e.Kind, err)
			}

			// A timer is due at the time the core gave it, measured
			// by the database's clock like now, varied by its jitter.
			runAfter := now
			if e.Kind == sc.EffectStartTimer {
				runAfter = now.Add(e.Wait(now, rand.Float64))
			}

			_, err = tx.Exec(ctx, `
				INSERT INTO statechart_effects (machine, instance_id, kind, effect_id, effect, run_after)
				VALUES ($1, $2, $3, $4, $5, $6)`,
				o.Machine.ID(), instance, string(e.Kind), e.ID, data, runAfter,
			)
			if err != nil {
				return fmt.Errorf("insert %s effect: %w", e.Kind, err)
			}
		case sc.EffectAcquire, sc.EffectRelease:
			// New rejects charts that declare resources, so these only
			// arrive from a sink used outside the driver.
			return fmt.Errorf("%w: %s effect", ErrResources, e.Kind)
		case sc.EffectSend:
			// New rejects charts that send to their services, likewise.
			return fmt.Errorf("%w: %s effect", ErrMessaging, e.Kind)
		}
	}

	return nil
}
