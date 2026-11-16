// Package payment is a full example of a chart that has its design from a
// TLA+ model. The chart is an order with these properties:
//
//   - It charges a card.
//   - After a decline, it waits and then charges again.
//   - The customer can cancel it.
//
// The model is Payment.tla in this directory. Payment.cfg examines the model
// in approximately one second. PaymentLarge.cfg examines it with more
// declines and more lost answers. CI does not run these checks. Thus, if you
// change the chart, you must also change the model and examine it again.
//
// The model showed that two simpler designs are not correct. Each design is
// still in the model behind a constant, so that you can make its trace again.
//
// First design: a cancel cannot only move the order to cancelled while a
// charge can still occur. The charge can already be on its way to the
// payment provider. Its answer will then arrive at a chart that does not
// listen any more, and the customer pays for a cancelled order. Instead, the
// chart records the request and continues until it knows the result. If the
// charge succeeds, the chart refunds it. If the charge is declined, the chart
// does nothing more.
//
// Second design: a charge that ends without a clear answer, for example a
// timeout, is not a decline. The card can be charged already. Thus, a second
// try as a new request can charge the card two times. Instead, the chart asks
// again with the same idempotency key until the provider gives a clear
// answer.
package payment

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/speakeasy-api/sc"
)

// State identifies a state of the chart.
type State string

const (
	// Idle is an order that the customer did not submit yet.
	Idle State = "idle"

	// Charging is an order for which the chart charges the card now.
	Charging State = "charging"

	// Retrying is an order that waits after a decline, before the chart
	// charges the card again.
	Retrying State = "retrying"

	// Paid is an order with a charge that succeeded.
	Paid State = "paid"

	// Cancelled is an order that will not be paid.
	Cancelled State = "cancelled"
)

// MaxAttempts is the number of declined charges that the chart tries again
// before it cancels the order. If a card is declined three times in
// sequence, it will not work until the customer changes something.
const MaxAttempts = 2

// RetryDelay is the time that the chart waits before it charges again. A
// temporary hold frequently causes a decline. Usually, the hold stops in
// less than one minute.
const RetryDelay = time.Minute

// Order is the context of the chart.
type Order struct {
	// ID identifies the order to the payment provider.
	ID string `json:"id"`

	// Attempts is the number of declined charges until now.
	Attempts int `json:"attempts"`

	// CancelRequested is true if the customer cancels while a charge can
	// still occur.
	CancelRequested bool `json:"cancel_requested"`

	// Unresolved is true while the last charge has no clear answer. In
	// that condition, the chart does not know if the card was charged.
	Unresolved bool `json:"unresolved"`

	// Receipt is the reference of the provider for the charge that
	// succeeded.
	Receipt string `json:"receipt"`
}

// IdempotencyKey identifies one charge attempt to the payment provider. The
// chart sends an attempt again if a worker repeats it or if its answer is
// lost. The key lets the provider know that all of these are one request.
// Only a clear decline starts a new attempt with a new key.
func (o Order) IdempotencyKey() string {
	return fmt.Sprintf("%s/%d", o.ID, o.Attempts)
}

// Submit is the request to pay for the order.
type Submit struct{}

// Cancel is the request to cancel the order.
type Cancel struct{}

// ErrDeclined is the error that a [Provider] returns if it refused the
// charge and took no money.
var ErrDeclined = errors.New("card declined")

// Provider is the payment provider that the chart uses for charges and
// refunds.
type Provider interface {
	// Charge charges the card of the order and returns a receipt. If the
	// provider refused the charge, Charge returns an error that wraps
	// ErrDeclined. All other errors mean that the result is not known. The
	// provider must decide one time for each idempotency key. It must give
	// the same result to all later requests with that key.
	Charge(ctx context.Context, idempotencyKey string) (receipt string, err error)

	// Refund asks the provider to return the money of the charge with the
	// given receipt. Refund reports nothing, because an action cannot
	// cause a transition to fail. A driver can deliver Refund more than one
	// time. Thus, the implementation must be idempotent for each receipt.
	// It must also try again until the provider has the request, for
	// example through a queue.
	Refund(ctx context.Context, receipt string)
}

// New makes the chart.
func New(provider Provider) (*sc.Machine[State, Order], error) {
	m, err := sc.New("payment", func(b *sc.Builder[State, Order]) {
		b.Initial(Idle)

		b.State(Idle).On[Submit]().To(Charging)

		charging := b.State(Charging)
		charging.On[Cancel]().Assign("requestCancel", requestCancel)

		charge := charging.Invoke("charge", func(ctx context.Context, _ *sc.Run, o Order) (string, error) {
			return provider.Charge(ctx, o.IdempotencyKey())
		})
		charge.OnDone().
			When("cancelRequested", func(o Order, _ sc.Done[string]) bool { return o.CancelRequested }).
			Assign("resolveCharged", resolve[sc.Done[string]]).
			Do("refund", func(ctx context.Context, _ Order, d sc.Done[string]) { provider.Refund(ctx, d.Result) }).
			To(Cancelled)
		charge.OnDone().
			Assign("setReceipt", func(o Order, d sc.Done[string]) Order {
				o.Receipt = d.Result
				o.Unresolved = false

				return o
			}).
			To(Paid)
		charge.OnError().
			When("outcomeUnknown", func(_ Order, f sc.Failed) bool { return !errors.Is(f.Err, ErrDeclined) }).
			Assign("markUnresolved", func(o Order, _ sc.Failed) Order {
				o.Unresolved = true
				return o
			}).
			To(Retrying)
		charge.OnError().
			When("canRetry", func(o Order, _ sc.Failed) bool { return !o.CancelRequested && o.Attempts < MaxAttempts }).
			Assign("countAttempt", func(o Order, _ sc.Failed) Order {
				o.Attempts++
				o.Unresolved = false

				return o
			}).
			To(Retrying)
		charge.OnError().Assign("resolveDeclined", resolve[sc.Failed]).To(Cancelled)

		retrying := b.State(Retrying)
		retrying.After(RetryDelay).To(Charging)
		retrying.On[Cancel]().
			When("chargeMayHaveLanded", func(o Order, _ Cancel) bool { return o.Unresolved }).
			Assign("requestCancel", requestCancel)
		retrying.On[Cancel]().To(Cancelled)

		b.Final(Paid)
		b.Final(Cancelled)
	})
	if err != nil {
		return nil, fmt.Errorf("build payment chart: %w", err)
	}

	return m, nil
}

// resolve records that the last charge got a clear answer.
func resolve[Ev any](o Order, _ Ev) Order {
	o.Unresolved = false
	return o
}

func requestCancel(o Order, _ Cancel) Order {
	o.CancelRequested = true
	return o
}
