package sc_test

import (
	"context"
	"fmt"
	"time"

	"github.com/speakeasy-api/sc"
)

type doorState string

const (
	doorClosed doorState = "closed"
	doorOpen   doorState = "open"
	doorLocked doorState = "locked"
)

type (
	openDoor   struct{}
	closeDoor  struct{}
	lockDoor   struct{}
	unlockDoor struct{}
)

type door struct {
	Opens int `json:"opens"`
}

func newDoor() (*sc.Machine[doorState, door], error) {
	m, err := sc.New("door", func(b *sc.Builder[doorState, door]) {
		b.Initial(doorClosed)

		closed := b.State(doorClosed)
		closed.On[openDoor]().
			Assign("countOpen", func(d door, _ openDoor) door {
				d.Opens++
				return d
			}).
			To(doorOpen)
		closed.On[lockDoor]().To(doorLocked)

		open := b.State(doorOpen)
		open.On[closeDoor]().To(doorClosed)
		// Nobody holds the door open forever.
		open.After(30 * time.Second).To(doorClosed)

		b.State(doorLocked).On[unlockDoor]().To(doorClosed)
	})
	if err != nil {
		return nil, fmt.Errorf("build door chart: %w", err)
	}

	return m, nil
}

// The core is pure: Transition takes a snapshot, an event and the time, and
// returns the next snapshot and the effects the caller must carry out.
func ExampleMachine_Transition() {
	m, err := newDoor()
	if err != nil {
		panic(err)
	}

	now := time.Date(2026, 1, 1, 9, 0, 0, 0, time.UTC)
	snap, _ := m.Start(door{}, now)
	snap, _ = m.Transition(snap, openDoor{}, now)
	fmt.Println(snap.Active(), snap.Context.Opens)

	// The open door's timer is due by now, so it fires before the event.
	snap, _ = m.Transition(snap, lockDoor{}, now.Add(time.Minute))
	fmt.Println(snap.Active())
	// Output:
	// [open] 1
	// [locked]
}

// An Instance runs the chart on its callers' goroutines and carries out the
// effects itself.
func ExampleMachine_Instance() {
	m, err := newDoor()
	if err != nil {
		panic(err)
	}

	ctx := context.Background()
	inst, err := m.Instance(ctx, door{})
	if err != nil {
		panic(err)
	}
	defer inst.Close()

	step, err := inst.Send(ctx, openDoor{})
	if err != nil {
		panic(err)
	}
	fmt.Println(step.Accepted(), inst.Snapshot().Active())

	// A type the chart never names is refused, instead of silently ignored.
	_, err = inst.Send(ctx, &openDoor{})
	fmt.Println(err != nil)
	// Output:
	// true [open]
	// true
}
