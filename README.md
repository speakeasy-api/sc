# sc

[![Go Reference](https://pkg.go.dev/badge/github.com/speakeasy-api/sc.svg)](https://pkg.go.dev/github.com/speakeasy-api/sc)
[![CI](https://github.com/speakeasy-api/sc/actions/workflows/ci.yaml/badge.svg)](https://github.com/speakeasy-api/sc/actions/workflows/ci.yaml)

`sc` is a statechart library for Go. A statechart is a state machine with nested states, parallel regions, history, timers and services that run while a state is active.

`sc` is heavily inspired by [XState](https://github.com/statelyai/xstate) and is a best-effort port of its semantics to Go. It follows XState's behaviour where Go allows, and the tests note each place it deliberately differs.

The core is pure. `Machine.Transition` takes a snapshot, an event and the current time, and returns the next snapshot and the effects the caller must carry out. It does no I/O, reads no clock and starts no goroutines. Because of that, one chart can run under any driver, be stored and resumed anywhere, and be checked exhaustively before it ships.

```sh
go get github.com/speakeasy-api/sc
```

`sc` needs Go 1.27 or later.

## A first chart

```go
type State string

const (
	Closed State = "closed"
	Open   State = "open"
	Locked State = "locked"
)

type (
	OpenDoor   struct{}
	CloseDoor  struct{}
	LockDoor   struct{}
	UnlockDoor struct{}
)

type Door struct {
	Opens int `json:"opens"`
}

m, err := sc.New("door", func(b *sc.Builder[State, Door]) {
	b.Initial(Closed)

	closed := b.State(Closed)
	closed.On[OpenDoor]().
		Assign("countOpen", func(d Door, _ OpenDoor) Door {
			d.Opens++
			return d
		}).
		To(Open)
	closed.On[LockDoor]().To(Locked)

	open := b.State(Open)
	open.On[CloseDoor]().To(Closed)
	open.After(30 * time.Second).To(Closed)

	b.State(Locked).On[UnlockDoor]().To(Closed)
})
```

The chart's states are values of a type you choose (`S`). Events are ordinary Go values, dispatched by type. The context (`C`) is the chart's data: assignments return a new one rather than changing it in place.

Drive the pure core yourself:

```go
now := time.Now()
snap, effects := m.Start(Door{}, now)
snap, effects = m.Transition(snap, OpenDoor{}, now)
// snap.Active() == [open]; carry out effects
```

Or let a driver carry out the effects:

```go
inst, err := m.Instance(ctx, Door{})
defer inst.Close()

step, err := inst.Send(ctx, OpenDoor{})
// step.Accepted() == true
```

The same example runs as [`example_test.go`](example_test.go).

## What it covers

- **States**: nested and parallel states, shallow and deep history, final states with typed outputs, eventless transitions, tags, and guards on other active states.
- **Time**: fixed, computed and deadline timers, delayed events, and time budgets across several steps. Every one is driven by the time passed in, never by a clock.
- **Services**: services run while their state is active and report back as events. They can take the event that entered their state, have a typed inbox, stream results, retry with backoff, checkpoint progress across restarts, and hold resources such as connections or shared permits. Implementations can be bound later, so a chart can be declared and checked without its dependencies.
- **Composition**: child machines, keyed spawning of services and children, and `sc.Group` for many instances keyed by ID.
- **Typed handles**: chart actions, delayed events, resources, child machines and mailboxes are all handles, so most mismatches are compile errors and the rest fail when the chart is built.

## Drivers

| Driver                 | Use it for                                                                                                  |
| ---------------------- | ----------------------------------------------------------------------------------------------------------- |
| `Machine.Transition`   | Full control: you store the snapshot and carry out the effects                                              |
| `sc.Instance`          | Poll loops and request handlers. `Send` returns once the event and its actions are done                     |
| `sc.Actor`             | An instance with a goroutine and mailbox of its own                                                         |
| [`pgdriver`](pgdriver) | Instances stored in PostgreSQL, with sends inside the caller's transaction and effects written to an outbox |

[`sco11y`](sco11y) reports steps and effects to OpenTelemetry through `sc.Hooks`.

## Checking a chart

`Machine.Explore` walks every state a chart can reach under the events and service outcomes you list. It returns the shortest trace to anything that breaks an invariant, gets stuck, or can no longer finish. `Machine.Definition` exports a chart in XState's machine configuration format, so it can be viewed in XState's tools.

For rules that span the chart, its driver and the outside world, model the design in TLA+ and check it with TLC first. [`examples/payment`](examples/payment) shows the whole approach on a card payment. It has a TLA+ model, the chart built from it, and tests that replay every trace TLC found.

The behaviour is checked against the SCION SCXML conformance fixtures, a few of which come from the W3C SCXML test suite ([`internal/scxmltest`](internal/scxmltest)), and against about 860 of XState's own test cases, ported in the `xstate_*_test.go` files. Where `sc` deliberately behaves differently, the test says so with a `DIVERGES from XState` note.

## Agent skill

[`.agents/skills/modeling-statecharts`](.agents/skills/modeling-statecharts/SKILL.md) is an agent skill for designing workflows with `sc`. It covers agreeing the rules, checking them in TLA+, building the chart and checking it with `Explore`. Copy it into your own repository's skills directory to use it there.

## Versioning

`sc` follows semantic versioning but stays on `0.x.y` and will never have a 1.0 release. Breaking changes bump the minor version, and everything else bumps the patch version. Pin a minor version if you need the API to stay still.

Release notes are on the [GitHub releases](https://github.com/speakeasy-api/sc/releases) page.

## Development

Tools are managed by [mise](https://mise.jdx.dev). Run `mise install`, then:

| Task             | What it does                                                                                                                                     |
| ---------------- | ------------------------------------------------------------------------------------------------------------------------------------------------ |
| `mise run build` | Builds and vets every package                                                                                                                    |
| `mise run test`  | Runs the tests with the race detector. The `pgdriver` tests start Postgres in a container, so Docker must be running                             |
| `mise run lint`  | Runs golangci-lint and audits the GitHub workflows with zizmor                                                                                   |
| `mise run tlc`   | Runs the TLC model checker; for example `mise run tlc -metadir "$(mktemp -d)" -config examples/payment/Payment.cfg examples/payment/Payment.tla` |

To release, run the **Release** workflow on `main` from the Actions tab. Pick `patch`, or `minor` for a breaking change, or type an exact version. The workflow tags the version, and [communique](https://github.com/jdx/communique) then writes the GitHub release notes from the commits and pull requests in the release. Its settings are in [`communique.toml`](communique.toml). Run `mise run release:next patch` to see the version a release would get.

## License

[MIT](LICENSE). Some test files come from XState and the SCION and W3C SCXML test suites, under their own licenses. See [`THIRD_PARTY_NOTICES.md`](THIRD_PARTY_NOTICES.md).
