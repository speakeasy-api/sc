// Package sc implements statecharts. A statechart is a hierarchical state
// machine that can have parallel regions, history, timers and invoked
// services.
//
// You declare a chart one time with [New]. After that, you cannot change the
// chart. The core of the chart is pure. [Machine.Transition] takes a
// [Snapshot], an event and the current time. It returns the next snapshot and
// the [Effect] values that the caller must do. The core does no I/O, reads no
// clock and starts no goroutine. As a result, any driver can run the same
// chart, and [Machine.Explore] can check it.
//
// # States and events
//
// A value of a comparable type S identifies each state. The caller chooses S.
// Usually, S is a string enum. Events are usual Go values. Because the chart
// dispatches each event on its dynamic type, each event has its own struct
// type. The context C is the extended state of the chart. An
// assignment returns a new C and does not change the old C. But maps and
// slices in C continue to alias. Copy them before you change them.
//
// A transition can have more than one guard. All of its guards must hold. A
// transition can also depend on other active states, with [Transition.WhenIn]
// and [Transition.WhenNotIn].
//
// The chart itself is a state like all other states. You get it with
// [Builder.Chart]. For all of the life of the instance, the chart can:
//
//   - run entry and exit actions
//   - invoke services
//   - have timers and a budget
//   - take transitions
//
// The transitions of the chart apply in each state that does not handle the
// event first. [Builder.Regions] makes the top-level states into regions that
// are all active at the same time. A chart does not have to declare states.
// Its own transitions, services and timers can be all of the chart.
// [Transition.ToChart] goes back to the point where the chart starts.
//
// When the chart enters or exits a state, the state takes a list of steps.
// These are the same steps that a transition takes. You declare them with
// [StateBuilder.Entry] and [StateBuilder.Exit]. The state takes them in the
// order of declaration. The steps are assignments, actions, raises, sends,
// emits and spawns.
//
// A final state reports an output with [FinalBuilder.Output]. The handle that
// Output returns reads the output from a snapshot with [Ending.Reached]. The
// [StateBuilder.OnDoneWith] transitions of the parent state also receive the
// output. A parallel state reports the outputs of its regions, by region
// name.
//
// A chart refers to some items by identity, not by name. For each of these
// items, the chart uses the handle that the declaration returned:
//
//   - chart actions
//   - delayed events
//   - spawned services
//   - resources
//   - child machines
//   - the services that the chart sends messages to
//
// A handle from a different chart causes a declaration error.
//
// If an event changes nothing, the chart ignores it, and the snapshot does
// not change. [StateBuilder.OnAny] catches the events that the other
// transitions of a state do not take. [StateBuilder.Tag] puts a label on
// states. As a result, a caller can use [Snapshot.HasTag] to ask a snapshot
// what kind of state it is in, and does not have to name each state.
//
// # Time
//
// The driver gives the current time to the core. The core never reads a
// clock. [Transition.WhenAt] and [Transition.AssignAt] see the time. A state
// can have three kinds of timer:
//
//   - [StateBuilder.After] waits a fixed duration.
//   - [StateBuilder.Delay] waits a duration that it calculates from the
//     context.
//   - [StateBuilder.Deadline] waits until a time that it calculates from the
//     context.
//
// Transition fires the timers that are due before it processes the event.
// The earliest timer fires first. Timers that are due at the same time fire
// in the order that they started. As a result, their order never depends on
// a driver. [Machine.Advance] fires the due timers without an event. A read
// uses it to see an expiry that nothing has processed.
//
// [StateBuilder.Within] gives the services in a state one shared deadline.
// [Transition.RaiseAfter] sends a [Builder.Delayed] event to the chart after
// a wait, in the state that the chart is in at that time.
// [Transition.CancelRaise] removes that event. [Transition.CancelAfter]
// cancels the delayed transitions of a state while the state stays active.
//
// # Actions and services
//
// Actions are side effects. The core returns them and does not run them. You
// declare the actions of a state in the state. You declare a chart action one
// time with [Builder.Action]. Any transition or state can then run it by
// name, and the action gets the [Move] that it is part of.
//
// Services run while their state is active. They send their results as
// [Done] and [Failed] events. You can give the implementations later with
// [Machine.Bind]. As a result, you can declare, export and explore a chart
// without the dependencies that its services need.
//
// A service function gets a context and a [Run]. The context is for
// cancellation, deadlines and tracing. The Run holds the data that the driver
// keeps for the service:
//
//   - its ID
//   - the progress that it recorded during an earlier run, with
//     [Run.Checkpoint]
//   - the resources of the states around it, with [Resource.Held]
//
// The service reads no data about the run from the context, and no value has
// the type any. Declare a service with [StateBuilder.InvokeOn] if it needs
// the event that caused the chart to enter its state. InvokeOn gives the
// service the event as its own type. Declare a service with
// [StateBuilder.Receive] if it gets instructions while it runs. Receive gives
// the service a typed inbox. [Invocation.Retry] runs a failed service again
// after waits that become longer. As a result, the chart sees only the final
// failure.
//
// # Composing machines
//
// [StateBuilder.InvokeMachine] runs a different machine as a child while a
// state is active. The child starts from the context of the parent. The last
// snapshot of the child becomes the result of the service. The parent sends
// events to the child with [Transition.SendTo]. The child sends events back
// with [Transition.Emit], and the parent handles them like all other events.
//
// A send goes to the handle that the declaration of the receiving service
// returned. As a result, the compiler checks the type of the message. The
// entry steps of a state can also send. Thus a state can send a first message
// to the service that it invokes. If a step stops a service, the service
// still gets the messages that the step sent to it.
//
// A parent reads the current state of its child with [Child.Snapshot]. It
// uses [Transition.WhenChildIn] to guard on that state. The snapshot of the
// parent contains the snapshot of the child. As a result, when you restore a
// parent, its child continues from the same state. Any service can do the
// same for its own progress with [Run.Checkpoint] and [Run.Resumed]. [Run.ID]
// gives a service an idempotency key that stays the same when the service
// runs again.
//
// Emitted events go through [Listen] to the code that runs the machine. There
// is one listener for each event type, and Listen gets the type from the
// listener. If a listener is for a type that the chart never emits, the build
// of the driver fails. The Send method of a driver refuses an event if no
// transition of the chart names its type, and returns [ErrUnknownEvent]. This
// catches a pointer that you send where the chart handles the value type.
//
// You can declare a service or child machine with [Invocation.Spawned] or
// with [StateBuilder.SpawnMachine]. Then it can run many times at the same
// time. [Transition.Spawn] starts each run with a key: for example, one run
// for each upload, each host or each job. [Group] does the same for full
// instances from outside a chart. Explore examines each machine separately.
// In the exploration of the parent, the child is a service that has
// outcomes.
//
// A state can hold a live resource, for example a connection, for all of the
// time that the state is active:
//
//   - [StateBuilder.Holds] acquires the resource on entry and releases it on
//     exit.
//   - The driver keeps the value out of the snapshot.
//   - The services in the state read the value with [Resource.Held]. Actions
//     read it with [Resource.From].
//
// [StateBuilder.HoldsPermit] is a resource that takes one unit of a shared
// budget, and does not wait. This sets a limit on the number of instances
// that are in a state at the same time. [Transition.Release] releases a
// resource early, while its state stays active.
//
// Each guard, assignment, action and service has a name. Names identify
// actions in persisted effects. Names also label the chart in
// [Machine.Definition], which exports the chart in the XState machine
// configuration format.
//
// # Drivers
//
// [Instance] runs an instance on the goroutines of its callers. Send returns
// after the event and its actions are complete. It also tells if the chart
// accepted the event. This is good for poll loops and request handlers.
// [Actor] runs an instance on its own goroutine, with a mailbox. The pgdriver
// subpackage keeps instances in PostgreSQL. If you want, it uses the
// transaction of the caller. It records effects through a sink that you can
// replace.
//
// # Parallel states
//
// The regions of a parallel state are active at the same time, but they do
// not run concurrently. One call to [Machine.Transition] processes an event
// against each region, in the order of declaration. This keeps the result
// deterministic. Services that different regions invoke do run concurrently,
// because drivers run them outside the core.
//
// Regions are independent. A transition into a region exits and enters only
// that region. The other regions keep their states, timers and services. This
// is also true for the region of the source. A region that enters itself
// again restarts only itself. This follows XState, not SCXML. In SCXML, such
// a transition exits the full parallel state.
//
// If you mark a transition between regions with [Transition.Reenter], it uses
// the SCXML rule and restarts all regions. [StrictSCXML] applies the SCXML
// rule to all transitions, for imported charts. [Transition.To] can name a
// target in each of more than one region. Thus one transition can put each of
// those regions into a given state.
//
// # Conformance
//
// Two external sources check the semantics of the core:
//
//   - The SCXML conformance fixtures of the SCION test framework that do not
//     need scripts. They run in internal/scxmltest.
//   - The tests of XState, which the xstate_*_test.go files port. They cover
//     the pure transition function, parallel states, history, final states,
//     eventless transitions, actions, guards, delayed transitions, invoked
//     services, spawns, emits, errors and time limits, the interpreter at run
//     time, and restore.
//
// In those files, a comment that starts with "DIVERGES from XState" marks
// each place where sc operates differently on purpose.
//
// # Chart checks
//
// [Machine.Explore] goes to each state that a chart can reach, on a clock of
// its own. It checks invariants in each state, as a model checker does with a
// specification. It reports:
//
//   - the shortest timed sequence of events to each rule that is broken
//   - the states that nothing moves out of
//   - the states from which the chart can never finish
//
// [Report.Paths] makes tests from the exploration: a shortest sequence of
// events to each state that Explore found. [Machine.StartIn] puts an instance
// directly into the given states. Use it for tests, and to adopt work that
// already started.
//
// # Observability
//
// Drivers call the [Hooks] for each transition and for each ignored event.
// They also call them at the start and at the end of each action run and
// each service run. The hooks get the context that came with the event. As a
// result, spans nest under the request that caused them. [JoinHooks] combines
// more than one set of hooks. The sco11y subpackage builds Hooks that report
// to OpenTelemetry.
package sc
