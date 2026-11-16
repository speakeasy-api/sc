package sc

import (
	"strconv"
	"strings"
)

// Definition describes a chart in the shape of an XState machine
// configuration. If you marshal it to JSON, tools that read that format can
// load it. The JSON refers to guards, actions and services by name.
//
// JSON objects have no order. Thus, the export does not keep the declaration
// order of states. Region priority follows the declaration order, so the
// export does not include region priority.
type Definition struct {
	// ID is the machine ID on the root. On all other states, ID is the
	// state name. Transition targets refer to it as "#ID".
	ID string `json:"id"`

	// Type is "parallel", "final" or "history". It is empty for atomic
	// states and compound states.
	Type string `json:"type,omitempty"`

	// History is "shallow" or "deep" on a history state.
	History string `json:"history,omitempty"`

	// Target is the default target of a history state.
	Target string `json:"target,omitempty"`

	// Also lists the other default targets of a history state that
	// covers more than one region.
	Also []string `json:"alsoTargets,omitempty"`

	// Initial is the key of the initial child state.
	Initial string `json:"initial,omitempty"`

	// Tags lists the state's tags.
	Tags []string `json:"tags,omitempty"`

	// Entry lists the entry action names.
	Entry []string `json:"entry,omitempty"`

	// Exit lists the exit action names.
	Exit []string `json:"exit,omitempty"`

	// On maps event names to their transitions. The key of a
	// [StateBuilder.OnAny] transition is "*".
	On map[string][]TransitionDefinition `json:"on,omitempty"`

	// Always lists the transitions that have no event.
	Always []TransitionDefinition `json:"always,omitempty"`

	// After maps delays to their transitions. The key of a fixed delay is
	// its length in milliseconds. The key of a computed delay or deadline is
	// its name. XState uses the same key for a named delay.
	After map[string][]TransitionDefinition `json:"after,omitempty"`

	// OnDone lists the transitions that the chart takes when the state is
	// done.
	OnDone []TransitionDefinition `json:"onDone,omitempty"`

	// Invoke lists the services that run while the state is active.
	Invoke []InvokeDefinition `json:"invoke,omitempty"`

	// States maps child state names to their definitions.
	States map[string]*Definition `json:"states,omitempty"`
}

// TransitionDefinition describes one transition.
type TransitionDefinition struct {
	// Target is "#" and then the ID of the target state. It is empty for a
	// transition that has no target.
	Target string `json:"target,omitempty"`

	// Also lists the other targets of a transition that enters more than
	// one region at the same time. XState writes all the targets of a
	// transition as one array. Thus, a tool that reads the export must join
	// Target and Also.
	Also []string `json:"alsoTargets,omitempty"`

	// Guard names the conditions of the transition, joined with &&. The
	// names have these forms:
	//   - A guard has its name.
	//   - A [Transition.WhenIn] state has the form in(#state).
	//   - A [Transition.WhenNotIn] state has the form !in(#state).
	//
	// XState has no string form for this. Thus, a tool that reads the
	// export sees it as one named guard.
	Guard string `json:"guard,omitempty"`

	// Actions lists the names of the assignments, actions and raises, in
	// order.
	Actions []string `json:"actions,omitempty"`

	// Reenter is true if the transition exits its source and enters it
	// again.
	Reenter bool `json:"reenter,omitempty"`
}

// InvokeDefinition describes one invoked service.
type InvokeDefinition struct {
	// ID is the service name.
	ID string `json:"id"`

	// Src is the service name. The implementation is registered with this
	// name.
	Src string `json:"src"`

	// Retry describes the retry policy of the service, for example
	// "3 times, 30s doubling to 30m0s". It is not part of the XState
	// format.
	Retry string `json:"retry,omitempty"`

	// OnDone lists the transitions that the chart takes when the service
	// is successful.
	OnDone []TransitionDefinition `json:"onDone,omitempty"`

	// OnError lists the transitions that the chart takes when the service
	// fails.
	OnError []TransitionDefinition `json:"onError,omitempty"`
}

// Definition returns a description of the chart for export.
//
// The format cannot show a condition on one action. Thus, the export shows a
// chart action that runs with [Transition.RunIf] the same as an action that
// always runs. This is true on a transition and in an entry or exit list.
func (m *Machine[S, C]) Definition() *Definition {
	return m.define(m.root)
}

func (m *Machine[S, C]) define(n *node[S, C]) *Definition {
	def := &Definition{
		ID:      n.name,
		Type:    "",
		History: string(n.historyKind),
		Target:  "",
		Also:    nil,
		Initial: "",
		Tags:    n.tags,
		Entry:   nil,
		Exit:    nil,
		On:      nil,
		Always:  nil,
		After:   nil,
		OnDone:  nil,
		Invoke:  nil,
		States:  nil,
	}

	if n.kind != kindState {
		def.Type = string(n.kind)
	}
	if n.defaultNode != nil {
		def.Target = "#" + n.defaultNode.name
	}
	for _, also := range n.defaultNodes[min(1, len(n.defaultNodes)):] {
		def.Also = append(def.Also, "#"+also.name)
	}
	if n.initial != nil {
		def.Initial = n.initial.name
	}

	def.Entry = stepNames(n.onEntry)
	def.Exit = stepNames(n.onExit)

	invokes := map[*invocation[S, C]]int{}
	for _, inv := range n.invocations {
		invokes[inv] = len(def.Invoke)
		def.Invoke = append(def.Invoke, InvokeDefinition{ID: inv.name, Src: inv.name, Retry: describeRetry(inv.retry), OnDone: nil, OnError: nil})
	}

	for _, t := range n.transitions {
		td := TransitionDefinition{Target: "", Also: nil, Guard: "", Actions: nil, Reenter: t.reenter}
		if t.target != nil {
			td.Target = "#" + t.target.name
		}
		for _, also := range t.targets[min(1, len(t.targets)):] {
			td.Also = append(td.Also, "#"+also.name)
		}
		var conditions []string
		for _, g := range t.guards {
			conditions = append(conditions, g.name)
		}
		for _, in := range t.inNodes {
			conditions = append(conditions, "in(#"+in.name+")")
		}
		for _, out := range t.notInNodes {
			conditions = append(conditions, "!in(#"+out.name+")")
		}
		td.Guard = strings.Join(conditions, " && ")
		for _, s := range t.steps {
			td.Actions = append(td.Actions, s.name)
		}

		switch t.trigger {
		case triggerEvent:
			if def.On == nil {
				def.On = map[string][]TransitionDefinition{}
			}
			name := m.eventNames[t.eventType]
			if t.wildcard {
				name = "*"
			}
			def.On[name] = append(def.On[name], td)
		case triggerAlways:
			def.Always = append(def.Always, td)
		case triggerAfter:
			if def.After == nil {
				def.After = map[string][]TransitionDefinition{}
			}
			key := strconv.FormatInt(t.delay.Milliseconds(), 10)
			if t.delayName != "" {
				key = t.delayName
			}
			def.After[key] = append(def.After[key], td)
		case triggerStateDone:
			def.OnDone = append(def.OnDone, td)
		case triggerInvokeDone, triggerAcquired:
			inv := &def.Invoke[invokes[t.invocation]]
			inv.OnDone = append(inv.OnDone, td)
		case triggerInvokeError:
			inv := &def.Invoke[invokes[t.invocation]]
			inv.OnError = append(inv.OnError, td)
		case triggerNone:
		}
	}

	for _, c := range n.children {
		if def.States == nil {
			def.States = map[string]*Definition{}
		}
		def.States[c.name] = m.define(c)
	}

	return def
}

// describeRetry puts a retry policy in words for the export.
func describeRetry(p *Retry) string {
	if p == nil {
		return ""
	}

	times := "forever"
	if p.Max != Forever {
		times = strconv.Itoa(p.Max) + " times"
	}

	waits := "every " + p.First.String()
	if p.Cap > p.First {
		waits = p.First.String() + " doubling to " + p.Cap.String()
	}

	return times + ", " + waits
}

// stepNames lists the names of t's steps in order, nil for none.
func stepNames[S comparable, C any](t *transition[S, C]) []string {
	if t == nil {
		return nil
	}

	var names []string
	for _, s := range t.steps {
		names = append(names, s.name)
	}

	return names
}
