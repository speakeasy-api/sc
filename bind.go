package sc

import (
	"context"
	"errors"
	"fmt"
	"iter"
	"maps"
	"reflect"
	"slices"
)

// ErrNotBound is the error for a service or chart action that runs without
// an implementation. This applies to a service that the chart declares with
// [StateBuilder.Requires] and to a chart action that the chart declares with
// [Builder.RequiresAction]. Drivers deliver this error to the chart as the
// failure of the service.
var ErrNotBound = errors.New("service or action has no implementation")

// boundService is an implementation given to Bind.
type boundService[C any] struct {
	doneType reflect.Type
	run      func(ctx context.Context, c C, id string) iter.Seq[any]
}

// Binder gives implementations to the services and chart actions of a
// chart. You get a Binder in the function that you give to [Machine.Bind].
type Binder[S comparable, C any] struct {
	m        *Machine[S, C]
	services map[string]boundService[C]
	releases map[string]func(context.Context, any, Leave[S])
	actions  map[string]chartAction[S, C]
	errs     []error
}

// Service binds the service with the name name to fn. Each state that
// declares a service with that name runs fn. This applies to services that
// the chart declares with [StateBuilder.Requires] or [StateBuilder.Invoke].
// For a service that the chart declares with Invoke, fn replaces the
// implementation of the declaration. A test uses this to replace a service
// with a fake. R must be the result type that the chart declares.
func (b *Binder[S, C]) Service[R any](name string, fn func(ctx context.Context, run *Run, c C) (R, error)) {
	if fn == nil {
		b.errs = append(b.errs, fmt.Errorf("service %q is bound to a nil function", name))
		return
	}

	b.bind(name, reflect.TypeFor[Done[R]](), adaptService(fn))
}

// Stream binds the stream service with the name name to fn. It does for a
// stream service what Service does for a service that returns one result.
func (b *Binder[S, C]) Stream(name string, fn func(ctx context.Context, run *Run, c C) iter.Seq2[any, error]) {
	if fn == nil {
		b.errs = append(b.errs, fmt.Errorf("stream service %q is bound to a nil function", name))
		return
	}

	b.bind(name, reflect.TypeFor[Done[struct{}]](), adaptStream(fn))
}

// Action binds the chart action with the name name to fn. The function fn
// replaces the old implementation of the action, if the action has one.
func (b *Binder[S, C]) Action(name string, fn func(ctx context.Context, c C, mv Move[S])) {
	if fn == nil {
		b.errs = append(b.errs, fmt.Errorf("chart action %q is bound to a nil function", name))
		return
	}

	if _, ok := b.m.actions[name]; !ok {
		b.errs = append(b.errs, fmt.Errorf("chart action %q is not declared", name))
		return
	}

	b.actions[name] = fn
}

func (b *Binder[S, C]) bind(name string, doneType reflect.Type, run func(context.Context, C, string) iter.Seq[any]) {
	b.bindKind("service", false, name, doneType, run)
}

// Resource binds the resource with the name name to the functions acquire
// and release. These functions replace the old functions of the resource,
// if the resource has them. R must be the type that the chart declares.
func (b *Binder[S, C]) Resource[R any](name string, acquire func(ctx context.Context, run *Run, c C) (R, error), release func(ctx context.Context, value R, why Leave[S])) {
	if acquire == nil || release == nil {
		b.errs = append(b.errs, fmt.Errorf("resource %q is bound to a nil function", name))
		return
	}

	if b.bindKind("resource", true, name, reflect.TypeFor[Acquired[R]](), adaptAcquire(acquire)) {
		b.releases[name] = adaptRelease(release)
	}
}

// bindKind binds a service or a resource, and reports whether one of that
// kind is declared under name.
func (b *Binder[S, C]) bindKind(kind string, resource bool, name string, doneType reflect.Type, run func(context.Context, C, string) iter.Seq[any]) bool {
	found := false
	for _, n := range b.m.all() {
		for _, inv := range n.invocations {
			if inv.name != name || (inv.resource != nil) != resource {
				continue
			}

			found = true
			if inv.doneType != doneType {
				b.errs = append(b.errs, fmt.Errorf("%s %q in state %q reports %s, not %s", kind, name, n.name, inv.doneType, doneType))
			}
		}
	}

	if !found {
		b.errs = append(b.errs, fmt.Errorf("%s %q is not declared", kind, name))
		return false
	}

	b.services[name] = boundService[C]{doneType: doneType, run: run}

	return true
}

// Bind returns a copy of the machine with the implementations that def
// gives. The copy and the machine use the same chart. Thus the snapshots and
// effects of one also work with the other.
//
// You can bind the same machine in many different ways at a low cost. For
// example, you can bind it one time for each tenant with the credentials of
// that tenant, or one time with fakes in a test.
//
// Bind returns all the binding errors together. A binding error is one of
// these:
//   - A name that the chart does not declare.
//   - A nil implementation.
//   - A result type that is not the same as the type in the declaration.
func (m *Machine[S, C]) Bind(def func(b *Binder[S, C])) (*Machine[S, C], error) {
	b := &Binder[S, C]{
		m:        m,
		services: map[string]boundService[C]{},
		releases: map[string]func(context.Context, any, Leave[S]){},
		actions:  map[string]chartAction[S, C]{},
		errs:     nil,
	}
	def(b)

	if len(b.errs) > 0 {
		return nil, fmt.Errorf("bind statechart %q: %w", m.id, errors.Join(b.errs...))
	}

	bound := *m
	bound.bound = make(map[string]boundService[C], len(m.bound)+len(b.services))
	maps.Copy(bound.bound, m.bound)
	maps.Copy(bound.bound, b.services)

	bound.boundReleases = make(map[string]func(context.Context, any, Leave[S]), len(m.boundReleases)+len(b.releases))
	maps.Copy(bound.boundReleases, m.boundReleases)
	maps.Copy(bound.boundReleases, b.releases)

	bound.actions = make(map[string]chartAction[S, C], len(m.actions))
	maps.Copy(bound.actions, m.actions)
	maps.Copy(bound.actions, b.actions)

	return &bound, nil
}

// Unbound lists the services, resources and chart actions that have no
// implementation, in the sequence of their names. A driver can use a machine
// that has unbound entries. But each unbound entry fails when it runs. If
// you examine Unbound when your program starts, you find these entries
// before they fail.
func (m *Machine[S, C]) Unbound() []string {
	var missing []string
	for _, n := range m.all() {
		for _, inv := range n.invocations {
			kind := "service "
			if inv.resource != nil {
				kind = "resource "
			}

			unbound := m.serviceRunner(inv) == nil || (inv.resource != nil && m.resourceRelease(inv) == nil)
			if unbound && !slices.Contains(missing, kind+inv.name) {
				missing = append(missing, kind+inv.name)
			}
		}
	}

	for name, fn := range m.actions {
		if fn == nil {
			missing = append(missing, "action "+name)
		}
	}

	slices.Sort(missing)

	return missing
}

// serviceRunner returns the implementation a service runs with: a bound one
// if Bind gave one, otherwise the one it was declared with.
func (m *Machine[S, C]) serviceRunner(inv *invocation[S, C]) func(context.Context, C, string) iter.Seq[any] {
	if b, ok := m.bound[inv.name]; ok {
		return b.run
	}

	return inv.run
}

// resourceRelease returns the release function of a resource: a bound one
// if Bind gave one, otherwise the one it was declared with.
func (m *Machine[S, C]) resourceRelease(inv *invocation[S, C]) func(context.Context, any, Leave[S]) {
	if release, ok := m.boundReleases[inv.name]; ok {
		return release
	}

	return inv.resource.release
}

// Resources lists the resources that the chart declares, in document order.
// Each item has the form "state.name". A driver that cannot hold resources
// uses this list to reject the chart.
func (m *Machine[S, C]) Resources() []string {
	var out []string
	for _, n := range m.all() {
		for _, inv := range n.invocations {
			if inv.resource != nil {
				out = append(out, n.name+"."+inv.name)
			}
		}
	}

	return out
}
