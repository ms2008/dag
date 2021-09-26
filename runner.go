// Package dag implements a directed acyclic graph task runner with deterministic teardown.
// it is similar to package errgroup, in that it runs multiple tasks in parallel and returns
// the first error it encounters. Users define a Runner as a set vertices (functions) and edges
// between them. During Run, the directed acyclec graph will be validated and each vertex
// will run in parallel as soon as it's dependencies have been resolved. The Runner will only
// return after all running goroutines have stopped.
package dag

import (
	"errors"
	"fmt"
	"sort"
	"time"
)

// Runner collects functions and arranges them as vertices and edges of a directed acyclic graph.
// Upon validation of the graph, functions are run in parallel topological order. The zero value
// is useful.
type Runner struct {
	fns   map[string]func() error
	graph map[string][]string
}

var errMissingVertex = errors.New("missing vertex")
var errCycleDetected = errors.New("dependency cycle detected")
var errCanceledVertex = errors.New("canceled due to dependency execute failed")

// AddVertex adds a function as a vertex in the graph. Only functions which have been added in this
// way will be executed during Run.
func (d *Runner) AddVertex(name string, fn func() error) {
	if d.fns == nil {
		d.fns = make(map[string]func() error)
	}
	d.fns[name] = fn
}

// AddEdge establishes a dependency between two vertices in the graph. Both from and to must exist
// in the graph, or Run will err. The vertex at from will execute before the vertex at to.
func (d *Runner) AddEdge(from, to string) {
	if d.graph == nil {
		d.graph = make(map[string][]string)
	}
	d.graph[from] = append(d.graph[from], to)
}

func (d *Runner) DelEdge(from, to string) {
	if d.graph == nil {
		return
	}

	edges, ok := d.graph[from]
	if !ok {
		return
	}

	// output index
	i := 0
	for _, v := range edges {
		if v == to {
			continue
		}
		// copy and increment index
		edges[i] = v
		i++
	}

	d.graph[from] = edges[:i]
	if len(d.graph[from]) == 0 {
		delete(d.graph, from)
	}
}

type result struct {
	name  string
	err   error
	stage int
}

type (
	CustomError struct {
		Group []*ErrorEntry
	}

	ErrorEntry struct {
		Name string
		Err  error
	}
)

func newCustomError(errs ...*ErrorEntry) *CustomError {
	return &CustomError{
		Group: errs,
	}
}

func newErrorEntry(name string, err error) *ErrorEntry {
	return &ErrorEntry{
		Name: name,
		Err:  err,
	}
}

func (e *CustomError) Error() string {
	var msg string
	for _, v := range e.Group {
		msg += fmt.Sprintf("%s: %s\n", v.Name, v.Err.Error())
	}

	return msg
}

func (d *Runner) detectCycles() bool {
	visited := make(map[string]bool)
	recStack := make(map[string]bool)

	for vertex := range d.graph {
		if !visited[vertex] {
			if d.detectCyclesHelper(vertex, visited, recStack) {
				return true
			}
		}
	}
	return false
}

func (d *Runner) detectCyclesHelper(vertex string, visited, recStack map[string]bool) bool {
	visited[vertex] = true
	recStack[vertex] = true

	for _, v := range d.graph[vertex] {
		// only check cycles on a vertex one time
		if !visited[v] {
			if d.detectCyclesHelper(v, visited, recStack) {
				return true
			}
			// if we've visited this vertex in this recursion stack, then we have a cycle
		} else if recStack[v] {
			return true
		}

	}
	recStack[vertex] = false
	return false
}

// allCanceled returned all calceled vertexs
func (d *Runner) allCanceled() []*ErrorEntry {
	var errGroup []*ErrorEntry
	for k := range d.fns {
		errGroup = append(errGroup, newErrorEntry(k, errCanceledVertex))
	}

	return errGroup
}

// Run will validate that all edges in the graph point to existing vertices, and that there are
// no dependency cycles. After validation, each vertex will be run, deterministically, in parallel
// topological order. If any vertex returns an error, no more vertices will be scheduled and
// Run will exit and return that error once all in-flight functions finish execution.
func (d *Runner) Run() ([]*ErrorEntry, error) {
	// sanity check
	if len(d.fns) == 0 {
		return nil, nil
	}
	// count how many deps each vertex has
	deps := make(map[string]int)
	for vertex, edges := range d.graph {
		// every vertex along every edge must have an associated fn
		if _, ok := d.fns[vertex]; !ok {
			return d.allCanceled(), errMissingVertex
		}
		for _, vertex := range edges {
			if _, ok := d.fns[vertex]; !ok {
				return d.allCanceled(), errMissingVertex
			}
			deps[vertex]++
		}
	}

	if d.detectCycles() {
		return d.allCanceled(), errCycleDetected
	}

	running := 0
	resc := make(chan result, len(d.fns))
	var errGroup []*ErrorEntry

	// start any vertex that has no deps
	for name := range d.fns {
		if deps[name] == 0 {
			running++
			start(name, 0, d.fns[name], resc)
		}
	}

	// wait for all running work to complete
	for running > 0 {
		res := <-resc
		running--

		// capture errors
		if res.err != nil {
			errGroup = append(errGroup, newErrorEntry(res.name, res.err))
		}

		// start any vertex whose deps are fully resolved
		for _, vertex := range d.graph[res.name] {
			if deps[vertex]--; deps[vertex] == 0 {
				// canceled vertex
				if res.err != nil {
					errGroup = append(errGroup, newErrorEntry(vertex, errCanceledVertex))
					continue
				}
				running++
				start(vertex, 0, d.fns[vertex], resc)
			}
		}
	}

	if errGroup != nil {
		return errGroup, newCustomError(errGroup...)
	}

	return nil, nil
}

type callback func() error

// only print the topological order stack, without running it
func (d *Runner) DryRun() [][]string {
	running := 0
	resc := make(chan result, len(d.fns))
	var topStack [][]string

	// count how many deps each vertex has
	deps := make(map[string]int)
	for _, edges := range d.graph {
		for _, vertex := range edges {
			deps[vertex]++
		}
	}

	mockFn := func(n int) callback {
		return func() error {
			// IMPORTANT: vertices must be returned by stage order
			time.Sleep(time.Duration(n) * time.Millisecond)
			return nil
		}
	}

	// start any vertex that has no deps
	for name := range d.fns {
		if deps[name] == 0 {
			running++
			start(name, 1, mockFn(0), resc)
		}
	}

	// wait for all running work to complete
	for running > 0 {
		res := <-resc
		running--

		if len(topStack) < res.stage {
			topStack = append(topStack, []string{res.name})
		} else {
			idx := res.stage - 1
			topStack[idx] = append(topStack[idx], res.name)
		}

		// start any vertex whose deps are fully resolved
		for _, vertex := range d.graph[res.name] {
			if deps[vertex]--; deps[vertex] == 0 {
				running++
				stageN := res.stage + 1
				start(vertex, stageN, mockFn(stageN), resc)
			}
		}
	}

	for _, v := range topStack {
		sort.SliceStable(v, func(i, j int) bool { return v[i] < v[j] })
	}

	return topStack
}

func start(name string, index int, fn callback, resc chan<- result) {
	go func() {
		resc <- result{
			name:  name,
			stage: index,
			err:   fn(),
		}
	}()
}
