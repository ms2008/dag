package dag_test

import (
	"errors"
	"fmt"

	"github.com/ms2008/dag"
)

func ExampleRunner() {
	var r dag.Runner

	r.AddVertex("one", func() error {
		fmt.Println("one and two will run in parallel before three")
		return nil
	})
	r.AddVertex("two", func() error {
		fmt.Println("one and two will run in parallel before three")
		return nil
	})
	r.AddVertex("three", func() error {
		fmt.Println("three will run before four")
		return errors.New("three is broken")
	})
	r.AddVertex("four", func() error {
		fmt.Println("four will never run")
		return nil
	})

	r.AddEdge("one", "three")
	r.AddEdge("two", "three")

	r.AddEdge("three", "four")

	_, err := r.Run()

	fmt.Printf("the runner terminated with: %v\n", err)
	// Output:
	// one and two will run in parallel before three
	// one and two will run in parallel before three
	// three will run before four
	// the runner terminated with: three: three is broken
	// four: canceled due to dependency execute failed
}
