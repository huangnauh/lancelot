package hll

// Sketch is the interface representing a sketch for estimating cardinality.
type Sketch interface {
	// Add adds a single value to the sketch.
	Add(v []byte) error

	// Count returns a cardinality estimate for the sketch.
	Count() (uint64, error)

	// Merge merges another sketch into this one.
	Merge(s Sketch) error
}
