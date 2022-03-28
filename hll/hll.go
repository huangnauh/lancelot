// Package hll contains a HyperLogLog++ with a LogLog-Beta bias correction implementation that is adapted (mostly
// copied) from an implementation provided by Clark DuVall
// github.com/clarkduvall/hyperloglog.
//
// The differences are that the implementation in this package:
//
//   * uses an AMD64 optimised xxhash algorithm instead of murmur;
//   * uses some AMD64 optimisations for things like clz;
//   * works with []byte rather than a Hash64 interface, to reduce allocations;
//   * implements encoding.BinaryMarshaler and encoding.BinaryUnmarshaler
//
// Based on some rough benchmarking, this implementation of HyperLogLog++ is
// around twice as fast as the github.com/clarkduvall/hyperloglog implementation.
package hll

import (
	"errors"
	"fmt"
	"math"
	"math/bits"

	"github.com/cespare/xxhash"
)

const (
	// Current version of HLL implementation.
	version = uint8(2)
	// DefaultPrecision is the default precision.
	DefaultPrecision = 16
)

func beta(ez float64) float64 {
	zl := math.Log(ez + 1)
	return -0.37331876643753059*ez +
		-1.41704077448122989*zl +
		0.40729184796612533*math.Pow(zl, 2) +
		1.56152033906584164*math.Pow(zl, 3) +
		-0.99242233534286128*math.Pow(zl, 4) +
		0.26064681399483092*math.Pow(zl, 5) +
		-0.03053811369682807*math.Pow(zl, 6) +
		0.00155770210179105*math.Pow(zl, 7)
}

// Plus implements the Hyperloglog++ algorithm, described in the following
// paper: http://static.googleusercontent.com/media/research.google.com/en//pubs/archive/40671.pdf
//
// The HyperLogLog++ algorithm provides cardinality estimations.
type Plus struct {
	// hash function used to hash values to add to the sketch.
	hash  func([]byte) uint64
	p     uint8   // precision.
	m     uint32  // Number of substream used for stochastic averaging of stream.
	alpha float64 // alpha is used for bias correction.
	dense Dense   // The dense representation of the HLL.
}

// NewPlus returns a new Plus with precision p. p must be between 4 and 18.
func NewPlus(p uint8, dense Dense) (*Plus, error) {
	if p > 18 || p < 4 {
		return nil, errors.New("precision must be between 4 and 18")
	}

	hll := &Plus{
		hash:  xxhash.Sum64,
		p:     p,
		m:     1 << p,
		dense: dense,
	}

	if dense == nil {
		hll.dense = NewDefaultDense(hll.m)
	}

	// Determine alpha.
	switch hll.m {
	case 16:
		hll.alpha = 0.673
	case 32:
		hll.alpha = 0.697
	case 64:
		hll.alpha = 0.709
	default:
		hll.alpha = 0.7213 / (1 + 1.079/float64(hll.m))
	}

	return hll, nil
}

// Add adds a new value to the HLL.
func (h *Plus) Add(v []byte) error {
	x := h.hash(v)
	i := bextr(x, 64-h.p, h.p) // {x63,...,x64-p}
	w := x<<h.p | 1<<(h.p-1)   // {x63-p,...,x0}

	rho := uint8(bits.LeadingZeros64(w)) + 1
	origin, err := h.dense.Get(i)
	if err != nil {
		return err
	}
	if rho > origin {
		h.dense.Set(i, rho)
	}
	return nil
}

// Count returns a cardinality estimate.
func (h *Plus) Count() (uint64, error) {
	if h == nil {
		return 0, nil // Nothing to do.
	}
	sum := 0.0
	m := float64(h.m)
	var count float64
	l, err := h.dense.List()
	if err != nil {
		return 0, err
	}
	for _, val := range l {
		sum += 1.0 / float64(uint32(1)<<val)
		if val == 0 {
			count++
		}
	}
	// Use LogLog-Beta bias estimation
	return uint64((h.alpha * m * (m - count) / (beta(count) + sum)) + 0.5), nil
}

// Merge takes another HyperLogLogPlus and combines it with HyperLogLogPlus h.
// If HyperLogLogPlus h is using the sparse representation, it will be converted
// to the normal representation.
func (h *Plus) Merge(s Sketch) error {
	if s == nil {
		// Nothing to do
		return nil
	}

	other, ok := s.(*Plus)
	if !ok {
		return fmt.Errorf("wrong type for merging: %T", other)
	}

	if h.p != other.p {
		return errors.New("precisions must be equal")
	}

	otherL, err := other.dense.List()
	if err != nil {
		return err
	}

	hL, err := h.dense.List()
	if err != nil {
		return err
	}

	for i, v := range otherL {
		if v > hL[i] {
			err = h.dense.Set(uint64(i), v)
			if err != nil {
				return err
			}
		}
	}
	return nil
}

// bextr performs a bitfield extract on v. start should be the LSB of the field
// you wish to extract, and length the number of bits to extract.
//
// For example: start=0 and length=4 for the following 64-bit word would result
// in 1111 being returned.
//
// <snip 56 bits>00011110
// returns 1110
func bextr(v uint64, start, length uint8) uint64 {
	return (v >> start) & ((1 << length) - 1)
}
