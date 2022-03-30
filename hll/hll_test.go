package hll

import (
	"encoding/binary"
	"fmt"
	"math"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"
)

func nopHash(buf []byte) uint64 {
	if len(buf) != 8 {
		panic(fmt.Sprintf("unexpected size buffer: %d", len(buf)))
	}
	return binary.BigEndian.Uint64(buf)
}

func toByte(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func TestPlus_Bytes(t *testing.T) {
	testCases := []struct {
		p uint8
	}{
		{4},
		{5},
		{4},
		{5},
	}

	for i, testCase := range testCases {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			h := NewTestPlus(testCase.p)
			denseList, err := h.dense.List()
			assert.NoError(t, err)

			// denseList has capacity for 2^p elements, one byte each
			expectedDenseListCapacity := int(math.Pow(2, float64(testCase.p)))
			if expectedDenseListCapacity != cap(denseList) {
				t.Errorf("denseList capacity: want %d got %d", expectedDenseListCapacity, cap(denseList))
			}
		})
	}
}

func TestPlus_Add_NoSparse(t *testing.T) {
	h := NewTestPlus(16)

	h.Add(toByte(0x00010fffffffffff))
	n, err := h.dense.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), n)

	h.Add(toByte(0x0002ffffffffffff))
	n, err = h.dense.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, uint8(1), n)

	h.Add(toByte(0x0003000000000000))
	n, err = h.dense.Get(3)
	assert.NoError(t, err)
	assert.Equal(t, uint8(49), n)

	h.Add(toByte(0x0003000000000001))
	n, err = h.dense.Get(3)
	assert.NoError(t, err)
	assert.Equal(t, uint8(49), n)

	h.Add(toByte(0xff03700000000000))
	n, err = h.dense.Get(0xff03)
	assert.NoError(t, err)
	assert.Equal(t, uint8(2), n)

	h.Add(toByte(0xff03080000000000))
	n, err = h.dense.Get(0xff03)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), n)
}

func TestPlusPrecision_NoSparse(t *testing.T) {
	h := NewTestPlus(4)

	h.Add(toByte(0x1fffffffffffffff))
	n, err := h.dense.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, uint8(1), n)

	h.Add(toByte(0xffffffffffffffff))
	n, err = h.dense.Get(0xf)
	assert.NoError(t, err)
	assert.Equal(t, uint8(1), n)

	h.Add(toByte(0x00ffffffffffffff))
	n, err = h.dense.Get(0)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), n)
}

func TestPlus_toNormal(t *testing.T) {
	h := NewTestPlus(16)
	h.Add(toByte(0x00010fffffffffff))
	c, err := h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(1), c)

	h = NewTestPlus(16)
	h.hash = nopHash
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x0002ffffffffffff))
	h.Add(toByte(0x0003000000000000))
	h.Add(toByte(0x0003000000000001))
	h.Add(toByte(0xff03700000000000))
	h.Add(toByte(0xff03080000000000))

	n, err := h.dense.Get(1)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), n)

	n, err = h.dense.Get(2)
	assert.NoError(t, err)
	assert.Equal(t, uint8(1), n)

	n, err = h.dense.Get(3)
	assert.NoError(t, err)
	assert.Equal(t, uint8(49), n)

	n, err = h.dense.Get(0xff03)
	assert.NoError(t, err)
	assert.Equal(t, uint8(5), n)
}

func TestPlusCount(t *testing.T) {
	h := NewTestPlus(16)

	c, err := h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(0), c)

	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	c, err = h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), c)

	h.Add(toByte(0x00060fffffffffff))

	c, err = h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(6), c)
}

func TestPlus_Merge_Error(t *testing.T) {
	h := NewTestPlus(16)
	h2 := NewTestPlus(10)

	err := h.Merge(h2)
	if err == nil {
		t.Error("different precision should return error")
	}
}

func TestHLL_Merge_Sparse(t *testing.T) {
	h := NewTestPlus(16)
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	h2 := NewTestPlus(16)
	h2.Merge(h)
	c, err := h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), c)

	h2.Merge(h)
	c, err = h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), c)

	h.Add(toByte(0x00060fffffffffff))
	h.Add(toByte(0x00070fffffffffff))
	h.Add(toByte(0x00080fffffffffff))
	h.Add(toByte(0x00090fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))

	c, err = h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), c)

	h2.Merge(h)
	c, err = h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), c)
}

func TestHLL_Merge_Normal(t *testing.T) {
	h := NewTestPlus(16)
	h.Add(toByte(0x00010fffffffffff))
	h.Add(toByte(0x00020fffffffffff))
	h.Add(toByte(0x00030fffffffffff))
	h.Add(toByte(0x00040fffffffffff))
	h.Add(toByte(0x00050fffffffffff))
	h.Add(toByte(0x00050fffffffffff))

	h2 := NewTestPlus(16)
	h2.Merge(h)
	c, err := h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), c)

	h2.Merge(h)
	c, err = h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(5), c)

	h.Add(toByte(0x00060fffffffffff))
	h.Add(toByte(0x00070fffffffffff))
	h.Add(toByte(0x00080fffffffffff))
	h.Add(toByte(0x00090fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	h.Add(toByte(0x000a0fffffffffff))
	c, err = h.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), c)

	h2.Merge(h)
	c, err = h2.Count()
	assert.NoError(t, err)
	assert.Equal(t, uint64(10), c)
}

func TestPlus_Error(t *testing.T) {
	_, err := NewPlus(3, nil)
	if err == nil {
		t.Error("precision 3 should return error")
	}

	_, err = NewPlus(18, nil)
	if err != nil {
		t.Error(err)
	}

	_, err = NewPlus(19, nil)
	if err == nil {
		t.Error("precision 17 should return error")
	}
}

func NewTestPlus(p uint8) *Plus {
	h, err := NewPlus(p, nil)
	if err != nil {
		panic(err)
	}
	h.hash = nopHash
	return h
}

// Generate random data to add to the sketch.
func genData(n int, src *rand.Rand) [][]byte {
	out := make([][]byte, 0, n)
	buf := make([]byte, 8)

	for i := 0; i < n; i++ {
		// generate 8 random bytes
		n, err := src.Read(buf)
		if err != nil {
			panic(err)
		} else if n != 8 {
			panic(fmt.Errorf("only %d bytes generated", n))
		}

		out = append(out, buf)
	}
	if len(out) != n {
		panic(fmt.Sprintf("wrong size slice: %d", n))
	}
	return out
}

// Memoises values to be added to a sketch during a benchmark.
var benchdata = map[int][][]byte{}

func benchmarkPlusAdd(b *testing.B, h *Plus, n int) {
	src := rand.New(rand.NewSource(9938))
	blobs, ok := benchdata[n]
	if !ok {
		// Generate it.
		benchdata[n] = genData(n, src)
		blobs = benchdata[n]
	}

	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		for j := 0; j < len(blobs); j++ {
			h.Add(blobs[j])
		}
	}
	b.StopTimer()
}

func BenchmarkPlus_Add_100(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 100)
}

func BenchmarkPlus_Add_1000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 1000)
}

func BenchmarkPlus_Add_10000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 10000)
}

func BenchmarkPlus_Add_100000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 100000)
}

func BenchmarkPlus_Add_1000000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 1000000)
}

func BenchmarkPlus_Add_10000000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 10000000)
}

func BenchmarkPlus_Add_100000000(b *testing.B) {
	h, _ := NewPlus(16, nil)
	benchmarkPlusAdd(b, h, 100000000)
}
