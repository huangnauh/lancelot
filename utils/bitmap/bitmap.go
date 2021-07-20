package bitmap

type Bitmap struct {
	segments []uint64
	length   int
}

func New(length int) *Bitmap {
	return &Bitmap{make([]uint64, length/64), length}
}

func (b *Bitmap) GetSegments() []uint64 {
	return b.segments
}

func (b *Bitmap) SetSegments(segments []uint64) {
	b.segments = segments
	b.length = len(segments) * 64
}

func (b *Bitmap) Add(index int) {
	if index < 0 || index >= b.length {
		return
	}
	b.segments[index/64] |= 1 << uint64(index%64)
}

func (b *Bitmap) IsSet(index int) bool {
	if index < 0 || index >= b.length {
		return false
	}
	return b.segments[index/64]&(1<<uint64(index%64)) != 0
}

func (b *Bitmap) Remove(index int) {
	if index < 0 || index >= b.length {
		return
	}
	b.segments[index/64] &^= 1 << uint64(index%64)
}

func (b *Bitmap) SetFull() {
	for i := 0; i < len(b.segments); i++ {
		b.segments[i] = 0xFFFFFFFFFFFFFFFF
	}
}

func (b *Bitmap) IsFull() bool {
	for _, v := range b.segments {
		if v != 0xFFFFFFFFFFFFFFFF {
			return false
		}
	}
	return true
}

func (b *Bitmap) SetEmpty() {
	for i := 0; i < len(b.segments); i++ {
		b.segments[i] = 0
	}
}

func (b *Bitmap) IsEmpty() bool {
	for _, v := range b.segments {
		if v != 0 {
			return false
		}
	}
	return true
}

func (b *Bitmap) Values() []int {
	values := make([]int, 0)
	for i := 0; i < b.length; i++ {
		if b.segments[i/64]&(1<<uint64(i%64)) != 0 {
			values = append(values, i)
		}
	}
	return values
}

func (b *Bitmap) EmptyValues() []int {
	values := make([]int, 0)
	for i := 0; i < b.length; i++ {
		if b.segments[i/64]&(1<<uint64(i%64)) == 0 {
			values = append(values, i)
		}
	}
	return values
}
