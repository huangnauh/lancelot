package utils

import "bytes"

type KV struct {
	Idx   int
	Key   []byte
	Value []byte
}

type BytesHeap []KV

func NewBytesHeap() *BytesHeap {
	return &BytesHeap{}
}

func (h BytesHeap) Len() int {
	return len(h)
}

func (h BytesHeap) Less(i, j int) bool {
	return bytes.Compare(h[i].Key, h[j].Key) < 0
}

func (h BytesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *BytesHeap) Push(x interface{}) {
	*h = append(*h, x.(KV))
}

func (h *BytesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
