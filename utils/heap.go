package utils

import "bytes"

type BytesHeap [][]byte

func NewBytesHeap() *BytesHeap {
	return &BytesHeap{}
}

func (h BytesHeap) Len() int {
	return len(h)
}

func (h BytesHeap) Less(i, j int) bool {
	return bytes.Compare(h[i], h[j]) < 0
}

func (h BytesHeap) Swap(i, j int) {
	h[i], h[j] = h[j], h[i]
}

func (h *BytesHeap) Push(x interface{}) {
	*h = append(*h, x.([]byte))
}

func (h *BytesHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}
