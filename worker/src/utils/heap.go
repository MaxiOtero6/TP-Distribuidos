package heap

import (
	"container/heap"
	"sort"
)

// Element represents an item in the heap with generics.
type Element[V any, D any] struct {
	Value V
	Data  D
}

// Ordered constraint allows types that support <, >, etc.
type Ordered interface {
	~int | ~int8 | ~int16 | ~int32 | ~int64 |
		~uint | ~uint8 | ~uint16 | ~uint32 | ~uint64 |
		~float32 | ~float64 | ~string
}

// MinHeap is a min-heap of Elements.
type MinHeap[V Ordered, D any] []*Element[V, D]

func (h MinHeap[V, D]) Len() int           { return len(h) }
func (h MinHeap[V, D]) Less(i, j int) bool { return h[i].Value < h[j].Value }
func (h MinHeap[V, D]) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap.
func (h *MinHeap[V, D]) Push(x any) {
	*h = append(*h, x.(*Element[V, D]))
}

// Pop removes and returns the smallest element from the heap.
func (h *MinHeap[V, D]) Pop() any {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// TopKHeap is a wrapper around MinHeap with a fixed size.
type TopKHeap[V Ordered, D any] struct {
	heap MinHeap[V, D]
	k    int
}

// NewTopKHeap creates a new TopKHeap with a given size.
func NewTopKHeap[V Ordered, D any](k int) *TopKHeap[V, D] {
	h := &MinHeap[V, D]{}
	heap.Init(h)
	return &TopKHeap[V, D]{
		heap: *h,
		k:    k,
	}
}

// Insert adds a new element to the heap and maintains the size.
func (h *TopKHeap[V, D]) Insert(value V, data D) {
	heap.Push(&h.heap, &Element[V, D]{Value: value, Data: data})
	if h.heap.Len() > h.k {
		heap.Pop(&h.heap)
	}
}

// GetTopK returns the elements in the heap in descending order.
func (h *TopKHeap[V, D]) GetTopK() []D {
	len := min(h.heap.Len(), h.k)

	result := make([]D, len)
	for i := range len {
		elem := heap.Pop(&h.heap).(*Element[V, D])
		result[i] = elem.Data
	}

	sort.Slice(result, func(i, j int) bool {
		return h.heap[i].Value > h.heap[j].Value
	})

	return result
}

func (h *TopKHeap[V, D]) Delete() {
	for i := range h.heap {
		h.heap[i] = nil
	}
	h.heap = nil
	h.k = 0
}

func (h *TopKHeap[V, D]) Len() int {
	return h.heap.Len()
}
