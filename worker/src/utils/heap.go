package heap

import (
	"container/heap"
)

// Element represents an item in the heap.
type Element struct {
	Value int         // The value to compare (e.g., total investment)
	Data  interface{} // Additional data
}

// MinHeap is a min-heap of Elements.
type MinHeap []*Element

func (h MinHeap) Len() int           { return len(h) }
func (h MinHeap) Less(i, j int) bool { return h[i].Value < h[j].Value } // Min-heap: smallest value at the top
func (h MinHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

// Push adds an element to the heap.
func (h *MinHeap) Push(x interface{}) {
	*h = append(*h, x.(*Element))
}

// Pop removes and returns the smallest element from the heap.
func (h *MinHeap) Pop() interface{} {
	old := *h
	n := len(old)
	x := old[n-1]
	*h = old[0 : n-1]
	return x
}

// TopKHeap is a wrapper around MinHeap with a fixed size.
type TopKHeap struct {
	heap MinHeap
	k    int
}

// NewTopKHeap creates a new TopKHeap with a given size.
func NewTopKHeap(k int) *TopKHeap {
	h := &MinHeap{}
	heap.Init(h)
	return &TopKHeap{
		heap: *h,
		k:    k,
	}
}

// Insert adds a new element to the heap and maintains the size.
func (h *TopKHeap) Insert(value int, data interface{}) {
	heap.Push(&h.heap, &Element{Value: value, Data: data})
	if h.heap.Len() > h.k {
		heap.Pop(&h.heap) // Remove the smallest element if the heap exceeds size K
	}
}

// GetTopK returns the elements in the heap in descending order.
func (h *TopKHeap) GetTopK() []*Element {
	result := make([]*Element, len(h.heap))
	copy(result, h.heap)

	// Sort in descending order (optional, depending on your use case)
	for i := 0; i < len(result)/2; i++ {
		result[i], result[len(result)-1-i] = result[len(result)-1-i], result[i]
	}

	return result
}
