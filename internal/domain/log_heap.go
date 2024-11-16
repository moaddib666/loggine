package domain

type LogHeap []*LogRecord

func (h LogHeap) Len() int           { return len(h) }
func (h LogHeap) Less(i, j int) bool { return h[i].Timestamp.Before(h[j].Timestamp) }
func (h LogHeap) Swap(i, j int)      { h[i], h[j] = h[j], h[i] }

func (h *LogHeap) Push(x interface{}) {
	*h = append(*h, x.(*LogRecord))
}

func (h *LogHeap) Pop() interface{} {
	old := *h
	n := len(old)
	item := old[n-1]
	*h = old[0 : n-1]
	return item
}
