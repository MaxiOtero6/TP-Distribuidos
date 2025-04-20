package model

type WorkerClusterConfig struct {
	FilterCount   int
	OverviewCount int
	MapCount      int
	JoinCount     int
	ReduceCount   int
	TopCount      int
}

func (w *WorkerClusterConfig) TotalWorkers() int {
	return w.FilterCount +
		w.OverviewCount +
		w.MapCount +
		w.JoinCount +
		w.ReduceCount +
		w.TopCount
}
