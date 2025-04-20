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

type RabbitConfig struct {
	FilterExchange   string
	OverviewExchange string
	MapExchange      string
	JoinExchange     string
	ReduceExchange   string
	TopExchange      string
	ResultExchange   string
	BroadcastID      string
}

type InfraConfig struct {
	Workers *WorkerClusterConfig
	Rabbit  *RabbitConfig
}
