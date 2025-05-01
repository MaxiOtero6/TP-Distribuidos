package model

type WorkerClusterConfig struct {
	FilterCount   int
	OverviewCount int
	MapCount      int
	JoinCount     int
	ReduceCount   int
	MergeCount    int
	TopCount      int
}

func (w *WorkerClusterConfig) TotalWorkers() int {
	return w.FilterCount +
		w.OverviewCount +
		w.MapCount +
		w.JoinCount +
		w.ReduceCount +
		w.MergeCount +
		w.TopCount
}

type RabbitConfig struct {
	FilterExchange   string
	OverviewExchange string
	MapExchange      string
	JoinExchange     string
	ReduceExchange   string
	MergeExchange    string
	TopExchange      string
	ResultExchange   string
	BroadcastID      string
}

type InfraConfig struct {
	nodeID        string
	workers       *WorkerClusterConfig
	rabbit        *RabbitConfig
	volumeBaseDir string
}

func NewInfraConfig(idNode string, workerConfig *WorkerClusterConfig, rabbitConfig *RabbitConfig, volumeBaseDir string) *InfraConfig {
	return &InfraConfig{
		nodeID:        idNode,
		workers:       workerConfig,
		rabbit:        rabbitConfig,
		volumeBaseDir: volumeBaseDir,
	}
}

func (i *InfraConfig) GetNodeId() string {
	return i.nodeID
}

func (i *InfraConfig) GetWorkers() *WorkerClusterConfig {
	return i.workers
}

func (i *InfraConfig) GetRabbit() *RabbitConfig {
	return i.rabbit
}

func (i *InfraConfig) GetFilterCount() int {
	return i.workers.FilterCount
}

func (i *InfraConfig) GetOverviewCount() int {
	return i.workers.OverviewCount
}

func (i *InfraConfig) GetMapCount() int {
	return i.workers.MapCount
}

func (i *InfraConfig) GetJoinCount() int {
	return i.workers.JoinCount
}

func (i *InfraConfig) GetReduceCount() int {
	return i.workers.ReduceCount
}

func (i *InfraConfig) GetMergeCount() int {
	return i.workers.MergeCount
}

func (i *InfraConfig) GetTopCount() int {
	return i.workers.TopCount
}

func (i *InfraConfig) GetTotalWorkers() int {
	return i.workers.TotalWorkers()
}

func (i *InfraConfig) GetFilterExchange() string {
	return i.rabbit.FilterExchange
}

func (i *InfraConfig) GetOverviewExchange() string {
	return i.rabbit.OverviewExchange
}

func (i *InfraConfig) GetMapExchange() string {
	return i.rabbit.MapExchange
}

func (i *InfraConfig) GetJoinExchange() string {
	return i.rabbit.JoinExchange
}

func (i *InfraConfig) GetReduceExchange() string {
	return i.rabbit.ReduceExchange
}

func (i *InfraConfig) GetMergeExchange() string {
	return i.rabbit.MergeExchange
}

func (i *InfraConfig) GetTopExchange() string {
	return i.rabbit.TopExchange
}

func (i *InfraConfig) GetResultExchange() string {
	return i.rabbit.ResultExchange
}

func (i *InfraConfig) GetBroadcastID() string {
	return i.rabbit.BroadcastID
}

func (i *InfraConfig) GetDirectory() string {
	return i.volumeBaseDir
}
