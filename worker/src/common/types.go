package common

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	heap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils"
)

type Tasks map[string]map[string]map[string]*protocol.Task

type NextStageData struct {
	Stage       string
	Exchange    string
	WorkerCount int
	RoutingKey  string
}

type PartialData[T any] struct {
	Data  map[string]T
	Ready bool // No va
	// Van para el otro modelo
	// TaskFragments map[uint32]*protocol.TaskIdentifier
	// OmegaReady    bool
	// RingRound     uint32
}

type MergerPartialResults struct {
	ToDeleteCount uint
	Delta3        PartialData[*protocol.Delta_3_Data]
	Eta3          PartialData[*protocol.Eta_3_Data]
	Kappa3        PartialData[*protocol.Kappa_3_Data]
	Nu3Data       PartialData[*protocol.Nu_3_Data]
}

type StageData[S any, B any] struct {
	SmallTable PartialData[S]
	BigTable   PartialData[[]B]
	Ready      bool
}

type JoinerPartialResults struct {
	ZetaData StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]
	IotaData StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]
}

type ReducerPartialResults struct {
	ToDeleteCount uint
	Delta2        map[string]*protocol.Delta_2_Data
	Eta2          map[string]*protocol.Eta_2_Data
	Kappa2        map[string]*protocol.Kappa_2_Data
	Nu2Data       map[string]*protocol.Nu_2_Data
}

type Result3 struct {
	MaxHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
	MinHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
}

type TopperPartialResults struct {
	EpsilonHeap *heap.TopKHeap[uint64, *protocol.Epsilon_Data]
	ThetaData   Result3
	LambdaHeap  *heap.TopKHeap[uint64, *protocol.Lambda_Data]
}
