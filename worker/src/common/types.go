package common

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"google.golang.org/protobuf/proto"
)

// map[Exchange][RoutingKey][]*protocol.Task
type Tasks map[string]map[string][]*protocol.Task

type NextStageData struct {
	Stage       string
	Exchange    string
	WorkerCount int
	RoutingKey  string
}

type PartialData[T proto.Message] struct {
	Data           map[string]T
	TaskFragments  map[model.TaskFragmentIdentifier]struct{}
	OmegaProcessed bool
	RingRound      uint32
}

type MergerPartialResults struct {
	// toDeleteCount uint
	Delta3 *PartialData[*protocol.Delta_3_Data]
	Eta3   *PartialData[*protocol.Eta_3_Data]
	Kappa3 *PartialData[*protocol.Kappa_3_Data]
	Nu3    *PartialData[*protocol.Nu_3_Data]
}

type ReducerPartialResults struct {
	ToDeleteCount uint
	Delta2        *PartialData[*protocol.Delta_2_Data]
	Eta2          *PartialData[*protocol.Eta_2_Data]
	Kappa2        *PartialData[*protocol.Kappa_2_Data]
	Nu2           *PartialData[*protocol.Nu_2_Data]
}

// type PartialData[T any] struct {
// 	Data  map[string]T
// 	Ready bool // No va
// 	// Van para el otro modelo
// 	TaskFragments map[uint32]*protocol.TaskIdentifier
// 	OmegaReady    bool
// 	RingRound     uint32
// }

// type MergerPartialResults struct {
// 	ToDeleteCount uint
// 	Delta3        PartialData[*protocol.Delta_3_Data]
// 	Eta3          PartialData[*protocol.Eta_3_Data]
// 	Kappa3        PartialData[*protocol.Kappa_3_Data]
// 	Nu3Data       PartialData[*protocol.Nu_3_Data]
// }

// type StageData[S any, B any] struct {
// 	SmallTable PartialData[S]
// 	BigTable   PartialData[[]B]
// 	Ready      bool
// }

// type JoinerPartialResults struct {
// 	ZetaData StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]
// 	IotaData StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]
// }

// type ReducerPartialResults struct {
// 	ToDeleteCount uint
// 	Delta2        map[string]*protocol.Delta_2_Data
// 	Eta2          map[string]*protocol.Eta_2_Data
// 	Kappa2        map[string]*protocol.Kappa_2_Data
// 	Nu2Data       map[string]*protocol.Nu_2_Data
// }

// type Result3 struct {
// 	MaxHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
// 	MinHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
// }

// type TopperPartialResults struct {
// 	EpsilonHeap *heap.TopKHeap[uint64, *protocol.Epsilon_Data]
// 	ThetaData   Result3
// 	LambdaHeap  *heap.TopKHeap[uint64, *protocol.Lambda_Data]
// }
