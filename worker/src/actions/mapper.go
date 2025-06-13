package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	// "github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
)

// Mapper is a struct that implements the Action interface.
type Mapper struct {
	infraConfig    *model.InfraConfig
	itemHashFunc   func(workersCount int, itemId string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatelessEofHandler
}

// NewMapper creates a new Mapper instance.
// It initializes the worker count and returns a pointer to the Mapper struct.
func NewMapper(infraConfig *model.InfraConfig) *Mapper {
	m := &Mapper{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     nil,
	}

	eofHandler := eof.NewStatelessEofHandler(
		m.getNextStageData,
	)

	m.eofHandler = eofHandler
	return m
}

/*
This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"delta_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) delta1Stage(data []*protocol.Delta_1_Data, clientId string) (tasks common.Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()
	REDUCE_COUNT := m.infraConfig.GetReduceCount()

	tasks = make(common.Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][common.DELTA_STAGE_2] = make(map[string]*protocol.Task)
	delta2Data := make(map[string][]*protocol.Delta_2_Data)

	dataMap := make(map[string]*protocol.Delta_2_Data)

	for _, d1Data := range data {
		prodCountry := d1Data.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_2_Data{
				Country:       prodCountry,
				PartialBudget: 0,
			}
		}

		dataMap[prodCountry].PartialBudget += d1Data.GetBudget()
	}

	for _, d2Data := range dataMap {
		nodeId := m.randomHashFunc(REDUCE_COUNT)

		if _, ok := delta2Data[nodeId]; !ok {
			delta2Data[nodeId] = make([]*protocol.Delta_2_Data, 0)
		}

		delta2Data[nodeId] = append(delta2Data[nodeId], d2Data)
	}

	for nodeId, data := range delta2Data {
		tasks[REDUCE_EXCHANGE][common.DELTA_STAGE_2][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Delta_2{
				Delta_2: &protocol.Delta_2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
eta1Stage partially counts and sum up ratings from movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"eta_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) eta1Stage(data []*protocol.Eta_1_Data, clientId string) (tasks common.Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()
	REDUCE_COUNT := m.infraConfig.GetReduceCount()

	tasks = make(common.Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][common.ETA_STAGE_2] = make(map[string]*protocol.Task)
	eta2Data := make(map[string][]*protocol.Eta_2_Data)

	dataMap := make(map[string]*protocol.Eta_2_Data)

	for _, e1Data := range data {
		movieId := e1Data.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_2_Data{
				MovieId: movieId,
				Title:   e1Data.GetTitle(),
				Rating:  0.0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += float64(e1Data.GetRating())
		dataMap[movieId].Count += 1
	}

	for _, e2Data := range dataMap {
		nodeId := m.randomHashFunc(REDUCE_COUNT)

		if _, ok := eta2Data[nodeId]; !ok {
			eta2Data[nodeId] = make([]*protocol.Eta_2_Data, 0)
		}

		eta2Data[nodeId] = append(eta2Data[nodeId], e2Data)
	}

	for nodeId, data := range eta2Data {
		tasks[REDUCE_EXCHANGE][common.ETA_STAGE_2][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_2{
				Eta_2: &protocol.Eta_2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
kappa1Stage partially counts actors participations in movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"kappa_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) kappa1Stage(data []*protocol.Kappa_1_Data, clientId string) (tasks common.Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()
	REDUCE_COUNT := m.infraConfig.GetReduceCount()

	tasks = make(common.Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][common.KAPPA_STAGE_2] = make(map[string]*protocol.Task)
	kappa2Data := make(map[string][]*protocol.Kappa_2_Data)

	dataMap := make(map[string]*protocol.Kappa_2_Data)

	for _, k1Data := range data {
		actorId := k1Data.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_2_Data{
				ActorId:               actorId,
				ActorName:             k1Data.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += 1
	}

	for _, k2Data := range dataMap {
		nodeId := m.randomHashFunc(REDUCE_COUNT)

		if _, ok := kappa2Data[nodeId]; !ok {
			kappa2Data[nodeId] = make([]*protocol.Kappa_2_Data, 0)
		}

		kappa2Data[nodeId] = append(kappa2Data[nodeId], k2Data)
	}

	for nodeId, data := range kappa2Data {
		tasks[REDUCE_EXCHANGE][common.KAPPA_STAGE_2][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_2{
				Kappa_2: &protocol.Kappa_2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
nu1Stage partially counts and sum up revenue and budget from movies by sentiment.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"reduceExchange": {
			"nu_2": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Mapper) nu1Stage(data []*protocol.Nu_1_Data, clientId string) (tasks common.Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()
	REDUCE_COUNT := m.infraConfig.GetReduceCount()

	tasks = make(common.Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][common.NU_STAGE_2] = make(map[string]*protocol.Task)
	nu2Data := make(map[string][]*protocol.Nu_2_Data)

	dataMap := make(map[string]*protocol.Nu_2_Data)

	for _, nu1Data := range data {
		sentiment := fmt.Sprintf("%t", nu1Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_2_Data{
				Sentiment: nu1Data.GetSentiment(),
				Ratio:     0.0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += float32(float64(nu1Data.GetRevenue()) / float64(nu1Data.GetBudget()))
		dataMap[sentiment].Count += 1
	}

	for _, n2Data := range dataMap {
		nodeId := m.randomHashFunc(REDUCE_COUNT)

		if _, ok := nu2Data[nodeId]; !ok {
			nu2Data[nodeId] = make([]*protocol.Nu_2_Data, 0)
		}

		nu2Data[nodeId] = append(nu2Data[nodeId], n2Data)
	}

	for nodeId, data := range nu2Data {
		tasks[REDUCE_EXCHANGE][common.NU_STAGE_2][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_2{
				Nu_2: &protocol.Nu_2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (m *Mapper) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	switch stage {
	case common.DELTA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.DELTA_STAGE_2,
				Exchange:    m.infraConfig.GetReduceExchange(),
				WorkerCount: m.infraConfig.GetReduceCount(),
				RoutingKey:  m.itemHashFunc(m.infraConfig.GetReduceCount(), clientId+common.DELTA_STAGE_2),
			},
		}, nil
	case common.ETA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_2,
				Exchange:    m.infraConfig.GetReduceExchange(),
				WorkerCount: m.infraConfig.GetReduceCount(),
				RoutingKey:  m.itemHashFunc(m.infraConfig.GetReduceCount(), clientId+common.ETA_STAGE_2),
			},
		}, nil
	case common.KAPPA_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_2,
				Exchange:    m.infraConfig.GetReduceExchange(),
				WorkerCount: m.infraConfig.GetReduceCount(),
				RoutingKey:  m.itemHashFunc(m.infraConfig.GetReduceCount(), clientId+common.KAPPA_STAGE_2),
			},
		}, nil
	case common.NU_STAGE_1:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_2,
				Exchange:    m.infraConfig.GetReduceExchange(),
				WorkerCount: m.infraConfig.GetReduceCount(),
				RoutingKey:  m.itemHashFunc(m.infraConfig.GetReduceCount(), clientId+common.NU_STAGE_2),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Mapper) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	switch v := stage.(type) {
	case *protocol.Task_Delta_1:
		data := v.Delta_1.GetData()
		return m.delta1Stage(data, clientId), nil

	case *protocol.Task_Eta_1:
		data := v.Eta_1.GetData()
		return m.eta1Stage(data, clientId), nil

	case *protocol.Task_Kappa_1:
		data := v.Kappa_1.GetData()
		return m.kappa1Stage(data, clientId), nil

	case *protocol.Task_Nu_1:
		data := v.Nu_1.GetData()
		return m.nu1Stage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.eofHandler.HandleOmegaEOF(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
