package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

// Mapper is a struct that implements the Action interface.
type Mapper struct {
	infraConfig *model.InfraConfig
}

// NewMapper creates a new Mapper instance.
// It initializes the worker count and returns a pointer to the Mapper struct.
func NewMapper(infraConfig *model.InfraConfig) *Mapper {
	return &Mapper{
		infraConfig: infraConfig,
	}
}

/*
delta1Stage partially sum up investment by country

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
func (m *Mapper) delta1Stage(data []*protocol.Delta_1_Data) (tasks Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()

	tasks = make(Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][DELTA_STAGE_2] = make(map[string]*protocol.Task)
	delta2Data := make(map[string][]*protocol.Delta_2_Data)

	dataMap := make(map[string]*protocol.Delta_2_Data)

	for _, movie := range data {
		prodCountry := movie.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_2_Data{
				Country:       prodCountry,
				PartialBudget: 0,
			}
		}

		dataMap[prodCountry].PartialBudget += movie.GetBudget()
	}

	for _, e2Data := range dataMap {
		idHash := utils.RandomHash(m.infraConfig.GetReduceCount())

		delta2Data[idHash] = append(delta2Data[idHash], e2Data)
	}

	for id, data := range delta2Data {
		tasks[REDUCE_EXCHANGE][DELTA_STAGE_2][id] = &protocol.Task{
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
func (m *Mapper) eta1Stage(data []*protocol.Eta_1_Data) (tasks Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()

	tasks = make(Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][ETA_STAGE_2] = make(map[string]*protocol.Task)
	eta2Data := make(map[string][]*protocol.Eta_2_Data)

	dataMap := make(map[string]*protocol.Eta_2_Data)

	for _, movieRating := range data {
		movieId := movieRating.GetId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_2_Data{
				Id:     movieId,
				Title:  movieRating.GetTitle(),
				Rating: 0.0,
				Count:  0,
			}
		}

		dataMap[movieId].Rating += float64(movieRating.GetRating())
		dataMap[movieId].Count += 1
	}

	for movieId, e2Data := range dataMap {
		idHash, err := utils.GetWorkerIdFromHash(m.infraConfig.GetReduceCount(), movieId)
		if err != nil {
			log.Errorf("Mapper: error getting worker id from hash %v", err)
			continue
		}

		eta2Data[idHash] = append(eta2Data[idHash], e2Data)
	}

	for id, data := range eta2Data {
		tasks[REDUCE_EXCHANGE][ETA_STAGE_2][id] = &protocol.Task{
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
func (m *Mapper) kappa1Stage(data []*protocol.Kappa_1_Data) (tasks Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()

	tasks = make(Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2] = make(map[string]*protocol.Task)
	kappa2Data := make(map[string][]*protocol.Kappa_2_Data)

	dataMap := make(map[string]*protocol.Kappa_2_Data)

	for _, actor := range data {
		actorId := actor.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_2_Data{
				ActorId:               actorId,
				ActorName:             actor.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += 1
	}

	for actorId, k2Data := range dataMap {
		idHash, err := utils.GetWorkerIdFromHash(m.infraConfig.GetReduceCount(), actorId)
		if err != nil {
			log.Errorf("Mapper: error getting worker id from hash %v", err)
			continue
		}

		kappa2Data[idHash] = append(kappa2Data[idHash], k2Data)
	}

	for id, data := range kappa2Data {
		tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2][id] = &protocol.Task{
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
func (m *Mapper) nu1Stage(data []*protocol.Nu_1_Data) (tasks Tasks) {
	REDUCE_EXCHANGE := m.infraConfig.GetReduceExchange()

	tasks = make(Tasks)
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][NU_STAGE_2] = make(map[string]*protocol.Task)
	nu2Data := make(map[string][]*protocol.Nu_2_Data)

	for _, movie := range data {
		idHash, err := utils.GetWorkerIdFromHash(m.infraConfig.GetReduceCount(), movie.GetId())
		if err != nil {
			log.Errorf("Mapper: error getting worker id from hash %v", err)
			continue
		}

		// Initialize idHash key if it doesn't exist
		if _, ok := nu2Data[idHash]; !ok {
			nu2Data[idHash] = []*protocol.Nu_2_Data{
				{
					Sentiment: false,
					Revenue:   0,
					Budget:    0,
					Count:     0,
				},
				{
					Sentiment: true,
					Revenue:   0,
					Budget:    0,
					Count:     0,
				},
			}
		}

		var sentiment_index int = 0
		if movie.GetSentiment() {
			sentiment_index = 1
		}

		nu2Data[idHash][sentiment_index].Revenue += movie.GetRevenue()
		nu2Data[idHash][sentiment_index].Budget += movie.GetBudget()
		nu2Data[idHash][sentiment_index].Count += 1
	}

	for id, data := range nu2Data {
		tasks[REDUCE_EXCHANGE][NU_STAGE_2][id] = &protocol.Task{
			Stage: &protocol.Task_Nu_2{
				Nu_2: &protocol.Nu_2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (m *Mapper) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Delta_1:
		data := v.Delta_1.GetData()
		return m.delta1Stage(data), nil

	case *protocol.Task_Eta_1:
		data := v.Eta_1.GetData()
		return m.eta1Stage(data), nil

	case *protocol.Task_Kappa_1:
		data := v.Kappa_1.GetData()
		return m.kappa1Stage(data), nil

	case *protocol.Task_Nu_1:
		data := v.Nu_1.GetData()
		return m.nu1Stage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
