package actions

import (
	"fmt"
	"strconv"

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

	for _, d2Data := range dataMap {
		nodeId := utils.RandomHash(m.infraConfig.GetReduceCount())
		delta2Data[nodeId] = append(delta2Data[nodeId], d2Data)
	}

	for nodeId, data := range delta2Data {
		tasks[REDUCE_EXCHANGE][DELTA_STAGE_2][nodeId] = &protocol.Task{
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
		movieId := movieRating.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_2_Data{
				MovieId: movieId,
				Title:   movieRating.GetTitle(),
				Rating:  0.0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += float64(movieRating.GetRating())
		dataMap[movieId].Count += 1
	}

	for _, e2Data := range dataMap {
		nodeId := utils.RandomHash(m.infraConfig.GetReduceCount())
		eta2Data[nodeId] = append(eta2Data[nodeId], e2Data)
	}

	for nodeId, data := range eta2Data {
		tasks[REDUCE_EXCHANGE][ETA_STAGE_2][nodeId] = &protocol.Task{
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

	for _, k2Data := range dataMap {
		nodeId := utils.RandomHash(m.infraConfig.GetReduceCount())
		kappa2Data[nodeId] = append(kappa2Data[nodeId], k2Data)
	}

	for nodeId, data := range kappa2Data {
		tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2][nodeId] = &protocol.Task{
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

	dataMap := make(map[string]*protocol.Nu_2_Data)

	for _, movie := range data {
		sentiment := fmt.Sprintf("%t", movie.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_2_Data{
				Sentiment: movie.GetSentiment(),
				Ratio:     0.0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += float32(movie.GetRevenue()) / float32(movie.GetBudget())
		dataMap[sentiment].Count += 1
	}

	for _, n2Data := range dataMap {
		nodeId := utils.RandomHash(m.infraConfig.GetReduceCount())
		nu2Data[nodeId] = append(nu2Data[nodeId], n2Data)
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

func (m *Mapper) getNextNodeId(nodeId string) (string, error) {
	clientId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert clientId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (clientId+1)%m.infraConfig.GetReduceCount())
	return nextNodeId, nil
}

func (m *Mapper) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case DELTA_STAGE_1:
		return DELTA_STAGE_2, m.infraConfig.GetReduceExchange(), m.infraConfig.GetReduceCount(), nil
	case ETA_STAGE_1:
		return ETA_STAGE_2, m.infraConfig.GetReduceExchange(), m.infraConfig.GetReduceCount(), nil
	case KAPPA_STAGE_1:
		return KAPPA_STAGE_2, m.infraConfig.GetReduceExchange(), m.infraConfig.GetReduceCount(), nil
	case NU_STAGE_1:
		return NU_STAGE_2, m.infraConfig.GetReduceExchange(), m.infraConfig.GetReduceCount(), nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Mapper) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == m.infraConfig.GetNodeId() {

		nextStage, nextExchange, nextStageCount, err := m.getNextStageData(data.GetStage())
		if err != nil {
			log.Errorf("Failed to get next stage data: %s", err)
			return nil
		}

		nextStageEOF := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						ClientId:        data.GetClientId(),
						WorkerCreatorId: "",
						Stage:           nextStage,
					},
				},
			},
		}

		randomNode := utils.RandomHash(nextStageCount)

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStage][randomNode] = nextStageEOF

	} else { // if the creator is not the same as the worker, send EOF to the next node
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = m.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := m.getNextNodeId(m.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		mapExchange := m.infraConfig.GetReduceExchange()
		stage := data.GetStage()

		tasks[mapExchange] = make(map[string]map[string]*protocol.Task)
		tasks[mapExchange][stage] = make(map[string]*protocol.Task)
		tasks[mapExchange][stage][nextNode] = eofTask

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

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.omegaEOFStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
