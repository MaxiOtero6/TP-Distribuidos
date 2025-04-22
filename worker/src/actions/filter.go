package actions

import (
	"fmt"
	"strconv"

	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

// Filter is a struct that implements the Action interface.
// It filters movies based on certain criteria.
// It is used in the worker to filter movies in the pipeline.
type Filter struct {
	infraConfig *model.InfraConfig
}

func NewFilter(infraConfig *model.InfraConfig) *Filter {
	return &Filter{
		infraConfig: infraConfig,
	}
}

const ARGENTINA_COUNTRY string = "Argentina"
const SPAIN_COUNTRY string = "Spain"
const MOVIE_YEAR_2000 uint32 = 2000
const MOVIE_YEAR_2010 uint32 = 2010

/*
alphaStage filters movies based on the following criteria:
The movie must be released from the year 2000 and must be produced in Argentina.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"filterExchange": {
			"beta": {
				"0": Task,
				"1": Task
			}
		},
		"joinExchange": {
			"zeta": {
				"0": Task,
				"1": Task
			},
			"iota": {
				"0": Task,
				"1": Task
			}
		}
	}
*/
func (f *Filter) alphaStage(data []*protocol.Alpha_Data) (tasks Tasks) {
	FILTER_EXCHANGE := f.infraConfig.GetFilterExchange()
	JOIN_EXCHANGE := f.infraConfig.GetJoinExchange()
	JOIN_COUNT := f.infraConfig.GetJoinCount()
	FILTER_COUNT := f.infraConfig.GetFilterCount()

	tasks = make(Tasks)
	tasks[FILTER_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[JOIN_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[FILTER_EXCHANGE][BETA_STAGE] = make(map[string]*protocol.Task)
	tasks[JOIN_EXCHANGE][ZETA_STAGE] = make(map[string]*protocol.Task)
	tasks[JOIN_EXCHANGE][IOTA_STAGE] = make(map[string]*protocol.Task)

	betaData := make(map[string][]*protocol.Beta_Data)
	zetaData := make(map[string][]*protocol.Zeta_Data)
	iotaData := make(map[string][]*protocol.Iota_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		if movie.GetReleaseYear() < MOVIE_YEAR_2000 {
			continue
		}

		if !slices.Contains(movie.GetProdCountries(), ARGENTINA_COUNTRY) {
			continue
		}

		filterIdHash := utils.GetWorkerIdFromHash(FILTER_COUNT, movie.GetId())

		betaData[filterIdHash] = append(betaData[filterIdHash], &protocol.Beta_Data{
			Id:            movie.GetId(),
			Title:         movie.GetTitle(),
			ReleaseYear:   movie.GetReleaseYear(),
			ProdCountries: movie.GetProdCountries(),
			Genres:        movie.GetGenres(),
		})

		joinerIdHash := utils.GetWorkerIdFromHash(JOIN_COUNT, movie.GetId())

		zetaData[joinerIdHash] = append(zetaData[joinerIdHash], &protocol.Zeta_Data{
			Data: &protocol.Zeta_Data_Movie_{
				Movie: &protocol.Zeta_Data_Movie{
					Id:    movie.GetId(),
					Title: movie.GetTitle(),
				},
			},
		})

		iotaData[joinerIdHash] = append(iotaData[joinerIdHash], &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Movie_{
				Movie: &protocol.Iota_Data_Movie{
					Id: movie.GetId(),
				},
			},
		})
	}

	for nodeId, data := range betaData {
		tasks[FILTER_EXCHANGE][BETA_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Beta{
				Beta: &protocol.Beta{
					Data: data,
				},
			},
		}
	}

	for nodeId, data := range zetaData {
		tasks[JOIN_EXCHANGE][ZETA_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: data,
				},
			},
		}
	}

	for nodeId, data := range iotaData {
		tasks[JOIN_EXCHANGE][IOTA_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Iota{
				Iota: &protocol.Iota{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
betaStage filters movies based on the following criteria:
The movie must be released before the year 2010 and must be produced in Argentina.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"": Task
			}
		},
	}
*/
func (f *Filter) betaStage(data []*protocol.Beta_Data) (tasks Tasks) {
	RESULT_EXCHANGE := f.infraConfig.GetResultExchange()
	BROADCAST_ID := f.infraConfig.GetBroadcastID()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	resData := make(map[string][]*protocol.Result1_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		if movie.GetReleaseYear() >= MOVIE_YEAR_2010 {
			continue
		}

		if !slices.Contains(movie.GetProdCountries(), SPAIN_COUNTRY) {
			continue
		}

		// TODO: USE CLIENT ID INSTEAD OF BROADCAST ID WHEN MULTICLIENTS ARE IMPLEMENTED
		resData[BROADCAST_ID] = append(resData[BROADCAST_ID], &protocol.Result1_Data{
			Id:     movie.GetId(),
			Title:  movie.GetTitle(),
			Genres: movie.GetGenres(),
		})
	}

	for nodeId, data := range resData {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Result1{
				Result1: &protocol.Result1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
gammaStage filters movies based on the following criteria:
The movie must be produced in only one country.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"delta_1": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (f *Filter) gammaStage(data []*protocol.Gamma_Data) (tasks Tasks) {
	MAP_EXCHANGE := f.infraConfig.GetMapExchange()
	MAP_COUNT := f.infraConfig.GetMapCount()

	tasks = make(Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][DELTA_STAGE_1] = make(map[string]*protocol.Task)
	delta1Data := make(map[string][]*protocol.Delta_1_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		countries := movie.GetProdCountries()

		if countries == nil {
			continue
		}

		if len(countries) != 1 {
			continue
		}

		mapIdHash := utils.GetWorkerIdFromHash(MAP_COUNT, movie.GetId())

		delta1Data[mapIdHash] = append(delta1Data[mapIdHash], &protocol.Delta_1_Data{
			Country: countries[0],
			Budget:  movie.GetBudget(),
		})
	}

	for nodeId, data := range delta1Data {
		tasks[MAP_EXCHANGE][DELTA_STAGE_1][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Delta_1{
				Delta_1: &protocol.Delta_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (f *Filter) getNextNodeId(nodeId string) (string, error) {
	clientId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert clientId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (clientId+1)%f.infraConfig.GetFilterCount())
	return nextNodeId, nil
}

func (f *Filter) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case ALPHA_STAGE:
		return BETA_STAGE, f.infraConfig.GetFilterExchange(), f.infraConfig.GetFilterCount(), nil
	case BETA_STAGE:
		return RESULT_STAGE, f.infraConfig.GetResultExchange(), 1, nil
	case GAMMA_STAGE:
		return DELTA_STAGE_1, f.infraConfig.GetMapExchange(), f.infraConfig.GetMapCount(), nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (f *Filter) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == f.infraConfig.GetNodeId() {
		nextStage, nextExchange, nextStageCount, err := f.getNextStageData(data.GetStage())
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
			nextRingEOF.WorkerCreatorId = f.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := f.getNextNodeId(f.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		filterExchange := f.infraConfig.GetFilterExchange()
		stage := data.GetStage()

		tasks[filterExchange] = make(map[string]map[string]*protocol.Task)
		tasks[filterExchange][stage] = make(map[string]*protocol.Task)
		tasks[filterExchange][stage][nextNode] = eofTask

	}
	return tasks
}

// Execute executes the action.
// It returns a map of tasks for the next stages.
// It returns an error if the action fails.
func (f *Filter) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Alpha:
		data := v.Alpha.GetData()
		return f.alphaStage(data), nil
	case *protocol.Task_Beta:
		data := v.Beta.GetData()
		return f.betaStage(data), nil
	case *protocol.Task_Gamma:
		data := v.Gamma.GetData()
		return f.gammaStage(data), nil
	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return f.omegaEOFStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
