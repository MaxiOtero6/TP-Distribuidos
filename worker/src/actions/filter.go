package actions

import (
	"fmt"

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
				"": Task
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
	BROADCAST_ID := f.infraConfig.GetBroadcastID()
	JOIN_COUNT := f.infraConfig.GetJoinCount()

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

		betaData[BROADCAST_ID] = append(betaData[BROADCAST_ID], &protocol.Beta_Data{
			Id:            movie.GetId(),
			Title:         movie.GetTitle(),
			ReleaseYear:   movie.GetReleaseYear(),
			ProdCountries: movie.GetProdCountries(),
			Genres:        movie.GetGenres(),
		})

		idHash := utils.GetWorkerIdFromHash(JOIN_COUNT, movie.GetId())

		zetaData[idHash] = append(zetaData[idHash], &protocol.Zeta_Data{
			Data: &protocol.Zeta_Data_Movie_{
				Movie: &protocol.Zeta_Data_Movie{
					Id:    movie.GetId(),
					Title: movie.GetTitle(),
				},
			},
		})

		iotaData[idHash] = append(iotaData[idHash], &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Movie_{
				Movie: &protocol.Iota_Data_Movie{
					Id: movie.GetId(),
				},
			},
		})
	}

	for id, data := range betaData {
		tasks[FILTER_EXCHANGE][BETA_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Beta{
				Beta: &protocol.Beta{
					Data: data,
				},
			},
		}
	}

	for id, data := range zetaData {
		tasks[JOIN_EXCHANGE][ZETA_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: data,
				},
			},
		}
	}

	for id, data := range iotaData {
		tasks[JOIN_EXCHANGE][IOTA_STAGE][id] = &protocol.Task{
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

		resData[BROADCAST_ID] = append(resData[BROADCAST_ID], &protocol.Result1_Data{
			Id:     movie.GetId(),
			Title:  movie.GetTitle(),
			Genres: movie.GetGenres(),
		})
	}

	for id, data := range resData {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
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
				"": Task
			}
		},
	}
*/
func (f *Filter) gammaStage(data []*protocol.Gamma_Data) (tasks Tasks) {
	MAP_EXCHANGE := f.infraConfig.GetMapExchange()
	BROADCAST_ID := f.infraConfig.GetBroadcastID()

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

		delta1Data[BROADCAST_ID] = append(delta1Data[BROADCAST_ID], &protocol.Delta_1_Data{
			Country: countries[0],
			Budget:  movie.GetBudget(),
		})
	}

	for id, data := range delta1Data {
		tasks[MAP_EXCHANGE][DELTA_STAGE_1][id] = &protocol.Task{
			Stage: &protocol.Task_Delta_1{
				Delta_1: &protocol.Delta_1{
					Data: data,
				},
			},
		}
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
	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
