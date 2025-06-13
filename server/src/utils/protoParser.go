package utils

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
)

const ALPHA_STAGE = "alpha"
const ZETA_STAGE = "zeta"
const IOTA_STAGE = "iota"
const MU_STAGE = "mu"
const GAMMA_STAGE = "gamma"

// EOF Types
const SMALL_TABLE string = "small"
const BIG_TABLE string = "big"
const GENERAL string = "general"

const EOF_BROADCAST_RK string = "eof"

func GetEOFTask(workersCount int, clientId string, stage string, taskCount uint32) map[string]*protocol.Task {
	tasks := make(map[string]*protocol.Task)

	var EofType string

	switch stage {
	case ALPHA_STAGE:
		EofType = SMALL_TABLE
	case ZETA_STAGE:
		EofType = BIG_TABLE
	case IOTA_STAGE:
		EofType = BIG_TABLE

	default:
		EofType = GENERAL
	}

	tasks[EOF_BROADCAST_RK] = &protocol.Task{
		ClientId: clientId,
		Stage: &protocol.Task_OmegaEOF{
			OmegaEOF: &protocol.OmegaEOF{
				Data: &protocol.OmegaEOF_Data{
					Stage:      stage,
					TasksCount: taskCount,
					EofType:    EofType,
				},
			},
		},
	}

	return tasks
}

func GetAlphaStageTask(movies []*model.Movie, filtersCount int, clientId string, taskNumber uint32) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	var alphaData = make(map[string][]*protocol.Alpha_Data)

	for _, movie := range movies {
		alphaData[""] = append(alphaData[""], &protocol.Alpha_Data{
			Id:            movie.Id,
			Title:         movie.Title,
			ProdCountries: movie.ProdCountries,
			Genres:        movie.Genres,
			ReleaseYear:   movie.ReleaseYear,
		})
	}

	for id, data := range alphaData {
		tasks[id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Alpha{
				Alpha: &protocol.Alpha{
					Data: data,
				},
			},
			TaskIdentifier: &protocol.TaskIdentifier{
				TaskNumber:         taskNumber,
				TaskFragmentNumber: 0,
				LastFragment:       true,
			},
		}
	}

	return tasks
}

func GetGammaStageTask(movies []*model.Movie, filtersCount int, clientId string, taskNumber uint32) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	var gammaData = make(map[string][]*protocol.Gamma_Data)

	for _, movie := range movies {
		gammaData[""] = append(gammaData[""], &protocol.Gamma_Data{
			Id:            movie.Id,
			Budget:        movie.Budget,
			ProdCountries: movie.ProdCountries,
		})
	}

	for id, data := range gammaData {
		tasks[id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Gamma{
				Gamma: &protocol.Gamma{
					Data: data,
				},
			},
			TaskIdentifier: &protocol.TaskIdentifier{
				TaskNumber:         taskNumber,
				TaskFragmentNumber: 0,
				LastFragment:       true,
			},
		}
	}

	return tasks
}

func GetZetaStageRatingsTask(ratings []*model.Rating, joinersCount int, clientId string, taskNumber uint32) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	zetaData := make(map[string][]*protocol.Zeta_Data)

	for _, rating := range ratings {
		idHash := utils.GetWorkerIdFromHash(joinersCount, rating.MovieId)

		zetaData[idHash] = append(zetaData[idHash], &protocol.Zeta_Data{
			Data: &protocol.Zeta_Data_Rating_{
				Rating: &protocol.Zeta_Data_Rating{
					MovieId: rating.MovieId,
					Rating:  rating.Rating,
				},
			},
		})
	}

	for id, data := range zetaData {
		tasks[id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: data,
				},
			},
			TaskIdentifier: &protocol.TaskIdentifier{
				TaskNumber:         taskNumber,
				TaskFragmentNumber: 0,
				LastFragment:       true,
			},
		}
	}

	return tasks
}

func GetIotaStageCreditsTask(actors []*model.Actor, joinersCount int, clientId string, taskNumber uint32) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	iotaData := make(map[string][]*protocol.Iota_Data)

	for _, actor := range actors {
		idHash := utils.GetWorkerIdFromHash(joinersCount, actor.MovieId)

		iotaData[idHash] = append(iotaData[idHash], &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Actor_{
				Actor: &protocol.Iota_Data_Actor{
					ActorId:   actor.Id,
					ActorName: actor.Name,
					MovieId:   actor.MovieId,
				},
			},
		})
	}

	for id, data := range iotaData {
		tasks[id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Iota{
				Iota: &protocol.Iota{
					Data: data,
				},
			},
			TaskIdentifier: &protocol.TaskIdentifier{
				TaskNumber:         taskNumber,
				TaskFragmentNumber: 0,
				LastFragment:       true,
			},
		}
	}

	return tasks
}

func GetMuStageTask(movies []*model.Movie, overviewCount int, clientId string, taskNumber uint32) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	var muData = make(map[string][]*protocol.Mu_Data)

	for _, movie := range movies {
		muData[""] = append(muData[""], &protocol.Mu_Data{
			Id:       movie.Id,
			Title:    movie.Title,
			Revenue:  movie.Revenue,
			Budget:   movie.Budget,
			Overview: movie.Overview,
		})
	}

	for id, data := range muData {
		tasks[id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Mu{
				Mu: &protocol.Mu{
					Data: data,
				},
			},
			TaskIdentifier: &protocol.TaskIdentifier{
				TaskNumber:         taskNumber,
				TaskFragmentNumber: 0,
				LastFragment:       true,
			},
		}
	}

	return tasks
}
