package utils

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
)

func GetAlphaStageTask(movies []*model.Movie, filtersCount int) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	var alphaData = make(map[string][]*protocol.Alpha_Data)

	for _, movie := range movies {
		idHash := utils.GetWorkerIdFromHash(filtersCount, movie.Id)

		alphaData[idHash] = append(alphaData[idHash], &protocol.Alpha_Data{
			Id:            movie.Id,
			Title:         movie.Title,
			ProdCountries: movie.ProdCountries,
			Genres:        movie.Genres,
			ReleaseYear:   movie.ReleaseYear,
		})
	}

	for id, data := range alphaData {
		tasks[id] = &protocol.Task{
			Stage: &protocol.Task_Alpha{
				Alpha: &protocol.Alpha{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func GetZetaStageRatingsTask(ratings []*model.Rating, joinersCount int) (tasks map[string]*protocol.Task) {
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
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func GetIotaStageCreditsTask(actors []*model.Actor, joinersCount int) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	iotaData := make(map[string][]*protocol.Iota_Data)

	for _, actor := range actors {
		idHash := utils.GetWorkerIdFromHash(joinersCount, actor.MovieId)

		iotaData[idHash] = append(iotaData[idHash], &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Actor_{
				Actor: &protocol.Iota_Data_Actor{
					Id:      actor.Id,
					Name:    actor.Name,
					MovieId: actor.MovieId,
				},
			},
		})
	}

	for id, data := range iotaData {
		tasks[id] = &protocol.Task{
			Stage: &protocol.Task_Iota{
				Iota: &protocol.Iota{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func GetMuStageTask(movies []*model.Movie, overviewCount int) (tasks map[string]*protocol.Task) {
	tasks = make(map[string]*protocol.Task)
	var muData = make(map[string][]*protocol.Mu_Data)

	for _, movie := range movies {
		idHash := utils.GetWorkerIdFromHash(overviewCount, movie.Id)

		muData[idHash] = append(muData[idHash], &protocol.Mu_Data{
			Id:       movie.Id,
			Title:    movie.Title,
			Revenue:  movie.Revenue,
			Budget:   movie.Budget,
			Overview: movie.Overview,
		})
	}

	for id, data := range muData {
		tasks[id] = &protocol.Task{
			Stage: &protocol.Task_Mu{
				Mu: &protocol.Mu{
					Data: data,
				},
			},
		}
	}

	return tasks
}
