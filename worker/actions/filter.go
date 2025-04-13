package actions

import (
	"fmt"

	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

// Filter is a struct that implements the Action interface.
// It filters movies based on certain criteria.
// It is used in the worker to filter movies in the pipeline.
type Filter struct{}

const ARGENTINA_COUNTRY = "Argentina"
const SPAIN_COUNTRY = "Spain"
const MOVIE_YEAR_2000 = 2000
const MOVIE_YEAR_2010 = 2010

/*
alphaStage filters movies based on the following criteria:
The movie must be released from the year 2000 and must be produced in Argentina.

It returns a map of tasks for the next stages: beta, zeta, and iota.
The beta stage contains the filtered movies.
The zeta stage contains the movie ID and title.
The iota stage contains the movie ID only.

The keys of the map are the stage names.
The values are the tasks for each stage.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return nil.
*/
func (f *Filter) alphaStage(data []*protocol.Alpha_Data) map[string]*protocol.Task {
	var betaData []*protocol.Beta_Data
	var zetaData []*protocol.Zeta_Data
	var iotaData []*protocol.Iota_Data

	if data == nil {
		return nil
	}

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

		betaData = append(betaData, &protocol.Beta_Data{
			Id:            movie.GetId(),
			Title:         movie.GetTitle(),
			ReleaseYear:   movie.GetReleaseYear(),
			ProdCountries: movie.GetProdCountries(),
			Genres:        movie.GetGenres(),
		})

		zetaData = append(zetaData, &protocol.Zeta_Data{
			Data: &protocol.Zeta_Data_Movie_{
				Movie: &protocol.Zeta_Data_Movie{
					Id:    movie.GetId(),
					Title: movie.GetTitle(),
				},
			},
		})

		iotaData = append(iotaData, &protocol.Iota_Data{
			Data: &protocol.Iota_Data_Movie_{
				Movie: &protocol.Iota_Data_Movie{
					Id: movie.GetId(),
				},
			},
		})
	}

	return map[string]*protocol.Task{
		"beta": {
			Stage: &protocol.Task_Beta{
				Beta: &protocol.Beta{
					Data: betaData,
				},
			},
		},
		"zeta": {
			Stage: &protocol.Task_Zeta{
				Zeta: &protocol.Zeta{
					Data: zetaData,
				},
			},
		},
		"iota": {
			Stage: &protocol.Task_Iota{
				Iota: &protocol.Iota{
					Data: iotaData,
				},
			},
		},
	}
}

/*
betaStage filters movies based on the following criteria:
The movie must be released before the year 2010 and must be produced in Argentina.

It returns a map of tasks for the next stage: result.
The result stage contains the filtered movies.

The keys of the map are the stage names.
The values are the tasks for each stage.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return nil.
*/
func (f *Filter) betaStage(data []*protocol.Beta_Data) map[string]*protocol.Task {
	var res []*protocol.Result1_Data

	if data == nil {
		return nil
	}

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

		res = append(res, &protocol.Result1_Data{
			Id:     movie.GetId(),
			Title:  movie.GetTitle(),
			Genres: movie.GetGenres(),
		})
	}

	return map[string]*protocol.Task{
		"result": {
			Stage: &protocol.Task_Result1{
				Result1: &protocol.Result1{
					Data: res,
				},
			},
		},
	}
}

/*
betaStage filters movies based on the following criteria:
The movie must be produced in only one country.

It returns a map of tasks for the next stage: delta.
The delta stage contains the movie ID, production country and budget for each movie.

The keys of the map are the stage names.
The values are the tasks for each stage.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return nil.
*/
func (f *Filter) gammaStage(data []*protocol.Gamma_Data) map[string]*protocol.Task {
	var res []*protocol.Delta_Data

	if data == nil {
		return nil
	}

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

		res = append(res, &protocol.Delta_Data{
			Id:          movie.GetId(),
			ProdCountry: countries[0],
			Budget:      movie.GetBudget(),
		})
	}

	return map[string]*protocol.Task{
		"delta": {
			Stage: &protocol.Task_Delta{
				Delta: &protocol.Delta{
					Data: res,
				},
			},
		},
	}
}

// Execute executes the action.
// It returns a map of tasks for the next stages.
// It returns an error if the action fails.
func (f *Filter) Execute(task *protocol.Task) (map[string]*protocol.Task, error) {
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
		return nil, fmt.Errorf("invalid query stage")
	}
}
