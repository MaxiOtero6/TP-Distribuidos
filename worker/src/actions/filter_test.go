package actions

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/stretchr/testify/assert"
)

func TestAlphaStage(t *testing.T) {
	clusterConfig := &model.WorkerClusterConfig{
		JoinCount: 1,
	}

	filter := NewFilter(clusterConfig)

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.alphaStage(nil)
		assert.Len(t, result, 2, "Expected 2 exchanges")
		assert.Len(t, result[FILTER_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[FILTER_EXCHANGE][BETA_STAGE], 0, "Expected 0 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE], 2, "Expected 2 stages")
		assert.Len(t, result[JOIN_EXCHANGE][IOTA_STAGE], 0, "Expected 0 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE][ZETA_STAGE], 0, "Expected 0 destination ID")

		beta := result[FILTER_EXCHANGE][BETA_STAGE][BROADCAST_ID].GetBeta().GetData()
		assert.Len(t, beta, 0, "Expected empty slice")

		iotaData := result[JOIN_EXCHANGE][IOTA_STAGE][TEST_WORKER_ID].GetIota().GetData()
		assert.Len(t, iotaData, 0, "Expected empty slice")

		zeta := result[JOIN_EXCHANGE][ZETA_STAGE][TEST_WORKER_ID].GetZeta().GetData()
		assert.Len(t, zeta, 0, "Expected empty slice")
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.alphaStage([]*protocol.Alpha_Data{})
		assert.Len(t, result, 2, "Expected 2 exchanges")
		assert.Len(t, result[FILTER_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[FILTER_EXCHANGE][BETA_STAGE], 0, "Expected 0 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE], 2, "Expected 2 stages")
		assert.Len(t, result[JOIN_EXCHANGE][IOTA_STAGE], 0, "Expected 0 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE][ZETA_STAGE], 0, "Expected 0 destination ID")

		beta := result[FILTER_EXCHANGE][BETA_STAGE][BROADCAST_ID].GetBeta().GetData()
		assert.Len(t, beta, 0, "Expected empty slice")

		iotaData := result[JOIN_EXCHANGE][IOTA_STAGE][TEST_WORKER_ID].GetIota().GetData()
		assert.Len(t, iotaData, 0, "Expected empty slice")

		zeta := result[JOIN_EXCHANGE][ZETA_STAGE][TEST_WORKER_ID].GetZeta().GetData()
		assert.Len(t, zeta, 0, "Expected empty slice")
	})

	t.Run("Test with valid data", func(t *testing.T) {
		data := []*protocol.Alpha_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   2005,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Drama"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1999,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Thriller"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain"},
				Genres:        []string{"Action", "Comedy"},
			},
			{
				Id:            "4",
				Title:         "Movie 4",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain", "Argentina"},
				Genres:        []string{"Romantic"},
			},
			{
				Id:            "5",
				Title:         "Movie 5",
				ReleaseYear:   2005,
				ProdCountries: nil,
				Genres:        []string{"Action", "Drama"},
			},
			nil,
		}

		result := filter.alphaStage(data)
		assert.Len(t, result, 2, "Expected 2 exchanges")
		assert.Len(t, result[FILTER_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[FILTER_EXCHANGE][BETA_STAGE], 1, "Expected 1 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE], 2, "Expected 2 stages")
		assert.Len(t, result[JOIN_EXCHANGE][IOTA_STAGE], 1, "Expected 1 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE][ZETA_STAGE], 1, "Expected 1 destination ID")

		betaData := result[FILTER_EXCHANGE][BETA_STAGE][BROADCAST_ID].GetBeta().GetData()
		assert.Len(t, betaData, 2, "Expected 2 movies")

		assert.Equal(t, "1", betaData[0].GetId(), "Expected movie ID 1")
		assert.Equal(t, "Movie 1", betaData[0].GetTitle(), "Expected movie title 'Movie 1'")
		assert.Equal(t, uint32(2005), betaData[0].GetReleaseYear(), "Expected release year 2005")
		assert.Equal(t, []string{"Argentina"}, betaData[0].GetProdCountries(), "Expected production countries 'Argentina'")
		assert.Equal(t, []string{"Action", "Drama"}, betaData[0].GetGenres(), "Expected genres 'Action', 'Drama'")

		assert.Equal(t, "4", betaData[1].GetId(), "Expected movie ID 4")
		assert.Equal(t, "Movie 4", betaData[1].GetTitle(), "Expected movie title 'Movie 4'")
		assert.Equal(t, uint32(2005), betaData[1].GetReleaseYear(), "Expected release year 2005")
		assert.Equal(t, []string{"Spain", "Argentina"}, betaData[1].GetProdCountries(), "Expected production countries 'Spain', 'Argentina'")
		assert.Equal(t, []string{"Romantic"}, betaData[1].GetGenres(), "Expected genres 'Romantic'")

		iotaData := result[JOIN_EXCHANGE][IOTA_STAGE][TEST_WORKER_ID].GetIota().GetData()
		assert.Len(t, iotaData, 2, "Expected 2 movies")

		assert.Equal(t, "1", iotaData[0].GetMovie().GetId(), "Expected movie ID 1")
		assert.Equal(t, "4", iotaData[1].GetMovie().GetId(), "Expected movie ID 4")

		zetaData := result[JOIN_EXCHANGE][ZETA_STAGE][TEST_WORKER_ID].GetZeta().GetData()
		assert.Len(t, zetaData, 2, "Expected 2 movies")

		assert.Equal(t, "1", zetaData[0].GetMovie().GetId(), "Expected movie ID 1")
		assert.Equal(t, "Movie 1", zetaData[0].GetMovie().GetTitle(), "Expected movie title 'Movie 1'")

		assert.Equal(t, "4", zetaData[1].GetMovie().GetId(), "Expected movie ID 4")
		assert.Equal(t, "Movie 4", zetaData[1].GetMovie().GetTitle(), "Expected movie title 'Movie 4'")
	})

	t.Run("Test with all movies filtered out", func(t *testing.T) {
		data := []*protocol.Alpha_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   1990,
				ProdCountries: []string{"USA"},
				Genres:        []string{"Action", "Drama"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1995,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Thriller"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: nil,
				Genres:        []string{"Action", "Comedy"},
			},
			nil,
		}

		result := filter.alphaStage(data)
		assert.Len(t, result, 2, "Expected 2 exchanges")
		assert.Len(t, result[FILTER_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[FILTER_EXCHANGE][BETA_STAGE], 0, "Expected 1 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE], 2, "Expected 2 stages")
		assert.Len(t, result[JOIN_EXCHANGE][IOTA_STAGE], 0, "Expected 1 destination ID")
		assert.Len(t, result[JOIN_EXCHANGE][ZETA_STAGE], 0, "Expected 1 destination ID")

		beta := result[FILTER_EXCHANGE][BETA_STAGE][BROADCAST_ID].GetBeta().GetData()
		assert.Len(t, beta, 0, "Expected empty slice")

		iotaData := result[JOIN_EXCHANGE][IOTA_STAGE][TEST_WORKER_ID].GetIota().GetData()
		assert.Len(t, iotaData, 0, "Expected empty slice")

		zeta := result[JOIN_EXCHANGE][ZETA_STAGE][TEST_WORKER_ID].GetZeta().GetData()
		assert.Len(t, zeta, 0, "Expected empty slice")
	})
}

func TestBetaStage(t *testing.T) {
	filter := &Filter{}

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.betaStage(nil)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[RESULT_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[RESULT_EXCHANGE][RESULT_STAGE], 0, "Expected 0 destination ID")

		res := result[RESULT_EXCHANGE][RESULT_STAGE][BROADCAST_ID].GetResult1().GetData()
		assert.Len(t, res, 0, "Expected empty slice")
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.betaStage([]*protocol.Beta_Data{})
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[RESULT_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[RESULT_EXCHANGE][RESULT_STAGE], 0, "Expected 0 destination ID")

		res := result[RESULT_EXCHANGE][RESULT_STAGE][BROADCAST_ID].GetResult1().GetData()
		assert.Len(t, res, 0, "Expected empty slice")
	})

	t.Run("Test with valid data", func(t *testing.T) {
		data := []*protocol.Beta_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   2005,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Drama"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1999,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Thriller"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain"},
				Genres:        []string{"Action", "Comedy"},
			},
			{
				Id:            "4",
				Title:         "Movie 4",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain", "Argentina"},
				Genres:        []string{"Romantic"},
			},
			{
				Id:            "5",
				Title:         "Movie 5",
				ReleaseYear:   2005,
				ProdCountries: nil,
				Genres:        []string{"Action", "Drama"},
			},
			nil,
		}

		result := filter.betaStage(data)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[RESULT_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[RESULT_EXCHANGE][RESULT_STAGE], 1, "Expected 1 destination ID")

		res := result[RESULT_EXCHANGE][RESULT_STAGE][BROADCAST_ID].GetResult1().GetData()
		assert.Len(t, res, 2, "Expected 2 movies")

		assert.Equal(t, "3", res[0].GetId(), "Expected movie ID 3")
		assert.Equal(t, "Movie 3", res[0].GetTitle(), "Expected movie title 'Movie 3'")
		assert.Equal(t, []string{"Action", "Comedy"}, res[0].GetGenres(), "Expected genres 'Action', 'Comedy'")

		assert.Equal(t, "4", res[1].GetId(), "Expected movie ID 4")
		assert.Equal(t, "Movie 4", res[1].GetTitle(), "Expected movie title 'Movie 4'")
		assert.Equal(t, []string{"Romantic"}, res[1].GetGenres(), "Expected genres 'Romantic'")
	})

	t.Run("Test with all movies filtered out", func(t *testing.T) {
		data := []*protocol.Beta_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   1990,
				ProdCountries: []string{"USA"},
				Genres:        []string{"Action", "Drama"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   2020,
				ProdCountries: []string{"Argentina"},
				Genres:        []string{"Action", "Thriller"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: nil,
				Genres:        []string{"Action", "Comedy"},
			},
			nil,
		}

		result := filter.betaStage(data)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[RESULT_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[RESULT_EXCHANGE][RESULT_STAGE], 0, "Expected 0 destination ID")

		res := result[RESULT_EXCHANGE][RESULT_STAGE][BROADCAST_ID].GetResult1().GetData()
		assert.Empty(t, res, "Expected empty slice")
	})
}

func TestGammaStage(t *testing.T) {
	filter := &Filter{}

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.gammaStage(nil)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[MAP_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[MAP_EXCHANGE][DELTA_STAGE_1], 0, "Expected 0 destination ID")

		res := result[MAP_EXCHANGE][DELTA_STAGE_1][BROADCAST_ID].GetDelta_1().GetData()
		assert.Empty(t, res, "Expected empty slice")
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.gammaStage([]*protocol.Gamma_Data{})
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[MAP_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[MAP_EXCHANGE][DELTA_STAGE_1], 0, "Expected 0 destination ID")

		res := result[MAP_EXCHANGE][DELTA_STAGE_1][BROADCAST_ID].GetDelta_1().GetData()
		assert.Empty(t, res, "Expected empty slice")
	})

	t.Run("Test with valid data", func(t *testing.T) {
		data := []*protocol.Gamma_Data{
			{
				Id:            "1",
				Budget:        2005,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "2",
				Budget:        1999,
				ProdCountries: []string{"Argentina", "Australia"},
			},
			{
				Id:            "3",
				Budget:        2005,
				ProdCountries: []string{"Spain"},
			},
			{
				Id:            "4",
				Budget:        2005,
				ProdCountries: []string{"Spain", "Argentina"},
			},
			{
				Id:            "5",
				Budget:        2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.gammaStage(data)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[MAP_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[MAP_EXCHANGE][DELTA_STAGE_1], 1, "Expected 1 destination ID")

		res := result[MAP_EXCHANGE][DELTA_STAGE_1][BROADCAST_ID].GetDelta_1().GetData()
		assert.Len(t, res, 2, "Expected 2 movies")

		assert.Equal(t, "1", res[0].GetId(), "Expected movie ID 1")
		assert.Equal(t, uint64(2005), res[0].GetBudget(), "Expected movie budget 2005")
		assert.Equal(t, "Argentina", res[0].GetProdCountry(), "Expected production country 'Argentina'")

		assert.Equal(t, "3", res[1].GetId(), "Expected movie ID 3")
		assert.Equal(t, uint64(2005), res[1].GetBudget(), "Expected movie budget 2005")
		assert.Equal(t, "Spain", res[1].GetProdCountry(), "Expected production country 'Spain'")
	})

	t.Run("Test with all movies filtered out", func(t *testing.T) {
		data := []*protocol.Gamma_Data{
			{
				Id:            "1",
				Budget:        1990,
				ProdCountries: []string{"USA", "Argentina"},
			},
			{
				Id:            "2",
				Budget:        2020,
				ProdCountries: []string{"Argentina", "Australia"},
			},
			{
				Id:            "3",
				Budget:        2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.gammaStage(data)
		assert.Len(t, result, 1, "Expected 1 exchange")
		assert.Len(t, result[MAP_EXCHANGE], 1, "Expected 1 stage")
		assert.Len(t, result[MAP_EXCHANGE][DELTA_STAGE_1], 0, "Expected 0 destination ID")

		res := result[MAP_EXCHANGE][DELTA_STAGE_1][BROADCAST_ID].GetDelta_1().GetData()
		assert.Empty(t, res, "Expected empty slice")
	})
}

func TestExecute(t *testing.T) {
	t.Run("Test with Alpha stage", func(t *testing.T) {
		filter := &Filter{}
		task := &protocol.Task{
			Stage: &protocol.Task_Alpha{
				Alpha: &protocol.Alpha{
					Data: []*protocol.Alpha_Data{},
				},
			},
		}

		_, err := filter.Execute(task)
		assert.NoError(t, err, "Expected no error, got %v", err)
	})

	t.Run("Test with Beta stage", func(t *testing.T) {
		filter := &Filter{}
		task := &protocol.Task{
			Stage: &protocol.Task_Beta{
				Beta: &protocol.Beta{
					Data: []*protocol.Beta_Data{},
				},
			},
		}

		_, err := filter.Execute(task)
		assert.NoError(t, err, "Expected no error, got %v", err)
	})

	t.Run("Test with Gamma stage", func(t *testing.T) {
		filter := &Filter{}
		task := &protocol.Task{
			Stage: &protocol.Task_Gamma{
				Gamma: &protocol.Gamma{
					Data: []*protocol.Gamma_Data{},
				},
			},
		}

		_, err := filter.Execute(task)
		assert.NoError(t, err, "Expected no error, got %v", err)
	})

	t.Run("Test with invalid stage", func(t *testing.T) {
		filter := &Filter{}
		task := &protocol.Task{
			Stage: &protocol.Task_Result1{},
		}

		result, err := filter.Execute(task)
		assert.Error(t, err, "Expected error, got nil")

		assert.Nil(t, result, "Expected nil result, got %v", result)
	})
}
