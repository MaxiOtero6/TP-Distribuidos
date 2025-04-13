package actions

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

func TestAlphaStage(t *testing.T) {
	filter := &Filter{}

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.alphaStage(nil)

		if len(result) != 3 {
			t.Errorf("Expected 3 stages, got %d", len(result))
		}

		beta := result["beta"].GetBeta().GetData()
		if len(beta) != 0 {
			t.Errorf("Expected empty slice, got %v", beta)
		}

		iotaData := result["iota"].GetIota().GetData()
		if len(iotaData) != 0 {
			t.Errorf("Expected empty slice, got %v", iotaData)
		}

		zeta := result["zeta"].GetZeta().GetData()
		if len(zeta) != 0 {
			t.Errorf("Expected empty slice, got %v", zeta)
		}
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.alphaStage([]*protocol.Alpha_Data{})

		if len(result) != 3 {
			t.Errorf("Expected 3 stages, got %d", len(result))
		}

		beta := result["beta"].GetBeta().GetData()
		if len(beta) != 0 {
			t.Errorf("Expected empty slice, got %v", beta)
		}

		iotaData := result["iota"].GetIota().GetData()
		if len(iotaData) != 0 {
			t.Errorf("Expected empty slice, got %v", iotaData)
		}

		zeta := result["zeta"].GetZeta().GetData()
		if len(zeta) != 0 {
			t.Errorf("Expected empty slice, got %v", zeta)
		}
	})

	t.Run("Test with valid data", func(t *testing.T) {
		data := []*protocol.Alpha_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   2005,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1999,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain"},
			},
			{
				Id:            "4",
				Title:         "Movie 4",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain", "Argentina"},
			},
			{
				Id:            "5",
				Title:         "Movie 5",
				ReleaseYear:   2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.alphaStage(data)

		if len(result) != 3 {
			t.Errorf("Expected 3 stages, got %d", len(result))
		}

		betaData := result["beta"].GetBeta().GetData()
		if len(betaData) != 2 {
			t.Errorf("Expected 2 movies, got %d", len(betaData))
			return
		}

		if betaData[0].GetId() != "1" {
			t.Errorf("Expected movie ID 1, got %s", betaData[0].GetId())
		}

		if betaData[1].GetId() != "4" {
			t.Errorf("Expected movie ID 4, got %s", betaData[1].GetId())
		}

		iotaData := result["iota"].GetIota().GetData()
		if len(iotaData) != 2 {
			t.Errorf("Expected 2 movies, got %d", len(iotaData))
			return
		}

		if iotaData[0].GetMovie().GetId() != "1" {
			t.Errorf("Expected movie ID 1, got %s", iotaData[0].GetMovie().GetId())
		}

		if iotaData[1].GetMovie().GetId() != "4" {
			t.Errorf("Expected movie ID 4, got %s", iotaData[1].GetMovie().GetId())
		}

		zetaData := result["zeta"].GetZeta().GetData()
		if len(zetaData) != 2 {
			t.Errorf("Expected 2 movies, got %d", len(zetaData))
			return
		}

		if zetaData[0].GetMovie().GetId() != "1" {
			t.Errorf("Expected movie ID 1, got %s", zetaData[0].GetMovie().GetId())
		}

		if zetaData[1].GetMovie().GetId() != "4" {
			t.Errorf("Expected movie ID 4, got %s", zetaData[1].GetMovie().GetId())
		}
	})

	t.Run("Test with all movies filtered out", func(t *testing.T) {
		data := []*protocol.Alpha_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   1990,
				ProdCountries: []string{"USA"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1995,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.alphaStage(data)

		if len(result) != 3 {
			t.Errorf("Expected 3 stages, got %d", len(result))
		}

		beta := result["beta"].GetBeta().GetData()
		if len(beta) != 0 {
			t.Errorf("Expected empty slice, got %v", beta)
		}

		iotaData := result["iota"].GetIota().GetData()
		if len(iotaData) != 0 {
			t.Errorf("Expected empty slice, got %v", iotaData)
		}

		zeta := result["zeta"].GetZeta().GetData()
		if len(zeta) != 0 {
			t.Errorf("Expected empty slice, got %v", zeta)
		}
	})
}

func TestBetaStage(t *testing.T) {
	filter := &Filter{}

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.betaStage(nil)

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["result"].GetResult1().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.betaStage([]*protocol.Beta_Data{})

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["result"].GetResult1().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
	})

	t.Run("Test with valid data", func(t *testing.T) {
		data := []*protocol.Beta_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   2005,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   1999,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain"},
			},
			{
				Id:            "4",
				Title:         "Movie 4",
				ReleaseYear:   2005,
				ProdCountries: []string{"Spain", "Argentina"},
			},
			{
				Id:            "5",
				Title:         "Movie 5",
				ReleaseYear:   2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.betaStage(data)

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["result"].GetResult1().GetData()

		if len(res) != 2 {
			t.Errorf("Expected 1 movie, got %d", len(res))
			return
		}

		if res[0].GetId() != "3" {
			t.Errorf("Expected movie ID 3, got %s", res[0].GetId())
		}

		if res[1].GetId() != "4" {
			t.Errorf("Expected movie ID 4, got %s", res[1].GetId())
		}
	})

	t.Run("Test with all movies filtered out", func(t *testing.T) {
		data := []*protocol.Beta_Data{
			{
				Id:            "1",
				Title:         "Movie 1",
				ReleaseYear:   1990,
				ProdCountries: []string{"USA"},
			},
			{
				Id:            "2",
				Title:         "Movie 2",
				ReleaseYear:   2020,
				ProdCountries: []string{"Argentina"},
			},
			{
				Id:            "3",
				Title:         "Movie 3",
				ReleaseYear:   2005,
				ProdCountries: nil,
			},
			nil,
		}

		result := filter.betaStage(data)

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["result"].GetResult1().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
	})
}

func TestGammaStage(t *testing.T) {
	filter := &Filter{}

	t.Run("Test with nil data", func(t *testing.T) {
		result := filter.gammaStage(nil)

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["delta"].GetDelta().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
	})

	t.Run("Test with empty data", func(t *testing.T) {
		result := filter.gammaStage([]*protocol.Gamma_Data{})

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["delta"].GetDelta().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
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

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["delta"].GetDelta().GetData()

		if len(res) != 2 {
			t.Errorf("Expected 2 movie, got %d", len(res))
			return
		}

		if res[0].GetId() != "1" {
			t.Errorf("Expected movie ID 1, got %s", res[0].GetId())
		}

		if res[1].GetId() != "3" {
			t.Errorf("Expected movie ID 3, got %s", res[1].GetId())
		}
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

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}

		res := result["delta"].GetDelta().GetData()
		if len(res) != 0 {
			t.Errorf("Expected empty slice, got %v", res)
		}
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

		result, err := filter.Execute(task)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 3 {
			t.Errorf("Expected 3 stages, got %d", len(result))
		}
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

		result, err := filter.Execute(task)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}
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

		result, err := filter.Execute(task)
		if err != nil {
			t.Errorf("Expected no error, got %v", err)
		}

		if len(result) != 1 {
			t.Errorf("Expected 1 stages, got %d", len(result))
		}
	})

	t.Run("Test with invalid stage", func(t *testing.T) {
		filter := &Filter{}
		task := &protocol.Task{
			Stage: &protocol.Task_Result1{},
		}

		result, err := filter.Execute(task)
		if err == nil {
			t.Errorf("Expected error, got nil")
		}

		if result != nil {
			t.Errorf("Expected nil result, got %v", result)
		}
	})
}
