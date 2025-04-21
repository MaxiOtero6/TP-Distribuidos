package actions

import (
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/stretchr/testify/assert"
)

func TestDelta1Stage(t *testing.T) {
	REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

	t.Run("Test Delta1Stage with one reducer", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 1,
			},
			testInfraConfig.GetRabbit(),
		)

		mapper := NewMapper(infra)

		data := []*protocol.Delta_1_Data{
			{Country: "USA", Budget: 100},
			{Country: "USA", Budget: 200},
			{Country: "UK", Budget: 150},
		}

		tasks := mapper.delta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], DELTA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][DELTA_STAGE_2], 1) // ReduceCount is 1

		resultData := tasks[REDUCE_EXCHANGE][DELTA_STAGE_2]["0"].GetDelta_2().GetData()
		assert.Len(t, resultData, 2) // Should contain 2 countries
	})

	t.Run("Test Delta1Stage with multiple reducers", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 2,
			},
			testInfraConfig.GetRabbit(),
		)

		mapper := NewMapper(infra)

		data := []*protocol.Delta_1_Data{
			{Country: "USA", Budget: 100},
			{Country: "USA", Budget: 200},
			{Country: "UK", Budget: 150},
		}

		tasks := mapper.delta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], DELTA_STAGE_2)

		if len(tasks[REDUCE_EXCHANGE][DELTA_STAGE_2]) > 2 || len(tasks[REDUCE_EXCHANGE][DELTA_STAGE_2]) == 0 {
			assert.Fail(t, "Because of random behaviour, it should be 1 or $ReduceCount tasks, got %v", len(tasks[REDUCE_EXCHANGE][DELTA_STAGE_2]))
		}
	})

	t.Run("Test Delta1Stage with empty data", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 2,
			},
			testInfraConfig.GetRabbit(),
		)

		mapper := NewMapper(infra)

		data := []*protocol.Delta_1_Data{}

		tasks := mapper.delta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], DELTA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][DELTA_STAGE_2], 0) // Empty data should return no tasks
	})
}

func TestEta1Stage(t *testing.T) {
	REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

	t.Run("Test Eta1Stage with one reducer", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 1,
			},
			testInfraConfig.GetRabbit(),
		)

		mapper := NewMapper(infra)

		data := []*protocol.Eta_1_Data{
			{Id: "0", Title: "Movie 0", Rating: 4},
			{Id: "0", Title: "Movie 0", Rating: 5},
			{Id: "1", Title: "Movie 1", Rating: 3},
		}

		tasks := mapper.eta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], ETA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][ETA_STAGE_2], 1) // ReduceCount is 1
		resultData := tasks[REDUCE_EXCHANGE][ETA_STAGE_2]["0"].GetEta_2().GetData()
		assert.Len(t, resultData, 2) // Should contain 2 movies
	})

	t.Run("Test Eta1Stage with multiple reducers", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Eta_1_Data{
			{Id: "0", Title: "Movie 0", Rating: 4},
			{Id: "0", Title: "Movie 0", Rating: 5},
			{Id: "1", Title: "Movie 1", Rating: 3},
		}

		tasks := mapper.eta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], ETA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][ETA_STAGE_2], 2) // ReduceCount is 2

		result0Data := tasks[REDUCE_EXCHANGE][ETA_STAGE_2]["0"].GetEta_2().GetData()
		result1Data := tasks[REDUCE_EXCHANGE][ETA_STAGE_2]["1"].GetEta_2().GetData()
		assert.Len(t, result0Data, 1) // Should contain 1 movie
		assert.Len(t, result1Data, 1) // Should contain 1 movie
	})

	t.Run("Test Eta1Stage with empty data", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Eta_1_Data{}

		tasks := mapper.eta1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], ETA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][ETA_STAGE_2], 0) // Empty data should return no tasks
	})
}

func TestKappa1Stage(t *testing.T) {
	REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

	t.Run("Test Kappa1Stage with one reducer", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 1,
			},
			testInfraConfig.GetRabbit(),
		)

		mapper := NewMapper(infra)

		data := []*protocol.Kappa_1_Data{
			{ActorId: "0", ActorName: "Actor 0"},
			{ActorId: "0", ActorName: "Actor 0"},
			{ActorId: "1", ActorName: "Actor 1"},
		}

		tasks := mapper.kappa1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], KAPPA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2], 1) // ReduceCount is 1
		resultData := tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2]["0"].GetKappa_2().GetData()
		assert.Len(t, resultData, 2) // Should contain 2 actors

	})

	t.Run("Test Kappa1Stage with multiple reducers", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Kappa_1_Data{
			{ActorId: "0", ActorName: "Actor 0"},
			{ActorId: "0", ActorName: "Actor 0"},
			{ActorId: "1", ActorName: "Actor 1"},
		}

		tasks := mapper.kappa1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], KAPPA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2], 2) // ReduceCount is 2
		result0Data := tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2]["0"].GetKappa_2().GetData()
		result1Data := tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2]["1"].GetKappa_2().GetData()
		assert.Len(t, result0Data, 1) // Should contain 1 actor
		assert.Len(t, result1Data, 1) // Should contain 1 actor
	})

	t.Run("Test Kappa1Stage with empty data", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Kappa_1_Data{}

		tasks := mapper.kappa1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], KAPPA_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][KAPPA_STAGE_2], 0) // Empty data should return no tasks
	})
}

func TestNu1Stage(t *testing.T) {
	REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

	t.Run("Test Nu1Stage with one reducer", func(t *testing.T) {
		infra := model.NewInfraConfig(
			&model.WorkerClusterConfig{
				ReduceCount: 1,
			},
			testInfraConfig.GetRabbit(),
		)
		mapper := NewMapper(infra)

		data := []*protocol.Nu_1_Data{
			{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
			{Id: "0", Sentiment: false, Revenue: 2000, Budget: 1000},
			{Id: "1", Sentiment: true, Revenue: 1500, Budget: 750},
		}

		tasks := mapper.nu1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], NU_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][NU_STAGE_2], 1) // ReduceCount is 1
		resultData := tasks[REDUCE_EXCHANGE][NU_STAGE_2]["0"].GetNu_2().GetData()
		assert.Len(t, resultData, 2) // Should contain 2 movies
	})

	t.Run("Test Nu1Stage with multiple reducers", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Nu_1_Data{
			{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
			{Id: "0", Sentiment: false, Revenue: 2000, Budget: 1000},
			{Id: "1", Sentiment: true, Revenue: 1500, Budget: 750},
		}

		tasks := mapper.nu1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], NU_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][NU_STAGE_2], 2) // ReduceCount is 2
		result0Data := tasks[REDUCE_EXCHANGE][NU_STAGE_2]["0"].GetNu_2().GetData()
		result1Data := tasks[REDUCE_EXCHANGE][NU_STAGE_2]["1"].GetNu_2().GetData()
		assert.Len(t, result0Data, 2) // Should contain 2 movies
		assert.Len(t, result1Data, 2) // Should contain 2 movies
	})

	t.Run("Test Nu1Stage with empty data", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		data := []*protocol.Nu_1_Data{}

		tasks := mapper.nu1Stage(data)

		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], NU_STAGE_2)
		assert.Len(t, tasks[REDUCE_EXCHANGE][NU_STAGE_2], 0) // Empty data should return no tasks
	})
}

func TestExecuteMapper(t *testing.T) {
	REDUCE_EXCHANGE := testInfraConfig.GetReduceExchange()

	t.Run("Test ExecuteMapper with Delta_1 stage", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		task := &protocol.Task{
			Stage: &protocol.Task_Delta_1{
				Delta_1: &protocol.Delta_1{
					Data: []*protocol.Delta_1_Data{
						{Country: "USA", Budget: 100},
						{Country: "UK", Budget: 200},
					},
				},
			},
		}

		tasks, err := mapper.Execute(task)

		assert.NoError(t, err)
		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], DELTA_STAGE_2)
	})

	t.Run("Test ExecuteMapper with Eta_1 stage", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		task := &protocol.Task{
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: []*protocol.Eta_1_Data{
						{Id: "0", Title: "Movie 0", Rating: 4},
						{Id: "1", Title: "Movie 1", Rating: 5},
					},
				},
			},
		}

		tasks, err := mapper.Execute(task)

		assert.NoError(t, err)
		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], ETA_STAGE_2)
	})

	t.Run("Test ExecuteMapper with Kappa_1 stage", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		task := &protocol.Task{
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: []*protocol.Kappa_1_Data{
						{ActorId: "0", ActorName: "Actor 0"},
						{ActorId: "1", ActorName: "Actor 1"},
					},
				},
			},
		}

		tasks, err := mapper.Execute(task)

		assert.NoError(t, err)
		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], KAPPA_STAGE_2)
	})

	t.Run("Test ExecuteMapper with Nu_1 stage", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		task := &protocol.Task{
			Stage: &protocol.Task_Nu_1{
				Nu_1: &protocol.Nu_1{
					Data: []*protocol.Nu_1_Data{
						{Id: "0", Sentiment: true, Revenue: 1000, Budget: 500},
						{Id: "1", Sentiment: false, Revenue: 2000, Budget: 1000},
					},
				},
			},
		}

		tasks, err := mapper.Execute(task)

		assert.NoError(t, err)
		assert.Contains(t, tasks, REDUCE_EXCHANGE)
		assert.Contains(t, tasks[REDUCE_EXCHANGE], NU_STAGE_2)
	})

	t.Run("Test ExecuteMapper with unknown stage", func(t *testing.T) {
		mapper := NewMapper(testInfraConfig)

		task := &protocol.Task{}

		tasks, err := mapper.Execute(task)

		assert.Error(t, err)
		assert.Nil(t, tasks)
	})
}
