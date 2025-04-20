package actions

import (
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
)

var testOverviewer = NewOverviewer(testInfraConfig)

func TestMuStage(t *testing.T) {
	t.Run("Process one valid Mu_Data", func(t *testing.T) {
		data := []*protocol.Mu_Data{
			{
				Id:       "1",
				Title:    "Movie 1",
				Revenue:  1000,
				Budget:   500,
				Overview: "Great!!! Great movie",
			},
		}

		result := testOverviewer.muStage(data)
		assert.NotNil(t, result)
		assert.Contains(t, result, testOverviewer.infraConfig.GetMapExchange())
		assert.Contains(t, result[testOverviewer.infraConfig.GetMapExchange()], NU_STAGE_1)
		assert.Len(t, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData(), 1)

		assert.Equal(t, "1", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetId())
		assert.Equal(t, "Movie 1", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetTitle())
		assert.Equal(t, uint64(1000), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetRevenue())
		assert.Equal(t, uint64(500), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetBudget())
		assert.Equal(t, true, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetSentiment())
	})

	t.Run("Process many valid Mu_Data", func(t *testing.T) {
		data := []*protocol.Mu_Data{
			{
				Id:       "1",
				Title:    "Movie 1",
				Revenue:  1000,
				Budget:   500,
				Overview: "Great!!! Great movie",
			},
			{
				Id:       "2",
				Title:    "Movie 2",
				Revenue:  1100,
				Budget:   600,
				Overview: "Bad bad bad bad!!!",
			},
		}

		result := testOverviewer.muStage(data)
		assert.NotNil(t, result)
		assert.Contains(t, result, testOverviewer.infraConfig.GetMapExchange())
		assert.Contains(t, result[testOverviewer.infraConfig.GetMapExchange()], NU_STAGE_1)
		assert.Len(t, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData(), 2)

		assert.Equal(t, "1", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetId())
		assert.Equal(t, "Movie 1", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetTitle())
		assert.Equal(t, uint64(1000), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetRevenue())
		assert.Equal(t, uint64(500), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetBudget())
		assert.Equal(t, true, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[0].GetSentiment())

		assert.Equal(t, "2", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[1].GetId())
		assert.Equal(t, "Movie 2", result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[1].GetTitle())
		assert.Equal(t, uint64(1100), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[1].GetRevenue())
		assert.Equal(t, uint64(600), result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[1].GetBudget())
		assert.Equal(t, false, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData()[1].GetSentiment())
	})

	t.Run("Handle nil Mu_Data", func(t *testing.T) {
		data := []*protocol.Mu_Data{nil}

		result := testOverviewer.muStage(data)
		assert.NotNil(t, result)
		assert.Contains(t, result, testOverviewer.infraConfig.GetMapExchange())
		assert.Contains(t, result[testOverviewer.infraConfig.GetMapExchange()], NU_STAGE_1)
		assert.Len(t, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData(), 0)
	})

	t.Run("Handle empty Mu_Data", func(t *testing.T) {
		data := []*protocol.Mu_Data{}

		result := testOverviewer.muStage(data)
		assert.NotNil(t, result)
		assert.Contains(t, result, testOverviewer.infraConfig.GetMapExchange())
		assert.Contains(t, result[testOverviewer.infraConfig.GetMapExchange()], NU_STAGE_1)
		assert.Len(t, result[testOverviewer.infraConfig.GetMapExchange()][NU_STAGE_1][testOverviewer.infraConfig.GetBroadcastID()].GetNu_1().GetData(), 0)
	})
}

func TestExecuteOverview(t *testing.T) {
	t.Run("Valid Mu stage", func(t *testing.T) {
		task := &protocol.Task{
			Stage: &protocol.Task_Mu{
				Mu: &protocol.Mu{
					Data: []*protocol.Mu_Data{
						{
							Id:       "1",
							Title:    "Movie 1",
							Revenue:  1000,
							Budget:   500,
							Overview: "Great movie",
						},
					},
				},
			},
		}

		result, err := testOverviewer.Execute(task)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Contains(t, result, testOverviewer.infraConfig.GetMapExchange())
		assert.Contains(t, result[testOverviewer.infraConfig.GetMapExchange()], NU_STAGE_1)
	})

	t.Run("Invalid stage type", func(t *testing.T) {
		task := &protocol.Task{
			Stage: &protocol.Task_Nu_1{},
		}

		result, err := testOverviewer.Execute(task)
		assert.Error(t, err)
		assert.Nil(t, result)
	})
}
