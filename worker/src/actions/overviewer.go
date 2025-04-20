package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/cdipaolo/sentiment"
)

// Overviewer is a struct that implements the Action interface.
type Overviewer struct {
	model       sentiment.Models
	infraConfig *model.InfraConfig
}

// NewOverviewer creates a new Overviewer instance.
// It loads the sentiment model and initializes the worker count.
// If the model fails to load, it panics with an error message.
func NewOverviewer(infraConfig *model.InfraConfig) *Overviewer {
	model, err := sentiment.Restore()
	if err != nil {
		log.Panicf("Failed to load sentiment model: %s", err)
	}

	return &Overviewer{
		model:       model,
		infraConfig: infraConfig,
	}
}

/*
muStage processes the input data and generates tasks for the next stage.
It analyzes the sentiment of the movie overview and creates Nu_1_Data tasks.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"nu": {
				"": Task
			}
		}
	}
*/
func (o *Overviewer) muStage(data []*protocol.Mu_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[o.infraConfig.GetMapExchange()] = make(map[string]map[string]*protocol.Task)
	tasks[o.infraConfig.GetMapExchange()][NU_STAGE_1] = make(map[string]*protocol.Task)
	nuData := make(map[string][]*protocol.Nu_1_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		analysis := o.model.SentimentAnalysis(movie.GetOverview(), sentiment.English)

		// true: POSITIVE
		// false: NEGATIVE
		nuData[o.infraConfig.GetBroadcastID()] = append(nuData[o.infraConfig.GetBroadcastID()], &protocol.Nu_1_Data{
			Id:        movie.GetId(),
			Title:     movie.GetTitle(),
			Revenue:   movie.GetRevenue(),
			Budget:    movie.GetBudget(),
			Sentiment: analysis.Score == 1,
		})
	}

	for id, data := range nuData {
		tasks[o.infraConfig.GetMapExchange()][NU_STAGE_1][id] = &protocol.Task{
			Stage: &protocol.Task_Nu_1{
				Nu_1: &protocol.Nu_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (o *Overviewer) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Mu:
		data := v.Mu.GetData()
		return o.muStage(data), nil
	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
