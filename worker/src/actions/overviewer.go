package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/cdipaolo/sentiment"
)

type Overviewer struct {
	model       sentiment.Models
	workerCount int
}

func NewOverviewer(workerCount int) *Overviewer {
	model, err := sentiment.Restore()
	if err != nil {
		log.Panicf("Failed to load sentiment model: %s", err)
	}

	return &Overviewer{
		model:       model,
		workerCount: workerCount,
	}
}

func (o *Overviewer) muStage(data []*protocol.Mu_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][NU_STAGE] = make(map[string]*protocol.Task)
	nuData := make(map[string][]*protocol.Nu_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		analysis := o.model.SentimentAnalysis(movie.GetOverview(), sentiment.English)

		// true: POSITIVE
		// false: NEGATIVE
		nuData[BROADCAST_ID] = append(nuData[BROADCAST_ID], &protocol.Nu_Data{
			Id:        movie.GetId(),
			Title:     movie.GetTitle(),
			Revenue:   movie.GetRevenue(),
			Budget:    movie.GetBudget(),
			Sentiment: analysis.Score == 1,
		})
	}

	for id, data := range nuData {
		tasks[MAP_EXCHANGE][NU_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Nu{
				Nu: &protocol.Nu{
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
