package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/cdipaolo/sentiment"
)

type Overviewer struct {
	model sentiment.Models
}

func NewOverviewer() (*Overviewer, error) {
	model, err := sentiment.Restore()
	if err != nil {
		return nil, err
	}

	return &Overviewer{
		model: model,
	}, nil
}

func (o *Overviewer) muStage(data []*protocol.Mu_Data) map[string]*protocol.Task {
	var nuData []*protocol.Nu_Data

	for _, movie := range data {
		if movie == nil {
			continue
		}

		analysis := o.model.SentimentAnalysis(movie.GetOverview(), sentiment.English)

		// true: POSITIVE
		// false: NEGATIVE
		nuData = append(nuData, &protocol.Nu_Data{
			Id:        movie.GetId(),
			Title:     movie.GetTitle(),
			Revenue:   movie.GetRevenue(),
			Budget:    movie.GetBudget(),
			Sentiment: analysis.Score == 1,
		})
	}

	return map[string]*protocol.Task{
		"nu": {
			Stage: &protocol.Task_Nu{
				Nu: &protocol.Nu{
					Data: nuData,
				},
			},
		},
	}
}

func (o *Overviewer) Execute(task *protocol.Task) (map[string]*protocol.Task, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Mu:
		data := v.Mu.GetData()
		return o.muStage(data), nil
	default:
		return nil, fmt.Errorf("invalid query stage")
	}
}
