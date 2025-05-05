package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
	"github.com/cdipaolo/sentiment"
)

// Overviewer is a struct that implements the Action interface.
type Overviewer struct {
	model          sentiment.Models
	infraConfig    *model.InfraConfig
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     eof_handler.IEOFHandler
}

// NewOverviewer creates a new Overviewer instance.
// It loads the sentiment model and initializes the worker count.
// If the model fails to load, it panics with an error message.
func NewOverviewer(infraConfig *model.InfraConfig, eofHandler eof_handler.IEOFHandler) *Overviewer {
	model, err := sentiment.Restore()
	if err != nil {
		log.Panicf("Failed to load sentiment model: %s", err)
	}

	return &Overviewer{
		model:          model,
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
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
func (o *Overviewer) muStage(data []*protocol.Mu_Data, clientId string) (tasks common.Tasks) {
	MAP_EXCHANGE := o.infraConfig.GetMapExchange()

	tasks = make(common.Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][common.NU_STAGE_1] = make(map[string]*protocol.Task)
	nuData := make(map[string][]*protocol.Nu_1_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		if movie.GetBudget() == 0 || movie.GetRevenue() == 0 {
			continue
		}

		analysis := o.model.SentimentAnalysis(movie.GetOverview(), sentiment.English)
		// mapIdHash := o.itemHashFunc(MAP_COUNT, movie.GetId())

		// true: POSITIVE
		// false: NEGATIVE
		nuData[""] = append(nuData[""], &protocol.Nu_1_Data{
			Id:        movie.GetId(),
			Title:     movie.GetTitle(),
			Revenue:   movie.GetRevenue(),
			Budget:    movie.GetBudget(),
			Sentiment: analysis.Score == 1,
		})
	}

	for nodeId, data := range nuData {
		tasks[MAP_EXCHANGE][common.NU_STAGE_1][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_1{
				Nu_1: &protocol.Nu_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (o *Overviewer) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	switch stage {
	case common.MU_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_1,
				Exchange:    o.infraConfig.GetMapExchange(),
				WorkerCount: o.infraConfig.GetMapCount(),
				RoutingKey:  o.infraConfig.GetBroadcastID(),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (o *Overviewer) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	return o.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)
}

func (o *Overviewer) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	// For overviewers eofStatus is always true
	// because one of them receives the EOF and init the ring
	// and the others just declare that they are alive
	return o.eofHandler.HandleRing(data, clientId, o.getNextStageData, true)
}

func (o *Overviewer) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	switch v := stage.(type) {
	case *protocol.Task_Mu:
		data := v.Mu.GetData()
		return o.muStage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return o.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return o.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
