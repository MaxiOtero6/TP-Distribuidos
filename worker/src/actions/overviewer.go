package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
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
	MAP_EXCHANGE := o.infraConfig.GetMapExchange()
	MAP_COUNT := o.infraConfig.GetMapCount()

	tasks = make(Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][NU_STAGE_1] = make(map[string]*protocol.Task)
	nuData := make(map[string][]*protocol.Nu_1_Data)

	for _, movie := range data {
		if movie == nil {
			continue
		}

		analysis := o.model.SentimentAnalysis(movie.GetOverview(), sentiment.English)

		mapIdHash := utils.GetWorkerIdFromHash(MAP_COUNT, movie.GetId())

		// true: POSITIVE
		// false: NEGATIVE
		nuData[mapIdHash] = append(nuData[mapIdHash], &protocol.Nu_1_Data{
			Id:        movie.GetId(),
			Title:     movie.GetTitle(),
			Revenue:   movie.GetRevenue(),
			Budget:    movie.GetBudget(),
			Sentiment: analysis.Score == 1,
		})
	}

	for nodeId, data := range nuData {
		tasks[MAP_EXCHANGE][NU_STAGE_1][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Nu_1{
				Nu_1: &protocol.Nu_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (o *Overviewer) getNextNodeId(nodeId string) (string, error) {
	currentNodeId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert currentNodeId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (currentNodeId+1)%o.infraConfig.GetOverviewCount())
	return nextNodeId, nil
}

func (o *Overviewer) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case MU_STAGE:
		return NU_STAGE_1, o.infraConfig.GetMapExchange(), o.infraConfig.GetMapCount(), nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (o *Overviewer) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == o.infraConfig.GetNodeId() {
		nextStage, nextExchange, nextStageCount, err := o.getNextStageData(data.GetStage())
		if err != nil {
			log.Errorf("Failed to get next stage data: %s", err)
			return nil
		}

		nextStageEOF := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						ClientId:        data.GetClientId(),
						WorkerCreatorId: "",
						Stage:           nextStage,
					},
				},
			},
		}

		randomNode := utils.RandomHash(nextStageCount)

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStage][randomNode] = nextStageEOF

	} else { // if the creator is not the same as the worker, send EOF to the next node
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = o.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := o.getNextNodeId(o.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		overviewExchange := o.infraConfig.GetOverviewExchange()
		stage := data.GetStage()

		tasks[overviewExchange] = make(map[string]map[string]*protocol.Task)
		tasks[overviewExchange][stage] = make(map[string]*protocol.Task)
		tasks[overviewExchange][stage][nextNode] = eofTask

	}
	return tasks
}

func (o *Overviewer) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Mu:
		data := v.Mu.GetData()
		return o.muStage(data), nil
	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return o.omegaEOFStage(data), nil
	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
