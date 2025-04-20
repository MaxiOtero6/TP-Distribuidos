package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
)

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	clusterConfig *model.WorkerClusterConfig
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(clusterConfig *model.WorkerClusterConfig) *Joiner {
	return &Joiner{
		clusterConfig: clusterConfig,
	}
}

/*
zetaStage joins movies and ratings by movieId:

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"eta_1": {
				"": Task
			}
		},
	}
*/
func (j *Joiner) zetaStage(data []*protocol.Zeta_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][ETA_STAGE_1] = make(map[string]*protocol.Task)
	eta1Data := make(map[string][]*protocol.Eta_1_Data)

	log.Panicf("Joiner: Zeta stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range eta1Data {
		tasks[MAP_EXCHANGE][ETA_STAGE_1][id] = &protocol.Task{
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
iotaStage joins movies and actors by movieId:

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"kappa_1": {
				"": Task
			}
		},
	}
*/
func (j *Joiner) iotaStage(data []*protocol.Iota_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[MAP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[MAP_EXCHANGE][KAPPA_STAGE_1] = make(map[string]*protocol.Task)
	kappa1Data := make(map[string][]*protocol.Kappa_1_Data)

	log.Panicf("Joiner: Iota stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range kappa1Data {
		tasks[MAP_EXCHANGE][KAPPA_STAGE_1][id] = &protocol.Task{
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (j *Joiner) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
