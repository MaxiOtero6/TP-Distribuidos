package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
)

// Topper is a struct that implements the Action interface.
type Topper struct {
	infraConfig *model.InfraConfig
}

// NewTopper creates a new Topper instance.
// It initializes the worker count and returns a pointer to the Topper struct.
func NewTopper(infraConfig *model.InfraConfig) *Topper {
	return &Topper{
		infraConfig: infraConfig,
	}
}

/*
epsilonStage get the top 5 countries that invested the most in movies in desc order

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"": Task,
			}
		},
	}
*/
func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data) (tasks Tasks) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result2Data := make(map[string][]*protocol.Result2_Data)

	log.Panicf("Topper: Epsilon stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range result2Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Result2{
				Result2: &protocol.Result2{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
thetaStage get one of the best and one of the worst rated movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"": Task
			}
		},
	}
*/
func (t *Topper) thetaStage(data []*protocol.Theta_Data) (tasks Tasks) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result3Data := make(map[string][]*protocol.Result3_Data)

	log.Panicf("Topper: Theta stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range result3Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Result3{
				Result3: &protocol.Result3{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
lambdaStage get the top 10 actors that participated in the most movies in desc order

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"": Task
			}
		},
	}
*/
func (t *Topper) lambdaStage(data []*protocol.Lambda_Data) (tasks Tasks) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result4Data := make(map[string][]*protocol.Result4_Data)

	log.Panicf("Topper: Lambda stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range result4Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Result4{
				Result4: &protocol.Result4{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (t *Topper) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Epsilon:
		data := v.Epsilon.GetData()
		return t.epsilonStage(data), nil

	case *protocol.Task_Theta:
		data := v.Theta.GetData()
		return t.thetaStage(data), nil

	case *protocol.Task_Lambda:
		data := v.Lambda.GetData()
		return t.lambdaStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
