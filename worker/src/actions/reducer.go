package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
)

// Reducer is a struct that implements the Action interface.
type Reducer struct {
	workerCount int
}

// NewReduce creates a new Reduce instance.
// It initializes the worker count and returns a pointer to the Reduce struct.
func NewReducer(workerCount int) *Reducer {
	return &Reducer{
		workerCount: workerCount,
	}
}

/*
delta2Stage partially sum up investment by country

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"topExchange": {
			"epsilon": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) delta2Stage(data []*protocol.Delta_2_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][EPSILON_STAGE] = make(map[string]*protocol.Task)
	epsilonData := make(map[string][]*protocol.Epsilon_Data)

	log.Panicf("Reduce: Delta_2 stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range epsilonData {
		tasks[TOP_EXCHANGE][EPSILON_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
eta2Stage calculates average rating for each movie

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"topExchange": {
			"theta": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) eta2Stage(data []*protocol.Eta_2_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][THETA_STAGE] = make(map[string]*protocol.Task)
	thetaData := make(map[string][]*protocol.Theta_Data)

	log.Panicf("Reduce: Eta_2 stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range thetaData {
		tasks[TOP_EXCHANGE][THETA_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
kappa2Stage reduce into one, partials actors participations in movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"topExchange": {
			"lambda": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) kappa2Stage(data []*protocol.Kappa_2_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][LAMBDA_STAGE] = make(map[string]*protocol.Task)
	lambdaData := make(map[string][]*protocol.Lambda_Data)

	log.Panicf("Reduce: Kappa_2 stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range lambdaData {
		tasks[TOP_EXCHANGE][LAMBDA_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: data,
				},
			},
		}
	}

	return tasks
}

/*
nu2Stage reduce into one, partials revenue and budget from movies by sentiment.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"" : Task
			}
		},
	}
*/
func (r *Reducer) nu2Stage(data []*protocol.Nu_2_Data) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result5Data := make(map[string][]*protocol.Result5_Data)

	log.Panicf("Reduce: Nu_2 stage not implemented yet %v", data)

	// TODO: process data
	// TODO: see filter.go or overviewer.go for examples
	// for _, movie := range data {

	// }

	for id, data := range result5Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
		}
	}

	return tasks
}

func (r *Reducer) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Delta_2:
		data := v.Delta_2.GetData()
		return r.delta2Stage(data), nil

	case *protocol.Task_Eta_2:
		data := v.Eta_2.GetData()
		return r.eta2Stage(data), nil

	case *protocol.Task_Kappa_2:
		data := v.Kappa_2.GetData()
		return r.kappa2Stage(data), nil

	case *protocol.Task_Nu_2:
		data := v.Nu_2.GetData()
		return r.nu2Stage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
