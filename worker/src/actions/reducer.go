package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

type PartialResults struct {
	delta3Data  map[string]*protocol.Delta_3_Data
	epsilonData map[string]*protocol.Epsilon_Data
	nu3Data     map[string]*protocol.Nu_3_Data
	result5Data map[string]*protocol.Result5_Data
}

// Reducer is a struct that implements the Action interface.
type Reducer struct {
	infraConfig    *model.InfraConfig
	partialResults *PartialResults
}

// NewReduce creates a new Reduce instance.
// It initializes the worker count and returns a pointer to the Reduce struct.
func NewReducer(infraConfig *model.InfraConfig) *Reducer {
	return &Reducer{
		infraConfig: infraConfig,
		partialResults: &PartialResults{
			epsilonData: make(map[string]*protocol.Epsilon_Data),
		},
	}
}

/*
delta2Stage partially sum up investment by country

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Then it divides the resulting countries by hashing each country and send it to the corresponding worker to finish the reduction.

# Return example

	{
		"topExchange": {
			"delta_3": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) delta2Stage(data []*protocol.Delta_2_Data) (tasks Tasks) {
	dataMap := r.partialResults.delta3Data

	// Sum up the partial budgets by country
	for _, country := range data {
		prodCountry := country.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_3_Data{
				Country:       prodCountry,
				PartialBudget: 0,
			}
		}

		dataMap[prodCountry].PartialBudget += country.GetPartialBudget()
	}

	return nil
}

/*
delta3Stage reduce the total investment by country

This function is nil-safe, meaning it will not panic if the input is nil.

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
func (r *Reducer) delta3Stage(data []*protocol.Delta_3_Data) (tasks Tasks) {
	dataMap := r.partialResults.epsilonData

	// Sum up the partial budgets by country
	for _, country := range data {
		prodCountry := country.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Epsilon_Data{
				ProdCountry:     prodCountry,
				TotalInvestment: 0,
			}
		}

		dataMap[prodCountry].TotalInvestment += country.GetPartialBudget()
	}

	return nil
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
	TOP_EXCHANGE := r.infraConfig.GetTopExchange()

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
 */
func (r *Reducer) eta3Stage(data []*protocol.Eta_3_Data) (tasks Tasks) {
	return nil
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
	TOP_EXCHANGE := r.infraConfig.GetTopExchange()

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
 */
func (r *Reducer) kappa3Stage(data []*protocol.Kappa_3_Data) (tasks Tasks) {
	return nil
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
	dataMap := r.partialResults.nu3Data

	// Sum up the budget and revenue by sentiment
	for _, nu2Data := range data {
		sentiment := fmt.Sprintf("%t", nu2Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_3_Data{
				Sentiment: nu2Data.GetSentiment(),
				Ratio:     0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += nu2Data.GetRatio()
		dataMap[sentiment].Count += nu2Data.GetCount()
	}

	return nil
}

/*
 */
func (r *Reducer) nu3Stage(data []*protocol.Nu_3_Data) (tasks Tasks) {
	dataMap := r.partialResults.result5Data

	// Sum up the budget and revenue by sentiment
	for _, nu3Data := range data {
		sentiment := fmt.Sprintf("%t", nu3Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Result5_Data{
				Sentiment: nu3Data.GetSentiment(),
				Ratio:     0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += nu3Data.GetRatio()
		dataMap[sentiment].Count += nu3Data.GetCount()
	}

	return nil
}

/*
 */
func (r *Reducer) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {

	tasks = make(Tasks)

	// if the creator is the same as the worker, return the TASK for the next stage
	if data.GetWorkerCreatorId() == r.infraConfig.GetNodeId() {
		nextStageEOF := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						ClientId:        data.GetClientId(),
						WorkerCreatorId: "",
					},
				},
			},
		}

		var nextExchange string
		var nextStageCount int

		switch data.GetStage() {
		case DELTA_STAGE_2:
			nextStageEOF.GetOmegaEOF().Data.Stage = DELTA_STAGE_3
			nextExchange = r.infraConfig.GetReduceExchange()
			nextStageCount = r.infraConfig.GetReduceCount()
		case DELTA_STAGE_3:
			nextStageEOF.GetOmegaEOF().Data.Stage = EPSILON_STAGE
			nextExchange = r.infraConfig.GetTopExchange()
			nextStageCount = r.infraConfig.GetTopCount()
		case ETA_STAGE_2:
			nextStageEOF.GetOmegaEOF().Data.Stage = ETA_STAGE_3
			nextExchange = r.infraConfig.GetReduceExchange()
			nextStageCount = r.infraConfig.GetReduceCount()
		case ETA_STAGE_3:
			nextStageEOF.GetOmegaEOF().Data.Stage = THETA_STAGE
			nextExchange = r.infraConfig.GetTopExchange()
			nextStageCount = r.infraConfig.GetTopCount()
		case KAPPA_STAGE_2:
			nextStageEOF.GetOmegaEOF().Data.Stage = KAPPA_STAGE_3
			nextExchange = r.infraConfig.GetReduceExchange()
			nextStageCount = r.infraConfig.GetReduceCount()
		case KAPPA_STAGE_3:
			nextStageEOF.GetOmegaEOF().Data.Stage = LAMBDA_STAGE
			nextExchange = r.infraConfig.GetTopExchange()
			nextStageCount = r.infraConfig.GetTopCount()
		case NU_STAGE_2:
			nextStageEOF.GetOmegaEOF().Data.Stage = NU_STAGE_3
			nextExchange = r.infraConfig.GetReduceExchange()
			nextStageCount = r.infraConfig.GetReduceCount()
		case NU_STAGE_3:
			nextStageEOF.GetOmegaEOF().Data.Stage = RESULT_STAGE
			nextExchange = r.infraConfig.GetResultExchange()
			// TODO: check if this is correct
			nextStageCount = 0
		default:
			log.Errorf("Invalid stage: %s", data.GetStage())
			return nil
		}

		id := utils.RandomHash(nextStageCount)

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStageEOF.GetOmegaEOF().Data.Stage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStageEOF.GetOmegaEOF().Data.Stage][id] = nextStageEOF
	} else {
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = r.infraConfig.GetNodeId()
		}

		clientId, err := strconv.Atoi(r.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to convert clientId to int: %s", err)
			return nil
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNodeId := fmt.Sprintf("%d", (clientId+1)%r.infraConfig.GetReduceCount())

		tasks[r.infraConfig.GetReduceExchange()] = make(map[string]map[string]*protocol.Task)
		tasks[r.infraConfig.GetReduceExchange()][data.GetStage()] = make(map[string]*protocol.Task)
		tasks[r.infraConfig.GetReduceExchange()][data.GetStage()][nextNodeId] = eofTask

		// send the results
		switch data.GetStage() {
		case DELTA_STAGE_2:
			r.delta2Results(tasks)
		case DELTA_STAGE_3:
			r.delta3Results(tasks)
		case ETA_STAGE_2:
			return nil
		case ETA_STAGE_3:
			return nil
		case KAPPA_STAGE_2:
			return nil
		case KAPPA_STAGE_3:
			return nil
		case NU_STAGE_2:
			return nil
		case NU_STAGE_3:
			return nil
		default:
			return nil
		}
	}

	return tasks
}

func (r *Reducer) delta2Results(tasks Tasks) {
	dataMap := r.partialResults.delta3Data

	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][DELTA_STAGE_3] = make(map[string]*protocol.Task)
	delta3Data := make(map[string][]*protocol.Delta_3_Data)

	// Divide the resulting countries by hashing each country
	for _, d3Data := range dataMap {
		idHash := utils.GetWorkerIdFromHash(r.infraConfig.GetReduceCount(), d3Data.GetCountry())
		delta3Data[idHash] = append(delta3Data[idHash], &protocol.Delta_3_Data{
			Country:       d3Data.GetCountry(),
			PartialBudget: d3Data.GetPartialBudget(),
		})
	}

	// Create tasks for each worker
	for id, data := range delta3Data {
		tasks[REDUCE_EXCHANGE][DELTA_STAGE_3][id] = &protocol.Task{
			Stage: &protocol.Task_Delta_3{
				Delta_3: &protocol.Delta_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) delta3Results(tasks Tasks) {
	dataMap := r.partialResults.epsilonData

	TOP_EXCHANGE := r.infraConfig.GetTopExchange()
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][EPSILON_STAGE] = make(map[string]*protocol.Task)
	epsilonData := make(map[string][]*protocol.Epsilon_Data)

	// Asign the data to the corresponding worker
	routingKey := utils.GetWorkerIdFromHash(r.infraConfig.GetTopCount(), EPSILON_STAGE)

	for _, eData := range dataMap {
		epsilonData[routingKey] = append(epsilonData[routingKey], &protocol.Epsilon_Data{
			ProdCountry:     eData.GetProdCountry(),
			TotalInvestment: eData.GetTotalInvestment(),
		})
	}

	// Create tasks for each worker
	for id, data := range epsilonData {
		tasks[TOP_EXCHANGE][EPSILON_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) nu2Results(tasks Tasks) {
	dataMap := r.partialResults.nu3Data

	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][NU_STAGE_3] = make(map[string]*protocol.Task)
	delta3Data := make(map[string][]*protocol.Nu_3_Data)

	// Divide the resulting sentiments by hashing each sentiment
	for _, n3Data := range dataMap {
		sentiment := fmt.Sprintf("%t", n3Data.GetSentiment())
		idHash := utils.GetWorkerIdFromHash(r.infraConfig.GetReduceCount(), sentiment)
		delta3Data[idHash] = append(delta3Data[idHash], &protocol.Nu_3_Data{
			Sentiment: n3Data.GetSentiment(),
			Ratio:     n3Data.GetRatio(),
			Count:     n3Data.GetCount(),
		})
	}

	// Create tasks for each worker
	for id, data := range delta3Data {
		tasks[REDUCE_EXCHANGE][NU_STAGE_3][id] = &protocol.Task{
			Stage: &protocol.Task_Nu_3{
				Nu_3: &protocol.Nu_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) nu3Results(tasks Tasks) {
	dataMap := r.partialResults.nu3Data

	RESULT_EXCHANGE := r.infraConfig.GetResultExchange()
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	resultData := make(map[string][]*protocol.Result5_Data)

	// Asign the data to the corresponding worker
	// TODO: check if this is corrects
	routingKey := "0"

	for _, n3Data := range dataMap {
		ratio := n3Data.GetRatio() / float32(n3Data.GetCount())
		resultData[routingKey] = append(resultData[routingKey], &protocol.Result5_Data{
			Sentiment: n3Data.GetSentiment(),
			Ratio:     ratio,
		})
	}

	// Create tasks for each worker
	for id, data := range resultData {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Delta_2:
		data := v.Delta_2.GetData()
		return r.delta2Stage(data), nil

	case *protocol.Task_Delta_3:
		data := v.Delta_3.GetData()
		return r.delta3Stage(data), nil

	case *protocol.Task_Eta_2:
		data := v.Eta_2.GetData()
		return r.eta2Stage(data), nil

	case *protocol.Task_Eta_3:
		data := v.Eta_3.GetData()
		return r.eta3Stage(data), nil

	case *protocol.Task_Kappa_2:
		data := v.Kappa_2.GetData()
		return r.kappa2Stage(data), nil

	case *protocol.Task_Kappa_3:
		data := v.Kappa_3.GetData()
		return r.kappa3Stage(data), nil

	case *protocol.Task_Nu_2:
		data := v.Nu_2.GetData()
		return r.nu2Stage(data), nil

	case *protocol.Task_Nu_3:
		data := v.Nu_3.GetData()
		return r.nu3Stage(data), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return r.omegaEOFStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
