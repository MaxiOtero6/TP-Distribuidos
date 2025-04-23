package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

type PartialResults struct {
	delta2  map[string]*protocol.Delta_2_Data
	delta3  map[string]*protocol.Delta_3_Data
	eta2    map[string]*protocol.Eta_2_Data
	eta3    map[string]*protocol.Eta_3_Data
	kappa2  map[string]*protocol.Kappa_2_Data
	kappa3  map[string]*protocol.Kappa_3_Data
	nu2Data map[string]*protocol.Nu_2_Data
	nu3Data map[string]*protocol.Nu_3_Data
}

// Reducer is a struct that implements the Action interface.
type Reducer struct {
	infraConfig    *model.InfraConfig
	partialResults *PartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
}

// NewReduce creates a new Reduce instance.
// It initializes the worker count and returns a pointer to the Reduce struct.
func NewReducer(infraConfig *model.InfraConfig) *Reducer {
	return &Reducer{
		infraConfig: infraConfig,
		partialResults: &PartialResults{
			delta2:  make(map[string]*protocol.Delta_2_Data),
			delta3:  make(map[string]*protocol.Delta_3_Data),
			eta2:    make(map[string]*protocol.Eta_2_Data),
			eta3:    make(map[string]*protocol.Eta_3_Data),
			kappa2:  make(map[string]*protocol.Kappa_2_Data),
			kappa3:  make(map[string]*protocol.Kappa_3_Data),
			nu2Data: make(map[string]*protocol.Nu_2_Data),
			nu3Data: make(map[string]*protocol.Nu_3_Data),
		},
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
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
	dataMap := r.partialResults.delta2

	// Sum up the partial budgets by country
	for _, country := range data {
		prodCountry := country.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_2_Data{
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
	dataMap := r.partialResults.delta3

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
	dataMap := r.partialResults.eta2

	// Sum up the partial ratings and counts for each movie
	for _, e2Data := range data {
		movieId := e2Data.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_2_Data{
				MovieId: movieId,
				Title:   e2Data.GetTitle(),
				Rating:  0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += e2Data.GetRating()
		dataMap[movieId].Count += 1
	}

	return nil
}

/*
 */
func (r *Reducer) eta3Stage(data []*protocol.Eta_3_Data) (tasks Tasks) {
	dataMap := r.partialResults.eta3

	// Sum up the partial ratings and counts for each movie
	for _, e3Data := range data {
		movieId := e3Data.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_3_Data{
				MovieId: movieId,
				Title:   e3Data.GetTitle(),
				Rating:  0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += e3Data.GetRating()
		dataMap[movieId].Count += 1
	}

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
	dataMap := r.partialResults.kappa2

	// Sum up the partial participations by actor
	for _, k2Data := range data {
		actorId := k2Data.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_2_Data{
				ActorId:               actorId,
				ActorName:             k2Data.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += k2Data.GetPartialParticipations()
	}

	return nil
}

/*
 */
func (r *Reducer) kappa3Stage(data []*protocol.Kappa_3_Data) (tasks Tasks) {
	dataMap := r.partialResults.kappa3

	// Sum up the partial participations by actor
	for _, k3Data := range data {
		actorId := k3Data.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_3_Data{
				ActorId:               actorId,
				ActorName:             k3Data.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += k3Data.GetPartialParticipations()
	}

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
	dataMap := r.partialResults.nu2Data

	// Sum up the budget and revenue by sentiment
	for _, nu2Data := range data {
		sentiment := fmt.Sprintf("%t", nu2Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_2_Data{
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
	dataMap := r.partialResults.nu3Data

	// Sum up the budget and revenue by sentiment
	for _, nu3Data := range data {
		sentiment := fmt.Sprintf("%t", nu3Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_3_Data{
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

func (r *Reducer) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case DELTA_STAGE_2:
		return DELTA_STAGE_3, r.infraConfig.GetReduceExchange(), r.infraConfig.GetReduceCount(), nil
	case DELTA_STAGE_3:
		return EPSILON_STAGE, r.infraConfig.GetTopExchange(), r.infraConfig.GetTopCount(), nil
	case ETA_STAGE_2:
		return ETA_STAGE_3, r.infraConfig.GetReduceExchange(), r.infraConfig.GetReduceCount(), nil
	case ETA_STAGE_3:
		return THETA_STAGE, r.infraConfig.GetTopExchange(), r.infraConfig.GetTopCount(), nil
	case KAPPA_STAGE_2:
		return KAPPA_STAGE_3, r.infraConfig.GetReduceExchange(), r.infraConfig.GetReduceCount(), nil
	case KAPPA_STAGE_3:
		return LAMBDA_STAGE, r.infraConfig.GetTopExchange(), r.infraConfig.GetTopCount(), nil
	case NU_STAGE_2:
		return NU_STAGE_3, r.infraConfig.GetReduceExchange(), r.infraConfig.GetReduceCount(), nil
	case NU_STAGE_3:
		return RESULT_STAGE, r.infraConfig.GetResultExchange(), 1, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *Reducer) getNextNodeId(nodeId string) (string, error) {
	clientId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert clientId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (clientId+1)%r.infraConfig.GetReduceCount())
	return nextNodeId, nil
}

func (r *Reducer) delta2Results(tasks Tasks) {
	dataMap := r.partialResults.delta2

	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	REDUCE_COUNT := r.infraConfig.GetReduceCount()

	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][DELTA_STAGE_3] = make(map[string]*protocol.Task)
	delta3Data := make(map[string][]*protocol.Delta_3_Data)

	// Divide the resulting countries by hashing each country
	for _, d3Data := range dataMap {
		idHash := utils.GetWorkerIdFromHash(REDUCE_COUNT, d3Data.GetCountry())
		delta3Data[idHash] = append(delta3Data[idHash], &protocol.Delta_3_Data{
			Country:       d3Data.GetCountry(),
			PartialBudget: d3Data.GetPartialBudget(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range delta3Data {
		tasks[REDUCE_EXCHANGE][DELTA_STAGE_3][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Delta_3{
				Delta_3: &protocol.Delta_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) delta3Results(tasks Tasks) {
	dataMap := r.partialResults.delta3

	TOP_EXCHANGE := r.infraConfig.GetTopExchange()
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][EPSILON_STAGE] = make(map[string]*protocol.Task)
	epsilonData := make(map[string][]*protocol.Epsilon_Data)

	// Asign the data to the corresponding worker
	routingKey := utils.GetWorkerIdFromHash(r.infraConfig.GetTopCount(), EPSILON_STAGE)

	for _, eData := range dataMap {
		epsilonData[routingKey] = append(epsilonData[routingKey], &protocol.Epsilon_Data{
			ProdCountry:     eData.GetCountry(),
			TotalInvestment: eData.GetPartialBudget(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range epsilonData {
		tasks[TOP_EXCHANGE][EPSILON_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) eta2Results(tasks Tasks) {
	dataMap := r.partialResults.eta2

	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	REDUCE_COUNT := r.infraConfig.GetReduceCount()

	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][ETA_STAGE_3] = make(map[string]*protocol.Task)
	eta3Data := make(map[string][]*protocol.Eta_3_Data)

	// Divide the resulting movies by hashing each movie
	for _, e2Data := range dataMap {
		idHash := utils.GetWorkerIdFromHash(REDUCE_COUNT, e2Data.GetMovieId())
		eta3Data[idHash] = append(eta3Data[idHash], &protocol.Eta_3_Data{
			MovieId: e2Data.GetMovieId(),
			Title:   e2Data.GetTitle(),
			Rating:  e2Data.GetRating(),
			Count:   e2Data.GetCount(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range eta3Data {
		tasks[REDUCE_EXCHANGE][ETA_STAGE_3][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Eta_3{
				Eta_3: &protocol.Eta_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) eta3Results(tasks Tasks) {
	dataMap := r.partialResults.eta3

	TOP_EXCHANGE := r.infraConfig.GetTopExchange()
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][THETA_STAGE] = make(map[string]*protocol.Task)
	thetaData := make(map[string][]*protocol.Theta_Data)

	// Asign the data to the corresponding worker
	routingKey := utils.GetWorkerIdFromHash(r.infraConfig.GetTopCount(), THETA_STAGE)

	for _, e3Data := range dataMap {
		avgRating := float32(e3Data.GetRating()) / float32(e3Data.GetCount())
		thetaData[routingKey] = append(thetaData[routingKey], &protocol.Theta_Data{
			Id:        e3Data.GetMovieId(),
			Title:     e3Data.GetTitle(),
			AvgRating: avgRating,
		})
	}

	// Create tasks for each worker
	for nodeId, data := range thetaData {
		tasks[TOP_EXCHANGE][THETA_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) kappa2Results(tasks Tasks) {
	dataMap := r.partialResults.kappa2
	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	REDUCE_COUNT := r.infraConfig.GetReduceCount()

	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][KAPPA_STAGE_3] = make(map[string]*protocol.Task)
	kappa3Data := make(map[string][]*protocol.Kappa_3_Data)

	// Divide the resulting actors by hashing each actor
	for _, k2Data := range dataMap {
		idHash := utils.GetWorkerIdFromHash(REDUCE_COUNT, k2Data.GetActorId())
		kappa3Data[idHash] = append(kappa3Data[idHash], &protocol.Kappa_3_Data{
			ActorId:               k2Data.GetActorId(),
			ActorName:             k2Data.GetActorName(),
			PartialParticipations: k2Data.GetPartialParticipations(),
		})
	}
	// Create tasks for each worker
	for nodeId, data := range kappa3Data {
		tasks[REDUCE_EXCHANGE][KAPPA_STAGE_3][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Kappa_3{
				Kappa_3: &protocol.Kappa_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) kappa3Results(tasks Tasks) {
	dataMap := r.partialResults.kappa3
	TOP_EXCHANGE := r.infraConfig.GetTopExchange()
	tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[TOP_EXCHANGE][LAMBDA_STAGE] = make(map[string]*protocol.Task)
	lambdaData := make(map[string][]*protocol.Lambda_Data)
	// Asign the data to the corresponding worker
	nodeId := utils.GetWorkerIdFromHash(r.infraConfig.GetTopCount(), LAMBDA_STAGE)
	// Divide the resulting actors by hashing each actor
	for _, k3Data := range dataMap {
		participations := k3Data.GetPartialParticipations()
		lambdaData[nodeId] = append(lambdaData[nodeId], &protocol.Lambda_Data{
			ActorId:        k3Data.GetActorId(),
			ActorName:      k3Data.GetActorName(),
			Participations: participations,
		})
	}
	// Create tasks for each worker
	for nodeId, data := range lambdaData {
		tasks[TOP_EXCHANGE][LAMBDA_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) nu2Results(tasks Tasks) {
	dataMap := r.partialResults.nu3Data

	REDUCE_EXCHANGE := r.infraConfig.GetReduceExchange()
	REDUCE_COUNT := r.infraConfig.GetReduceCount()

	tasks[REDUCE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[REDUCE_EXCHANGE][NU_STAGE_3] = make(map[string]*protocol.Task)
	delta3Data := make(map[string][]*protocol.Nu_3_Data)

	// Divide the resulting sentiments by hashing each sentiment
	for _, n3Data := range dataMap {
		sentiment := fmt.Sprintf("%t", n3Data.GetSentiment())
		idHash := utils.GetWorkerIdFromHash(REDUCE_COUNT, sentiment)
		delta3Data[idHash] = append(delta3Data[idHash], &protocol.Nu_3_Data{
			Sentiment: n3Data.GetSentiment(),
			Ratio:     n3Data.GetRatio(),
			Count:     n3Data.GetCount(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range delta3Data {
		tasks[REDUCE_EXCHANGE][NU_STAGE_3][nodeId] = &protocol.Task{
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
	// TODO: USE CLIENT ID INSTEAD OF BROADCAST ID WHEN MULTICLIENTS ARE IMPLEMENTED
	routingKey := ""

	for _, n3Data := range dataMap {
		ratio := n3Data.GetRatio() / float32(n3Data.GetCount())
		resultData[routingKey] = append(resultData[routingKey], &protocol.Result5_Data{
			Sentiment: n3Data.GetSentiment(),
			Ratio:     ratio,
		})
	}

	// Create tasks for each worker
	for nodeId, data := range resultData {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][nodeId] = &protocol.Task{
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) addResultsToNextStage(tasks Tasks, stage string) error {
	switch stage {
	case DELTA_STAGE_2:
		r.delta2Results(tasks)
	case DELTA_STAGE_3:
		r.delta3Results(tasks)
	case ETA_STAGE_2:
		r.eta2Results(tasks)
	case ETA_STAGE_3:
		r.eta3Results(tasks)
	case KAPPA_STAGE_2:
		r.kappa2Results(tasks)
	case KAPPA_STAGE_3:
		r.kappa3Results(tasks)
	case NU_STAGE_2:
		r.nu2Results(tasks)
	case NU_STAGE_3:
		r.nu3Results(tasks)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

/*
 */
func (r *Reducer) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == r.infraConfig.GetNodeId() {
		nextStage, nextExchange, nextStageCount, err := r.getNextStageData(data.GetStage())
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

		randomNode := r.randomHashFunc(nextStageCount)

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStage][randomNode] = nextStageEOF

	} else { // if the creator is not the same as the worker, send the stage results and EOF to the next node
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = r.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := r.getNextNodeId(r.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		reduceExchange := r.infraConfig.GetReduceExchange()
		stage := data.GetStage()

		tasks[reduceExchange] = make(map[string]map[string]*protocol.Task)
		tasks[reduceExchange][stage] = make(map[string]*protocol.Task)
		tasks[reduceExchange][stage][nextNode] = eofTask

		// send the results
		r.addResultsToNextStage(tasks, data.GetStage())
	}
	return tasks
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
