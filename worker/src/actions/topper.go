package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	heap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils"
)

// ParcilResult is a struct that holds the results of the different stages.

type result3 struct {
	maxHeap *heap.TopKHeap
	minHeap *heap.TopKHeap
}

type ParcialResults struct {
	result2Heap *heap.TopKHeap
	result3Heap result3
	result4Heap *heap.TopKHeap
}

// Topper is a struct that implements the Action interface.
type Topper struct {
	infraConfig    *model.InfraConfig
	parcialResults *ParcialResults
}

// NewTopper creates a new Topper instance.
// It initializes the worker count and returns a pointer to the Topper struct.
func NewTopper(infraConfig *model.InfraConfig) *Topper {
	return &Topper{
		infraConfig: infraConfig,

		parcialResults: &ParcialResults{
			result2Heap: heap.NewTopKHeap(5),
			result3Heap: result3{
				maxHeap: heap.NewTopKHeap(1),
				minHeap: heap.NewTopKHeap(1),
			},
			result4Heap: heap.NewTopKHeap(10),
		},
	}
}

func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data) (tasks Tasks) {
	result2Heap := t.parcialResults.result2Heap

	for _, country := range data {
		prodCountry := country.GetProdCountry()
		investment := country.GetTotalInvestment()

		result2Heap.Insert(investment, map[string]interface{}{
			"prodCountry": prodCountry,
		})
	}

	return nil
}

func (t *Topper) lambdaStage(data []*protocol.Lambda_Data) (tasks Tasks) {
	result4Heap := t.parcialResults.result4Heap
	for _, actor := range data {
		actorId := actor.GetActorId()
		participations := actor.GetParticipations()
		actorName := actor.GetActorName()

		result4Heap.Insert(int(participations), map[string]interface{}{
			"ActorId":   actorId,
			"ActorName": actorName,
		})

	}

	return nil
}

func (t *Topper) thetaStage(data []*protocol.Theta_Data) (tasks Tasks) {
	result3HeapMax := t.parcialResults.result3Heap.maxHeap
	result3HeapMin := t.parcialResults.result3Heap.minHeap
	for _, movie := range data {
		movieId := movie.GetId()
		title := movie.GetTitle()
		avgRating := movie.GetAvgRating()

		result3HeapMax.Insert(avgRating, map[string]interface{}{
			"Title":     title,
			"AvgRating": avgRating,
		})
		result3HeapMin.Insert(-avgRating, map[string]interface{}{
			"Title":     title,
			"AvgRating": avgRating,
		})
	}
	return nil
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
func (t *Topper) epsilonResultStage(tasks Tasks) {

	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()
	BROADCAST_ID := t.infraConfig.GetBroadcastID()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result2Data := make(map[string]*protocol.Result2_Data)

	resultHeap := t.parcialResults.result2Heap

	position := uint32(1)
	for _, element := range resultHeap.GetTopK() {
		data := element.Data.(map[string]interface{})
		prodCountry := data["prodCountry"].(string)

		resultMap[BROADCAST_ID] = append(resultMap[BROADCAST_ID], &protocol.Result2_Data{
			Position:        position,
			Country:         prodCountry,
			TotalInvestment: uint64(element.Value),
		})
		position++
	}

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
func (t *Topper) thetaResultStage(data []*protocol.Theta_Data) (tasks Tasks) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()
	BROADCAST_ID := t.infraConfig.GetBroadcastID()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result3Data := make(map[string][]*protocol.Result3_Data)

	resultHeapMax := t.parcialResults.result3Heap.maxHeap
	resultHeapMin := t.parcialResults.result3Heap.minHeap

	// Process the maximum value
	if len(resultHeapMax.GetTopK()) > 0 {
		element := resultHeapMax.GetTopK()[0]
		data := element.Data.(map[string]interface{})
		movieId := data["MovieId"].(string)
		title := data["Title"].(string)

		resultMap[BROADCAST_ID] = append(resultMap[BROADCAST_ID], protocol.Result3_Data{
			Type:      "max",
			MovieId:   movieId,
			Title:     title,
			AvgRating: element.Value,
		})
	}

	// Process the minimum value
	if len(resultHeapMin.GetTopK()) > 0 {
		element := resultHeapMin.GetTopK()[0]
		data := element.Data.(map[string]interface{})
		movieId := data["MovieId"].(string)
		title := data["Title"].(string)

		resultMap[BROADCAST_ID] = append(resultMap[BROADCAST_ID], protocol.Result3_Data{
			Type:      "min",
			MovieId:   movieId,
			Title:     title,
			AvgRating: -element.Value,
		})
	}

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
func (t *Topper) lambdaResultStage(data []*protocol.Lambda_Data) (tasks Tasks) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()
	BROADCAST_ID := t.infraConfig.GetBroadcastID()

	tasks = make(Tasks)
	tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result4Data := make(map[string][]*protocol.Result4_Data)

	resultHeap := t.parcialResults.result4Heap

	position := 1
	for _, element := range resultHeap.GetTopK() {
		data := element.Data.(map[string]interface{})
		actorId := data["ActorId"].(string)
		actorName := data["ActorName"].(string)

		resultMap[BROADCAST_ID] = append(resultMap[BROADCAST_ID], protocol.Result4_Data{
			Position:  position,
			ActorName: actorName,
			ActorId:   actorId,
		})
		position++
	}

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

func (t *Topper) getNextNodeId(nodeId string) (string, error) {
	clientId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert clientId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (clientId+1)%t.infraConfig.GetReduceCount())
	return nextNodeId, nil
}

func (t *Topper) addResultsToNextStage(tasks Tasks, stage string) error {
	switch stage {
	case EPSILON_STAGE:
		t.epsilonResultStage(tasks)
	case LAMBDA_STAGE:
		t.lambdaResultStage(tasks)
	case THETA_STAGE:
		t.thetaResultStage(tasks)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (t *Topper) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case EPSILON_STAGE, LAMBDA_STAGE, THETA_STAGE:
		return RESULT_STAGE, t.infraConfig.GetResultExchange(), 0, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	nextStage, nextExchange, nextStageCount, err := t.getNextStageData(data.GetStage())
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
	nextNodeId := fmt.Sprintf("%d", (clientId+1)%t.infraConfig.GetReduceCount())

	tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
	tasks[nextExchange][nextExchange] = make(map[string]*protocol.Task)
	tasks[nextExchange][nextExchange][randomNode] = nextStageEOF

	if err != nil {
		log.Errorf("Failed to get next node id: %s", err)
		return nil
	}

	// send the results
	t.addResultsToNextStage(tasks, data.GetStage())

	return tasks
}
