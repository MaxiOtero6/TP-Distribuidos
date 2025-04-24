package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	heap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils"
)

// ParcilResult is a struct that holds the results of the different stages.

type result3 struct {
	maxHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
	minHeap *heap.TopKHeap[float32, *protocol.Theta_Data]
}

type ParcialResults struct {
	epsilonHeap *heap.TopKHeap[uint64, *protocol.Epsilon_Data]
	thetaData   result3
	lamdaHeap   *heap.TopKHeap[uint64, *protocol.Lambda_Data]
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
			epsilonHeap: heap.NewTopKHeap[uint64, *protocol.Epsilon_Data](5),
			thetaData: result3{
				maxHeap: heap.NewTopKHeap[float32, *protocol.Theta_Data](1),
				minHeap: heap.NewTopKHeap[float32, *protocol.Theta_Data](1),
			},
			lamdaHeap: heap.NewTopKHeap[uint64, *protocol.Lambda_Data](10),
		},
	}
}

func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data) (tasks Tasks) {
	epsilonHeap := t.parcialResults.epsilonHeap

	for _, eData := range data {
		investment := eData.GetTotalInvestment()
		epsilonHeap.Insert(investment, eData)
	}

	return nil
}

func (t *Topper) lambdaStage(data []*protocol.Lambda_Data) (tasks Tasks) {
	lamdaHeap := t.parcialResults.lamdaHeap

	for _, lData := range data {
		participations := lData.GetParticipations()

		lamdaHeap.Insert(participations, lData)

	}

	return nil
}

func (t *Topper) thetaStage(data []*protocol.Theta_Data) (tasks Tasks) {
	thetaMinHeap := t.parcialResults.thetaData.minHeap
	thetaMaxHeap := t.parcialResults.thetaData.maxHeap

	for _, tData := range data {
		avgRating := tData.GetAvgRating()

		thetaMaxHeap.Insert(avgRating, tData)
		thetaMinHeap.Insert(-avgRating, tData)
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
func (t *Topper) epsilonResultStage(tasks Tasks, clientId string) {

	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result2Data := make(map[string][]*protocol.Result2_Data)

	resultHeap := t.parcialResults.epsilonHeap

	// Asign the data to the corresponding worker
	nodeId := clientId

	position := uint32(1)
	for _, element := range resultHeap.GetTopK() {
		eData := element.Data

		result2Data[nodeId] = append(result2Data[nodeId], &protocol.Result2_Data{
			Position:        position,
			Country:         eData.GetProdCountry(),
			TotalInvestment: element.Value,
		})
		position++
	}

	for id, data := range result2Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result2{
				Result2: &protocol.Result2{
					Data: data,
				},
			},
		}
	}

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
func (t *Topper) thetaResultStage(tasks Tasks, clientId string) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result3Data := make(map[string][]*protocol.Result3_Data)

	resultHeapMax := t.parcialResults.thetaData.maxHeap
	resultHeapMin := t.parcialResults.thetaData.minHeap

	nodeId := clientId

	// Process the maximum value
	if len(resultHeapMax.GetTopK()) > 0 {
		element := resultHeapMax.GetTopK()[0]
		tData := element.Data

		result3Data[nodeId] = append(result3Data[nodeId], &protocol.Result3_Data{
			Type:   "Max",
			Id:     tData.GetId(),
			Title:  tData.GetTitle(),
			Rating: element.Value,
		})
	}

	// Process the minimum value
	if len(resultHeapMin.GetTopK()) > 0 {
		element := resultHeapMin.GetTopK()[0]
		tData := element.Data
		result3Data[nodeId] = append(result3Data[nodeId], &protocol.Result3_Data{
			Type:   "Min",
			Id:     tData.GetId(),
			Title:  tData.GetTitle(),
			Rating: -element.Value,
		})
	}

	for id, data := range result3Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result3{
				Result3: &protocol.Result3{
					Data: data,
				},
			},
		}
	}
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
func (t *Topper) lambdaResultStage(tasks Tasks, clientId string) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}
	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	result4Data := make(map[string][]*protocol.Result4_Data)

	resultHeap := t.parcialResults.lamdaHeap

	nodeId := clientId

	position := uint32(1)

	for _, element := range resultHeap.GetTopK() {
		lData := element.Data

		result4Data[nodeId] = append(result4Data[nodeId], &protocol.Result4_Data{
			Position:       position,
			ActorName:      lData.GetActorName(),
			ActorId:        lData.GetActorId(),
			Participations: element.Value,
		})
		position++
	}

	for id, data := range result4Data {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result4{
				Result4: &protocol.Result4{
					Data: data,
				},
			},
		}
	}
}

func (t *Topper) addResultsToNextStage(tasks Tasks, stage string, clientId string) error {
	switch stage {
	case EPSILON_STAGE:
		t.epsilonResultStage(tasks, clientId)
	case LAMBDA_STAGE:
		t.lambdaResultStage(tasks, clientId)
	case THETA_STAGE:
		t.thetaResultStage(tasks, clientId)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (t *Topper) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	tasks = make(Tasks)

	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()
	TOP_EXCHANGE := t.infraConfig.GetTopExchange()

	if data.GetWorkerCreatorId() == t.infraConfig.GetNodeId() {

		nextStageEOF := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						WorkerCreatorId: "",
						Stage:           RESULT_STAGE,
					},
				},
			},
		}

		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
		tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
		tasks[RESULT_EXCHANGE][RESULT_STAGE][clientId] = nextStageEOF

	} else {

		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = t.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode := t.infraConfig.GetNodeId()

		tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
		tasks[TOP_EXCHANGE][data.GetStage()] = make(map[string]*protocol.Task)
		tasks[TOP_EXCHANGE][data.GetStage()][nextNode] = eofTask

		t.addResultsToNextStage(tasks, data.GetStage(), clientId)
	}

	return tasks
}

func (t *Topper) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

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

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return t.omegaEOFStage(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
