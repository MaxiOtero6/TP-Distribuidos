package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
	heap "github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
)

const EPSILON_TOP_K = 5
const LAMBDA_TOP_K = 10
const THETA_TOP_K = 1
const TYPE_MAX = "Max"
const TYPE_MIN = "Min"
const TOPPER_STAGES_COUNT uint = 3

// ParcilResult is a struct that holds the results of the different stages.

// Topper is a struct that implements the Action interface.
type Topper struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.PartialResults
	eofHandler     eof_handler.IEOFHandler
}

func (t *Topper) makePartialResults(clientId string) {
	if _, ok := t.partialResults[clientId]; ok {
		return
	}

	t.partialResults[clientId] = &common.PartialResults{
		EpsilonHeap: heap.NewTopKHeap[uint64, *protocol.Epsilon_Data](EPSILON_TOP_K),
		ThetaData: common.Result3{
			MaxHeap: heap.NewTopKHeap[float32, *protocol.Theta_Data](THETA_TOP_K),
			MinHeap: heap.NewTopKHeap[float32, *protocol.Theta_Data](THETA_TOP_K),
		},
		LambdaHeap: heap.NewTopKHeap[uint64, *protocol.Lambda_Data](LAMBDA_TOP_K),
	}
}

// NewTopper creates a new Topper instance.
// It initializes the worker count and returns a pointer to the Topper struct.
func NewTopper(infraConfig *model.InfraConfig, eofHandler eof_handler.IEOFHandler) *Topper {
	topper := &Topper{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*common.PartialResults),
		eofHandler:     eofHandler,
	}
	go storage.StartCleanupRoutine(infraConfig.GetDirectory())

	return topper
}

func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data, clientId string) (tasks common.Tasks) {
	epsilonHeap := t.partialResults[clientId].EpsilonHeap

	for _, eData := range data {
		investment := eData.GetTotalInvestment()
		epsilonHeap.Insert(investment, eData)
	}

	// Prepare the data to be saved
	convertedData := make(map[string]*protocol.Epsilon_Data)

	for value, element := range epsilonHeap.GetTopK() {
		convertedData[fmt.Sprintf("%d", value)] = element.Data
	}

	err := storage.SaveDataToFile(t.infraConfig.GetDirectory(), clientId, common.EPSILON_STAGE, common.ANY_SOURCE, convertedData)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.EPSILON_STAGE, err)
	}

	return nil
}

func (t *Topper) lambdaStage(data []*protocol.Lambda_Data, clientId string) (tasks common.Tasks) {
	LambdaHeap := t.partialResults[clientId].LambdaHeap

	for _, lData := range data {
		participations := lData.GetParticipations()

		LambdaHeap.Insert(participations, lData)

	}

	// Prepare the data to be saved
	convertedData := make(map[string]*protocol.Lambda_Data)

	for value, element := range LambdaHeap.GetTopK() {
		convertedData[fmt.Sprintf("%d", value)] = element.Data
	}

	err := storage.SaveDataToFile(t.infraConfig.GetDirectory(), clientId, common.LAMBDA_STAGE, common.ANY_SOURCE, convertedData)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.LAMBDA_STAGE, err)
	}

	return nil
}

func (t *Topper) thetaStage(data []*protocol.Theta_Data, clientId string) (tasks common.Tasks) {
	thetaMinHeap := t.partialResults[clientId].ThetaData.MinHeap
	thetaMaxHeap := t.partialResults[clientId].ThetaData.MaxHeap

	for _, tData := range data {
		avgRating := tData.GetAvgRating()

		thetaMaxHeap.Insert(avgRating, tData)
		thetaMinHeap.Insert(-avgRating, tData)
	}

	// Prepare the data to be saved
	convertedData := make(map[string]*protocol.Theta_Data)

	if len(thetaMaxHeap.GetTopK()) == 0 || len(thetaMinHeap.GetTopK()) == 0 {
		log.Errorf("No data found for %s stage", common.THETA_STAGE)
		return nil
	}

	elementMax := thetaMaxHeap.GetTopK()[0]
	elementMin := thetaMinHeap.GetTopK()[0]

	convertedData[fmt.Sprintf("%f", elementMax.Value)] = elementMax.Data
	convertedData[fmt.Sprintf("%f", elementMin.Value)] = elementMin.Data

	err := storage.SaveDataToFile(t.infraConfig.GetDirectory(), clientId, common.THETA_STAGE, common.ANY_SOURCE, convertedData)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.THETA_STAGE, err)
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
func (t *Topper) epsilonResultStage(tasks common.Tasks, clientId string) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[RESULT_EXCHANGE][common.RESULT_STAGE] = make(map[string]*protocol.Task)
	result2Data := make(map[string][]*protocol.Result2_Data)

	resultHeap := t.partialResults[clientId].EpsilonHeap

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
		tasks[RESULT_EXCHANGE][common.RESULT_STAGE][id] = &protocol.Task{
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
func (t *Topper) thetaResultStage(tasks common.Tasks, clientId string) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[RESULT_EXCHANGE][common.RESULT_STAGE] = make(map[string]*protocol.Task)
	result3Data := make(map[string][]*protocol.Result3_Data)

	resultHeapMax := t.partialResults[clientId].ThetaData.MaxHeap
	resultHeapMin := t.partialResults[clientId].ThetaData.MinHeap

	nodeId := clientId

	// Process the maximum value
	if len(resultHeapMax.GetTopK()) > 0 {
		element := resultHeapMax.GetTopK()[0]
		tData := element.Data

		result3Data[nodeId] = append(result3Data[nodeId], &protocol.Result3_Data{
			Type:   TYPE_MAX,
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
			Type:   TYPE_MIN,
			Id:     tData.GetId(),
			Title:  tData.GetTitle(),
			Rating: -element.Value,
		})
	}

	for id, data := range result3Data {
		tasks[RESULT_EXCHANGE][common.RESULT_STAGE][id] = &protocol.Task{
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
func (t *Topper) lambdaResultStage(tasks common.Tasks, clientId string) {
	RESULT_EXCHANGE := t.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[RESULT_EXCHANGE][common.RESULT_STAGE] = make(map[string]*protocol.Task)
	result4Data := make(map[string][]*protocol.Result4_Data)

	resultHeap := t.partialResults[clientId].LambdaHeap

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
		tasks[RESULT_EXCHANGE][common.RESULT_STAGE][id] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result4{
				Result4: &protocol.Result4{
					Data: data,
				},
			},
		}
	}
}

func (t *Topper) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.EPSILON_STAGE:
		t.epsilonResultStage(tasks, clientId)
		t.partialResults[clientId].EpsilonHeap.Delete()
	case common.LAMBDA_STAGE:
		t.lambdaResultStage(tasks, clientId)
		t.partialResults[clientId].LambdaHeap.Delete()
	case common.THETA_STAGE:
		t.thetaResultStage(tasks, clientId)
		t.partialResults[clientId].ThetaData.MaxHeap.Delete()
		t.partialResults[clientId].ThetaData.MinHeap.Delete()
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (t *Topper) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	switch stage {
	case common.EPSILON_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    t.infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.THETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    t.infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.LAMBDA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    t.infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = t.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)

	if err := t.addResultsToNextStage(tasks, data.GetStage(), clientId); err == nil {
		if err := storage.DeletePartialResults(t.infraConfig.GetDirectory(), clientId, data.Stage, common.ANY_SOURCE); err != nil {
			log.Errorf("Failed to delete partial results: %s", err)
		}
	} else {
		log.Errorf("Failed to add results to next stage: %s", err)
	}

	return tasks
}

func (t *Topper) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	// For toppers eofStatus is always true
	// because one of them receives the EOF and init the ring
	// and the others just declare that they are alive
	// Only one topper resolves the query for a client
	return t.eofHandler.HandleRing(data, clientId, t.getNextStageData, true)
}

func (t *Topper) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	t.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Epsilon:
		data := v.Epsilon.GetData()
		return t.epsilonStage(data, clientId), nil

	case *protocol.Task_Theta:
		data := v.Theta.GetData()
		return t.thetaStage(data, clientId), nil

	case *protocol.Task_Lambda:
		data := v.Lambda.GetData()
		return t.lambdaStage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return t.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return t.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
