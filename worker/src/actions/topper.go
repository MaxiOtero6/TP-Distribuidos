package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
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

type TopperPartialData[K heap.Ordered, V any] struct {
	heap           *heap.TopKHeap[K, *V]
	taskFragments  map[common.TaskFragmentIdentifier]struct{}
	omegaProcessed bool
	ringRound      uint32
}

func newTopperPartialData[K heap.Ordered, V any](top_k int) TopperPartialData[K, V] {
	return TopperPartialData[K, V]{
		heap:           heap.NewTopKHeap[K, *V](top_k),
		taskFragments:  make(map[common.TaskFragmentIdentifier]struct{}),
		omegaProcessed: false,
		ringRound:      0,
	}
}

type ThetaPartialData struct {
	minPartialData TopperPartialData[float32, protocol.Theta_Data]
	maxPartialData TopperPartialData[float32, protocol.Theta_Data]
}

type TopperPartialResults struct {
	epsilonData TopperPartialData[uint64, protocol.Epsilon_Data]
	thetaData   ThetaPartialData
	lamdaData   TopperPartialData[uint64, protocol.Lambda_Data]
}

// Topper is a struct that implements the Action interface.
type Topper struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*TopperPartialResults
	itemHashFunc   func(workersCount int, item string) string
	eofHandler     *eof.StatefulEofHandler
}

func (t *Topper) makePartialResults(clientId string) {
	if _, ok := t.partialResults[clientId]; ok {
		return
	}

	t.partialResults[clientId] = &TopperPartialResults{
		epsilonData: newTopperPartialData[uint64, protocol.Epsilon_Data](EPSILON_TOP_K),
		thetaData: ThetaPartialData{
			minPartialData: newTopperPartialData[float32, protocol.Theta_Data](THETA_TOP_K),
			maxPartialData: newTopperPartialData[float32, protocol.Theta_Data](THETA_TOP_K),
		},
		lamdaData: newTopperPartialData[uint64, protocol.Lambda_Data](LAMBDA_TOP_K),
	}
}

// NewTopper creates a new Topper instance.
// It initializes the worker count and returns a pointer to the Topper struct.
func NewTopper(infraConfig *model.InfraConfig) *Topper {
	eofHandler := eof.NewStatefulEofHandler(
		model.TopperAction,
		infraConfig,
		topperNextStageData,
		utils.GetWorkerIdFromHash,
	)

	topper := &Topper{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*TopperPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		eofHandler:     eofHandler,
	}

	go storage.StartCleanupRoutine(infraConfig.GetDirectory())

	return topper
}

func (t *Topper) epsilonStage(data []*protocol.Epsilon_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].epsilonData

	valueFunc := func(input *protocol.Epsilon_Data) uint64 {
		return input.GetTotalInvestment()
	}

	processTopperStage(
		partialData,
		data,
		clientId,
		taskIdentifier,
		valueFunc,
	)

	return nil
}

func (t *Topper) thetaStage(data []*protocol.Theta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].thetaData
	minPartialData := partialData.minPartialData
	maxPartialData := partialData.maxPartialData

	minValueFunc := func(input *protocol.Theta_Data) float32 {
		return -input.GetAvgRating()
	}
	maxValueFunc := func(input *protocol.Theta_Data) float32 {
		return input.GetAvgRating()
	}

	processTopperStage(
		minPartialData,
		data,
		clientId,
		taskIdentifier,
		minValueFunc,
	)
	processTopperStage(
		maxPartialData,
		data,
		clientId,
		taskIdentifier,
		maxValueFunc,
	)

	return nil
}

func (t *Topper) lambdaStage(data []*protocol.Lambda_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	partialData := t.partialResults[clientId].lamdaData

	valueFunc := func(input *protocol.Lambda_Data) uint64 {
		return input.GetParticipations()
	}

	processTopperStage(
		partialData,
		data,
		clientId,
		taskIdentifier,
		valueFunc,
	)

	return nil
}

func (t *Topper) epsilonResultStage(tasks common.Tasks, clientId string) {
	epsilonHeap := t.partialResults[clientId].epsilonData.heap
	partialData := epsilonHeap.GetTopK()

	results := utils.MapSlice(partialData, func(position int, data *protocol.Epsilon_Data) *protocol.Result2_Data {
		return &protocol.Result2_Data{
			Position:        uint32(position + 1),
			Country:         data.GetProdCountry(),
			TotalInvestment: data.GetTotalInvestment(),
		}
	})

	identifierFunc := func(data *protocol.Result2_Data) string {
		return data.GetCountry()
	}

	taskDataCreator := func(stage string, data []*protocol.Result2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result2{
				Result2: &protocol.Result2{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.EPSILON_STAGE, clientId)
	hashFunc := func(workersCount int, item string) string {
		return clientId
	}

	creatorId := t.infraConfig.GetNodeId()

	AddResults(tasks, results, nextStageData[0], clientId, creatorId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (t *Topper) thetaResultStage(tasks common.Tasks, clientId string) {
	thetaData := t.partialResults[clientId].thetaData

	minHeapData := thetaData.maxPartialData.heap
	maxHeapData := thetaData.minPartialData.heap

	minPartialData := minHeapData.GetTopK()
	maxPartialData := maxHeapData.GetTopK()

	minResults := utils.MapSlice(minPartialData, func(_ int, data *protocol.Theta_Data) *protocol.Result3_Data {
		return &protocol.Result3_Data{
			Type:   TYPE_MIN,
			Id:     data.GetId(),
			Title:  data.GetTitle(),
			Rating: -data.GetAvgRating(), // Negate for min heap
		}
	})

	maxResults := utils.MapSlice(maxPartialData, func(_ int, data *protocol.Theta_Data) *protocol.Result3_Data {
		return &protocol.Result3_Data{
			Type:   TYPE_MAX,
			Id:     data.GetId(),
			Title:  data.GetTitle(),
			Rating: data.GetAvgRating(),
		}
	})

	results := make([]*protocol.Result3_Data, 0, len(minResults)+len(maxResults))
	results = append(results, maxResults...)
	results = append(results, minResults...)

	identifierFunc := func(data *protocol.Result3_Data) string {
		return data.GetId()
	}

	taskDataCreator := func(stage string, data []*protocol.Result3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result3{
				Result3: &protocol.Result3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.THETA_STAGE, clientId)
	hashFunc := func(workersCount int, item string) string {
		return clientId
	}

	creatorId := t.infraConfig.GetNodeId()

	AddResults(tasks, results, nextStageData[0], clientId, creatorId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (t *Topper) lambdaResultStage(tasks common.Tasks, clientId string) {
	lambdaHeap := t.partialResults[clientId].lamdaData.heap
	partialData := lambdaHeap.GetTopK()

	results := utils.MapSlice(partialData, func(position int, data *protocol.Lambda_Data) *protocol.Result4_Data {
		return &protocol.Result4_Data{
			Position:       uint32(position + 1),
			ActorId:        data.GetActorId(),
			ActorName:      data.GetActorName(),
			Participations: data.GetParticipations(),
		}
	})

	identifierFunc := func(data *protocol.Result4_Data) string {
		return data.GetActorId()
	}

	taskDataCreator := func(stage string, data []*protocol.Result4_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result4{
				Result4: &protocol.Result4{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := t.getNextStageData(common.LAMBDA_STAGE, clientId)
	hashFunc := func(workersCount int, item string) string {
		return clientId
	}

	creatorId := t.infraConfig.GetNodeId()

	AddResults(tasks, results, nextStageData[0], clientId, creatorId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (t *Topper) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.EPSILON_STAGE:
		t.epsilonResultStage(tasks, clientId)
		// t.partialResults[clientId].epsilonData.Delete()
	case common.LAMBDA_STAGE:
		t.lambdaResultStage(tasks, clientId)
		// t.partialResults[clientId].lamdaData.Delete()
	case common.THETA_STAGE:
		t.thetaResultStage(tasks, clientId)
		// t.partialResults[clientId].thetaData.maxHeap.Delete()
		// t.partialResults[clientId].thetaData.minHeap.Delete()
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (t *Topper) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return topperNextStageData(stage, clientId, t.infraConfig, t.itemHashFunc)
}

func topperNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.EPSILON_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.THETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.LAMBDA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  infraConfig.GetNodeId(),
			},
		}, nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	omegaReady, err := t.getOmegaProcessed(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get omega ready for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if omegaReady {
		log.Debugf("Omega EOF for stage %s has already been processed for client %s", data.GetStage(), clientId)
		return tasks
	}

	t.updateOmegaProcessed(clientId, data.GetStage())

	t.eofHandler.HandleOmegaEOF(tasks, data, clientId)

	return tasks
}

func (t *Topper) ringEOFStage(data *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	taskIdentifiers, err := t.getTaskIdentifiers(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", data.GetStage(), err)
		return tasks
	}

	ringRound, err := t.getRingRound(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get ring round for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if ringRound >= data.GetRoundNumber() {
		log.Debugf("Ring EOF for stage %s and client %s has already been processed for round %d", data.GetStage(), clientId, ringRound)
		return tasks
	}

	t.updateRingRound(clientId, data.GetStage(), data.GetRoundNumber())

	taskCount := t.participatesInResults(clientId, data.GetStage())
	ready := t.eofHandler.HandleRingEOF(tasks, data, clientId, taskIdentifiers, taskCount)

	if ready {
		err = t.addResultsToNextStage(tasks, data.GetStage(), clientId)
		if err != nil {
			log.Errorf("Failed to add results to next stage for stage %s: %s", data.GetStage(), err)
			return nil
		}
	}

	return tasks
}

func (t *Topper) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	t.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Epsilon:
		data := v.Epsilon.GetData()
		return t.epsilonStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Theta:
		data := v.Theta.GetData()
		return t.thetaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Lambda:
		data := v.Lambda.GetData()
		return t.lambdaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return t.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return t.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (t *Topper) getOmegaProcessed(clientId string, stage string) (bool, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return partialResults.epsilonData.omegaProcessed, nil
	case common.THETA_STAGE:
		return partialResults.thetaData.minPartialData.omegaProcessed, nil
	case common.LAMBDA_STAGE:
		return partialResults.lamdaData.omegaProcessed, nil
	default:
		return false, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) getRingRound(clientId string, stage string) (uint32, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return partialResults.epsilonData.ringRound, nil
	case common.THETA_STAGE:
		return partialResults.thetaData.minPartialData.ringRound, nil
	case common.LAMBDA_STAGE:
		return partialResults.lamdaData.ringRound, nil
	default:
		return 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) getTaskIdentifiers(clientId string, stage string) ([]common.TaskFragmentIdentifier, error) {
	partialResults := t.partialResults[clientId]
	switch stage {
	case common.EPSILON_STAGE:
		return utils.MapKeys(partialResults.epsilonData.taskFragments), nil
	case common.THETA_STAGE:
		return utils.MapKeys(partialResults.thetaData.minPartialData.taskFragments), nil
	case common.LAMBDA_STAGE:
		return utils.MapKeys(partialResults.lamdaData.taskFragments), nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (t *Topper) participatesInResults(clientId string, stage string) int {
	partialResults, ok := t.partialResults[clientId]
	if !ok {
		return 0
	}

	participates := false

	switch stage {
	case common.EPSILON_STAGE:
		participates = partialResults.epsilonData.heap.Len() > 0
	case common.THETA_STAGE:
		participates = partialResults.thetaData.minPartialData.heap.Len() > 0 ||
			partialResults.thetaData.maxPartialData.heap.Len() > 0
	case common.LAMBDA_STAGE:
		participates = partialResults.lamdaData.heap.Len() > 0
	default:
		log.Errorf("Invalid stage: %s", stage)
		return 0
	}

	if participates {
		return 1
	}
	return 0
}

func processTopperStage[K heap.Ordered, V any](
	partialData TopperPartialData[K, V],
	data []*V,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	valueFunc func(input *V) K,
) {
	taskID := common.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, processed := partialData.taskFragments[taskID]; !processed {
		return
	}

	// Mark task as processed
	partialData.taskFragments[taskID] = struct{}{}

	// Aggregate data
	for _, item := range data {
		partialData.heap.Insert(valueFunc(item), item)
	}
}

// Actualizar funciones para usar las constantes de etapas del paquete common
func (t *Topper) updateOmegaProcessed(clientId string, stage string) {
	switch stage {
	case common.EPSILON_STAGE:
		t.partialResults[clientId].epsilonData.omegaProcessed = true
	case common.THETA_STAGE:
		t.partialResults[clientId].thetaData.minPartialData.omegaProcessed = true
		t.partialResults[clientId].thetaData.maxPartialData.omegaProcessed = true
	case common.LAMBDA_STAGE:
		t.partialResults[clientId].lamdaData.omegaProcessed = true
	}
}

func (t *Topper) updateRingRound(clientId string, stage string, round uint32) {
	switch stage {
	case common.EPSILON_STAGE:
		t.partialResults[clientId].epsilonData.ringRound = round
	case common.THETA_STAGE:
		t.partialResults[clientId].thetaData.minPartialData.ringRound = round
		t.partialResults[clientId].thetaData.maxPartialData.ringRound = round
	case common.LAMBDA_STAGE:
		t.partialResults[clientId].lamdaData.ringRound = round
	}
}
