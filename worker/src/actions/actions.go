package actions

import (
	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PartialData[T any] struct {
	data           map[string]*T
	taskFragments  map[common.TaskFragmentIdentifier]struct{}
	omegaProcessed bool
	ringRound      uint32
}

func NewPartialData[T any]() *PartialData[T] {
	return &PartialData[T]{
		data:           make(map[string]*T),
		taskFragments:  make(map[common.TaskFragmentIdentifier]struct{}),
		omegaProcessed: false,
		ringRound:      0,
	}
}

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (common.Tasks, error)
}

// NewAction creates a new action based on the worker type.
func NewAction(workerType string, infraConfig *model.InfraConfig) Action {
	kind := model.ActionType(workerType)

	switch kind {
	case model.FilterAction:
		return NewFilter(infraConfig)
	case model.OverviewerAction:
		return NewOverviewer(infraConfig)
	case model.MapperAction:
		return NewMapper(infraConfig)
	case model.JoinerAction:
		return NewJoiner(infraConfig)
	case model.ReducerAction:
		return NewReducer(infraConfig)
	case model.MergerAction:
		return NewMerger(infraConfig)
	case model.TopperAction:
		return NewTopper(infraConfig)
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}

func AddResults[T any](
	tasks common.Tasks,
	results []T,
	nextStageData common.NextStageData,
	clientId string,
	taskNumber int,
	initialTaskFragmentNumber int,
	allFragments bool,
	itemHashFunc func(workersCount int, item string) string,
	identifierFunc func(input T) string,
	taskDataCreator func(stage string, data []T, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task,
) int {
	// Ensure the nested maps exist
	if _, ok := tasks[nextStageData.Exchange]; !ok {
		tasks[nextStageData.Exchange] = make(map[string]map[string]*protocol.Task)
	}
	if _, ok := tasks[nextStageData.Exchange][nextStageData.Stage]; !ok {
		tasks[nextStageData.Exchange][nextStageData.Stage] = make(map[string]*protocol.Task)
	}

	dataByNode := make(map[string][]T)

	for _, data := range results {
		nodeId := itemHashFunc(nextStageData.WorkerCount, identifierFunc(data))
		dataByNode[nodeId] = append(dataByNode[nodeId], data)
	}

	destinationNodes := make([]string, 0, len(dataByNode))
	for nodeId := range dataByNode {
		destinationNodes = append(destinationNodes, nodeId)
	}
	slices.Sort(destinationNodes)

	createTaskIdentifier := func(nodeId string, index int) *protocol.TaskIdentifier {
		return &protocol.TaskIdentifier{
			TaskNumber:         uint32(taskNumber),
			TaskFragmentNumber: uint32(initialTaskFragmentNumber + index),
			LastFragment:       allFragments && index == len(destinationNodes)-1,
		}
	}

	for index, nodeId := range destinationNodes {
		taskIdentifier := createTaskIdentifier(nodeId, index)
		tasks[nextStageData.Exchange][nextStageData.Stage][nodeId] = taskDataCreator(
			nextStageData.Stage,
			dataByNode[nodeId],
			clientId,
			taskIdentifier,
		)
	}

	return len(destinationNodes) + initialTaskFragmentNumber
}

func ProcessStage[T any](
	partial *PartialData[T],
	newItems []*T,
	clientID string,
	taskIdentifier *protocol.TaskIdentifier,
	merge func(*T, *T),
	keySelector func(*T) string,
) {

	taskID := common.TaskFragmentIdentifier{
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, processed := partial.taskFragments[taskID]; processed {
		// Task already handled
		return
	}

	// Mark task as processed
	partial.taskFragments[taskID] = struct{}{}

	// Aggregate data
	utils.MergeIntoMap(partial.data, newItems, keySelector, merge)
}
