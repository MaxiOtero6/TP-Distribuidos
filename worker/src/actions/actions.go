package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type PartialData[T any] struct {
	data  map[string]T
	ready bool
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

	workerCount := infraConfig.GetWorkersCountByType(workerType)

	nextNodeId, err := utils.GetNextNodeId(
		infraConfig.GetNodeId(),
		workerCount,
	)
	if err != nil {
		log.Panicf("Failed to get next node id, self id %s: %s", infraConfig.GetNodeId(), err)
	}

	eofHandler := eof_handler.NewEOFHandler(
		infraConfig.GetNodeId(),
		workerType,
		workerCount,
		infraConfig.GetEofExchange(),
		nextNodeId,
	)

	switch kind {
	case model.FilterAction:
		return NewFilter(infraConfig, eofHandler)
	case model.OverviewerAction:
		return NewOverviewer(infraConfig, eofHandler)
	case model.MapperAction:
		return NewMapper(infraConfig, eofHandler)
	case model.JoinerAction:
		eofHandler.IgnoreDuplicates()
		return NewJoiner(infraConfig, eofHandler)
	case model.ReducerAction:
		return NewReducer(infraConfig, eofHandler)
	case model.MergerAction:
		eofHandler.IgnoreDuplicates()
		return NewMerger(infraConfig, eofHandler)
	case model.TopperAction:
		eofHandler.IgnoreDuplicates()
		return NewTopper(infraConfig, eofHandler)
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}
