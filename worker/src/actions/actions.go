package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (map[string]*protocol.Task, error)
}

type ActionType string

const (
	FilterAction     ActionType = "FILTER"
	OverviewerAction ActionType = "OVERVIEWER"
	MapperAction     ActionType = "MAPPER"
	JoinerAction     ActionType = "JOINER"
	ReducerAction    ActionType = "REDUCER"
	TopperAction     ActionType = "TOPPER"
)

func NewAction(workerType string) Action {
	kind := ActionType(workerType)

	switch kind {
	case FilterAction:
		return &Filter{}
	case OverviewerAction:
		return NewOverviewer()
	case MapperAction:
		return nil
	case JoinerAction:
		return nil
	case ReducerAction:
		return nil
	case TopperAction:
		return nil
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}
