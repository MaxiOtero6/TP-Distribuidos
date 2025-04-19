package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Tasks map[string]map[string]map[string]*protocol.Task

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (Tasks, error)
}

const FILTER_EXCHANGE string = "filterExchange"
const JOIN_EXCHANGE string = "joinExchange"
const RESULT_EXCHANGE string = "resultExchange"
const MAP_EXCHANGE string = "mapExchange"
const BROADCAST_ID string = ""

const BETA_STAGE string = "beta"
const ZETA_STAGE string = "zeta"
const IOTA_STAGE string = "iota"
const DELTA_STAGE string = "delta"
const NU_STAGE string = "nu"
const RESULT_STAGE string = "result"

const TEST_WORKER_COUNT int = 1
const TEST_WORKER_ID string = "0"

type ActionType string

const (
	FilterAction     ActionType = "FILTER"
	OverviewerAction ActionType = "OVERVIEWER"
	MapperAction     ActionType = "MAPPER"
	JoinerAction     ActionType = "JOINER"
	ReducerAction    ActionType = "REDUCER"
	TopperAction     ActionType = "TOPPER"
)

func NewAction(workerType string, workerCount int) Action {
	kind := ActionType(workerType)

	switch kind {
	case FilterAction:
		return NewFilter(workerCount)
	case OverviewerAction:
		return NewOverviewer(workerCount)
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
