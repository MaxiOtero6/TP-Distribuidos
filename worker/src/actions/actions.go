package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
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

// Exchanges
const FILTER_EXCHANGE string = "filterExchange"
const OVERVIEWER_EXCHANGE string = "overviewExchange"
const MAP_EXCHANGE string = "mapExchange"
const JOIN_EXCHANGE string = "joinExchange"
const REDUCE_EXCHANGE string = "reduceExchange"
const TOP_EXCHANGE string = "topExchange"
const RESULT_EXCHANGE string = "resultExchange"

// Query 1
const ALPHA_STAGE string = "alpha"
const BETA_STAGE string = "beta"

// Query 2
const GAMMA_STAGE string = "gamma"
const DELTA_STAGE_1 string = "delta_1"
const DELTA_STAGE_2 string = "delta_2"
const EPSILON_STAGE string = "epsilon"

// Query 3
const ZETA_STAGE string = "zeta"
const ETA_STAGE_1 string = "eta_1"
const ETA_STAGE_2 string = "eta_2"
const THETA_STAGE string = "theta"

// Query 4
const IOTA_STAGE string = "iota"
const KAPPA_STAGE_1 string = "kappa_1"
const KAPPA_STAGE_2 string = "kappa_2"
const LAMBDA_STAGE string = "lambda"

// Query 5
const MU_STAGE string = "mu"
const NU_STAGE_1 string = "nu_1"
const NU_STAGE_2 string = "nu_2"

// Results
const RESULT_STAGE string = "result"

// Hardcoded routing key. Other routing keys are workers IDs.
const BROADCAST_ID string = ""

// Consts for tests
const TEST_WORKER_COUNT int = 1
const TEST_WORKER_ID string = "0"

// ActionType represents the type of action to be performed.
type ActionType string

const (
	FilterAction     ActionType = "FILTER"
	OverviewerAction ActionType = "OVERVIEWER"
	MapperAction     ActionType = "MAPPER"
	JoinerAction     ActionType = "JOINER"
	ReducerAction    ActionType = "REDUCER"
	TopperAction     ActionType = "TOPPER"
)

// NewAction creates a new action based on the worker type.
func NewAction(workerType string, workerCount int) Action {
	kind := ActionType(workerType)

	switch kind {
	case FilterAction:
		return NewFilter(workerCount)
	case OverviewerAction:
		return NewOverviewer(workerCount)
	case MapperAction:
		return NewMapper(workerCount)
	case JoinerAction:
		return NewJoiner(workerCount)
	case ReducerAction:
		return NewReducer(workerCount)
	case TopperAction:
		return NewTopper(workerCount)
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}
