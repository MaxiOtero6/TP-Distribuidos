package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
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

// EOF Types
const SMALL_TABLE string = "small"
const BIG_TABLE string = "big"
const GENERAL string = "general"

// Query 1
const ALPHA_STAGE string = "alpha"
const BETA_STAGE string = "beta"

// Query 2
const GAMMA_STAGE string = "gamma"
const DELTA_STAGE_1 string = "delta_1"
const DELTA_STAGE_2 string = "delta_2"
const DELTA_STAGE_3 string = "delta_3"
const EPSILON_STAGE string = "epsilon"

// Query 3
const ZETA_STAGE string = "zeta"
const ETA_STAGE_1 string = "eta_1"
const ETA_STAGE_2 string = "eta_2"
const ETA_STAGE_3 string = "eta_3"
const THETA_STAGE string = "theta"

// Query 4
const IOTA_STAGE string = "iota"
const KAPPA_STAGE_1 string = "kappa_1"
const KAPPA_STAGE_2 string = "kappa_2"
const KAPPA_STAGE_3 string = "kappa_3"
const LAMBDA_STAGE string = "lambda"

// Query 5
const MU_STAGE string = "mu"
const NU_STAGE_1 string = "nu_1"
const NU_STAGE_2 string = "nu_2"
const NU_STAGE_3 string = "nu_3"

// Results
const RESULT_STAGE string = "result"

// Consts for tests
const TEST_WORKER_COUNT int = 1
const TEST_WORKER_ID string = "0"

// Source const
const BIG_TABLE_SOURCE string = "bigTable"
const SMALL_TABLE_SOURCE string = "smallTable"
const ANY_SOURCE string = ""

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
