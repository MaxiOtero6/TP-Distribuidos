package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Tasks = common.Tasks
type NextStageData = common.NextStageData

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (common.Tasks, error)
}

// EOF Types
const SMALL_TABLE string = common.SMALL_TABLE
const BIG_TABLE string = common.BIG_TABLE
const GENERAL string = common.GENERAL

// Query 1
const ALPHA_STAGE string = common.ALPHA_STAGE
const BETA_STAGE string = common.BETA_STAGE

// Query 2
const GAMMA_STAGE string = common.GAMMA_STAGE
const DELTA_STAGE_1 string = common.DELTA_STAGE_1
const DELTA_STAGE_2 string = common.DELTA_STAGE_2
const DELTA_STAGE_3 string = common.DELTA_STAGE_3
const EPSILON_STAGE string = common.EPSILON_STAGE

// Query 3
const ZETA_STAGE string = common.ZETA_STAGE
const ETA_STAGE_1 string = common.ETA_STAGE_1
const ETA_STAGE_2 string = common.ETA_STAGE_2
const ETA_STAGE_3 string = common.ETA_STAGE_3
const THETA_STAGE string = common.THETA_STAGE

// Query 4
const IOTA_STAGE string = common.IOTA_STAGE
const KAPPA_STAGE_1 string = common.KAPPA_STAGE_1
const KAPPA_STAGE_2 string = common.KAPPA_STAGE_2
const KAPPA_STAGE_3 string = common.KAPPA_STAGE_3
const LAMBDA_STAGE string = common.LAMBDA_STAGE

// Query 5
const MU_STAGE string = common.MU_STAGE
const NU_STAGE_1 string = common.NU_STAGE_1
const NU_STAGE_2 string = common.NU_STAGE_2
const NU_STAGE_3 string = common.NU_STAGE_3

// Results
const RESULT_STAGE string = common.RESULT_STAGE

// Consts for tests
const TEST_WORKER_COUNT int = common.TEST_WORKER_COUNT
const TEST_WORKER_ID string = common.TEST_WORKER_ID

// Source const
const BIG_TABLE_SOURCE string = common.BIG_TABLE_SOURCE
const SMALL_TABLE_SOURCE string = common.SMALL_TABLE_SOURCE
const ANY_SOURCE string = common.ANY_SOURCE

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
