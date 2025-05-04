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

type Tasks = common.Tasks
type NextStageData = common.NextStageData

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
		return NewFilter(workerType, infraConfig, eofHandler)
	case model.OverviewerAction:
		return NewOverviewer(workerType, infraConfig, eofHandler)
	case model.MapperAction:
		return NewMapper(workerType, infraConfig, eofHandler)
	case model.JoinerAction:
		eofHandler.IgnoreDuplicates()
		return NewJoiner(workerType, infraConfig, eofHandler)
	case model.ReducerAction:
		eofHandler.IgnoreDuplicates()
		return NewReducer(workerType, infraConfig, eofHandler)
	case model.MergerAction:
		eofHandler.IgnoreDuplicates()
		return NewMerger(workerType, infraConfig, eofHandler)
	case model.TopperAction:
		eofHandler.IgnoreDuplicates()
		return NewTopper(workerType, infraConfig, eofHandler)
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}
