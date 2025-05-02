package eof_handler

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

const RING_STAGE string = common.RING_STAGE
const RESULT_STAGE string = common.RESULT_STAGE

type Tasks = common.Tasks
type NextStageData = common.NextStageData

type IEOFHandler interface {
	// InitRing initializes the ring for the EOF handler.
	// It returns a map of tasks for the next stages.
	InitRing(stage string, eofType string) (tasks Tasks)
	// HandleRing handles the ring for the EOF handler.
	// It returns a map of tasks for the next stages.
	HandleRing(
		data *protocol.RingEOF, clientId string,
		nextStageFunc func(stage string) ([]NextStageData, error), eofStatus bool,
	) (tasks Tasks)
}
