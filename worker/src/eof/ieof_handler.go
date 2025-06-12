package eof

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
)

// import (
// 	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
// 	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
// 	"github.com/op/go-logging"
// )

// var log = logging.MustGetLogger("log")

type IEOFHandler interface {
	// InitRing initializes the ring for the EOF handler.
	// It returns a map of tasks for the next stages.
	InitRing(stage string, eofType string, clientId string) (tasks common.Tasks)
	// HandleRing handles the ring for the EOF handler.
	// It returns a map of tasks for the next stages.
	HandleRing(
		data *protocol.RingEOF, clientId string,
		nextStageFunc func(stage string, clientId string) ([]common.NextStageData, error), eofStatus bool,
	) (tasks common.Tasks)
	// IgnoreDuplicates ables the EOF handler to ignore duplicates.
	// It is useful when you expect to receive the same RingEOF from multiple workers.
	IgnoreDuplicates()
}
