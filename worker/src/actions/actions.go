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

func NewAction(workerType string) Action {
	switch workerType {
	case "filter":
		return &Filter{}
	case "overviewer":
		return NewOverviewer()
		// case "mapper":
		// 	return
		// case "joiner":
		// 	return
		// case "reducer":
		// 	return
		// case "topper":
		// 	return
	default:
		log.Panicf("Unknown worker type: %s", workerType)
		return nil
	}
}
