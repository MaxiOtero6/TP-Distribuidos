package actions

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

type Action interface {
	// Execute executes the action.
	// It returns a map of tasks for the next stages.
	// It returns an error if the action fails.
	Execute(task *protocol.Task) (map[string]*protocol.Task, error)
}
