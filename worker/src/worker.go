package worker

import (
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/actions"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Worker struct {
	WorkerId string
	rabbitMQ *mom.RabbitMQ
	action   actions.Action
	done     chan os.Signal
}

func NewWorker(id string, workerType string, workerCount int, signalChan chan os.Signal) *Worker {
	rabbitMQ := mom.NewRabbitMQ()

	action := actions.NewAction(workerType, workerCount)

	return &Worker{
		WorkerId: id,
		rabbitMQ: rabbitMQ,
		action:   action,
		done:     signalChan,
	}
}

func (w *Worker) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	w.rabbitMQ.InitConfig(exchanges, queues, binds, w.WorkerId)
}
