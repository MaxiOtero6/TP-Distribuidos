package worker

import (
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/actions"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

type Worker struct {
	WorkerId    string
	rabbitMQ    *mom.RabbitMQ
	action      actions.Action
	done        chan os.Signal
	consumeChan mom.ConsumerChan
}

func NewWorker(id string, workerType string, workerCount int, signalChan chan os.Signal) *Worker {
	rabbitMQ := mom.NewRabbitMQ()

	action := actions.NewAction(workerType, workerCount)

	return &Worker{
		WorkerId: id,
		rabbitMQ: rabbitMQ,
		action:   action,
		done:     signalChan,
		consumeChan: nil,
	}
}

func (w *Worker) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	if len(binds) != 1 {
		log.Panicf("For workers is expected to load one bind from the config file, got %d", len(binds))
	}

	w.rabbitMQ.InitConfig(exchanges, queues, binds, w.WorkerId)
	w.consumeChan = w.rabbitMQ.Consume(binds[0]["queue"])
}

func (w *Worker) Run() {
	if w.consumeChan == nil {
		log.Panicf("Consume channel is nil, did you call InitConfig?")
	}
	
	defer w.Shutdown()

	for w.isRunning() {
		message := <-w.consumeChan
		taskRaw := message.Body

		task := &protocol.Task{}

		err := proto.Unmarshal(taskRaw, task)

		if err != nil {
			log.Errorf("Failed to unmarshal task: %s", err)
			continue
		}

		subTasks, err := w.action.Execute(task)

		if err != nil {
			log.Errorf("Failed to execute task: %s", err)
			continue
		}

		w.sendSubTasks(subTasks)
		message.Ack(false)
	}
}

func (w *Worker) sendSubTasks(subTasks actions.Tasks) {
	for exchange, stage := range subTasks {
		for _, value := range stage {
			for routingKey, task := range value {

				taskRaw, err := proto.Marshal(task)

				if err != nil {
					log.Errorf("Failed to marshal task: %s", err)
					continue
				}

				w.rabbitMQ.Publish(exchange, routingKey, taskRaw)
				log.Debugf("Task %s sent to exchange %s with routing key %s", task, exchange, routingKey)
			}
		}
	}
}

func (w *Worker) Shutdown() {
	w.rabbitMQ.Close()
}

func (w *Worker) isRunning() bool {
	select {
	case <-w.done:
		return false
	default:
		return true
	}
}
