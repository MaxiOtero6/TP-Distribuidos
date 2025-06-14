package worker

import (
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/actions"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

// Worker is a struct that represents a worker that consumes tasks from a message queue and executes them
type Worker struct {
	WorkerId    string
	rabbitMQ    *mom.RabbitMQ
	action      actions.Action
	done        chan os.Signal
	consumeChan mom.ConsumerChan
	eofChan     mom.ConsumerChan
}

// NewWorker creates a new worker with the given id, type, and infraConfig
// and initializes RabbitMQ and action structs
// It also takes a signal channel to handle SIGTERM signal
func NewWorker(workerType string, infraConfig *model.InfraConfig, signalChan chan os.Signal) *Worker {
	rabbitMQ := mom.NewRabbitMQ()

	action := actions.NewAction(workerType, infraConfig)

	return &Worker{
		WorkerId:    infraConfig.GetNodeId(),
		rabbitMQ:    rabbitMQ,
		action:      action,
		done:        signalChan,
		consumeChan: nil,
	}
}

// InitConfig initializes the worker with the given exchanges, queues, and binds
// It expects to load one bind from the config file
// If the number of binds is not equal to 1, it panics
// It also initializes the RabbitMQ with the given exchanges, queues, and binds
// and sets the consume channel to the queue specified in the bind
// The workerId is used as routingKey for the bind
func (w *Worker) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	w.rabbitMQ.InitConfig(exchanges, queues, binds, w.WorkerId)
	w.consumeChan = w.rabbitMQ.Consume(binds[0]["queue"])
	w.eofChan = w.rabbitMQ.Consume(binds[1]["queue"])
}

// Run starts the worker and listens for messages on the consume channel
// It unmarshals the task from the message body and executes it using the action struct
// It also sends the subTasks to the RabbitMQ for each exchange and routing key
// If the task fails to unmarshal or execute, it logs the error and continues to the next message
// It also acknowledges the message after processing it
// The worker will run until it receives a signal on the done channel
// After that, it will close the RabbitMQ connection
// and exit
// It panics if the consume channel is nil
func (w *Worker) Run() {
	if w.consumeChan == nil || w.eofChan == nil {
		log.Panicf("Consume channel is nil, did you call InitConfig?")
	}

	defer w.Shutdown()

outer:
	for {
		select {
		case <-w.done:
			log.Infof("Worker %s received SIGTERM", w.WorkerId)
			break outer

		case message, ok := <-w.eofChan:
			if !ok {
				log.Warningf("Worker %s eof consume channel closed", w.WorkerId)
				break outer
			}
			w.handleMessage(&message)

		case message, ok := <-w.consumeChan:
			if !ok {
				log.Warningf("Worker %s consume channel closed", w.WorkerId)
				break outer
			}
			w.handleMessage(&message)
		}
	}

	log.Infof("Worker stop running gracefully")
}

// handleMessage handles the incoming message from the RabbitMQ
// It unmarshals the task from the message body and executes it using the action struct
// It also sends the subTasks to the RabbitMQ for each exchange and routing key
// If the task fails to unmarshal or execute, it logs the error and continues to the next message
// It also acknowledges the message after processing it
func (w *Worker) handleMessage(message *amqp.Delivery) {
	taskRaw := message.Body

	task := &protocol.Task{}

	err := proto.Unmarshal(taskRaw, task)

	if err != nil {
		log.Errorf("Failed to unmarshal task: %s", err)
		message.Reject(false)
		return
	}

	subTasks, err := w.action.Execute(task)

	if err != nil {
		log.Errorf("Failed to execute task: %s", err)
		message.Reject(false)
		return
	}

	w.sendSubTasks(subTasks)
	message.Ack(false)
}

// sendSubTasks sends the subTasks to the RabbitMQ for each exchange and routing key
// It marshals the task to a byte array and publishes it to the RabbitMQ
// It logs the task, exchange, and routing key for debugging purposes
// This function is nil-safe, meaning it will not panic if the input is nil
// It will simply return without doing anything
func (w *Worker) sendSubTasks(subTasks common.Tasks) {
	sendTask := func(exchange, routingKey string, task *protocol.Task) {
		taskRaw, err := proto.Marshal(task)

		if err != nil {
			log.Errorf("Failed to marshal task: %s", err)
			return
		}

		w.rabbitMQ.Publish(exchange, routingKey, taskRaw)
		log.Debugf("Task %T sent to exchange '%s' with routing key '%s'", task.GetStage(), exchange, routingKey)
	}

	for exchange, stages := range subTasks {
		for _, stage := range stages {
			for routingKey, task := range stage {
				if task.GetRingEOF() != nil || task.GetOmegaEOF() != nil {
					defer sendTask(exchange, routingKey, task)
					continue
				}

				sendTask(exchange, routingKey, task)
			}
		}
	}
}

// Shutdown closes the RabbitMQ connection
// It is safe to call this method multiple times
func (w *Worker) Shutdown() {
	w.rabbitMQ.Close()
}
