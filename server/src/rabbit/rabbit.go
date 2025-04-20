package rabbit

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
	common_model "github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/utils"
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

// RabbitHandler handles the RabbitMQ connection and message publishing for the server
type RabbitHandler struct {
	rabbitMQ           *mom.RabbitMQ
	infraConfig        *common_model.InfraConfig
	resultQueueName    string
	resultExchangeName string
	consumeChan        mom.ConsumerChan
}

// NewRabbitHandler creates a new RabbitHandler instance
func NewRabbitHandler(infraConfig *common_model.InfraConfig) *RabbitHandler {
	return &RabbitHandler{
		rabbitMQ:    mom.NewRabbitMQ(),
		infraConfig: infraConfig,
	}
}

// InitConfig initializes the RabbitMQ connection
// and sets up the exchanges, queues, and bindings
func (r *RabbitHandler) InitConfig(id string, exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	if len(binds) != 1 {
		log.Panicf("Expected exactly one binding to the results queue for servers, but got %d", len(binds))
	}

	// Do not bind the server to a queue without some clientId as routing key.
	// This routingKey param does nothing
	r.rabbitMQ.InitConfig(exchanges, queues, nil, id)

	r.resultQueueName = binds[0]["queue"]
	r.resultExchangeName = binds[0]["exchange"]
}

// Register a new client with the server
// The client is identified by its ID
// The server binds the client ID to the result queue and exchange
func (r *RabbitHandler) RegisterNewClient(clientId string) {
	// Check if the result exchange name is empty.
	// Do not check if the result queue name is empty, because it can be an anonymous queue
	if len(r.resultExchangeName) == 0 {
		log.Panicf("Result exchange name is empty, do you call InitConfig?")
	}

	r.rabbitMQ.BindQueue(r.resultQueueName, r.resultExchangeName, clientId)

	// If no consume channel for the result queue is set, create one
	// This is to avoid creating multiple consume channels for the same queue
	// and to ensure that the consume channel is created only once
	if r.consumeChan == nil {
		r.consumeChan = r.rabbitMQ.Consume(r.resultQueueName)
	}
}

// ConsumeResult consumes a message from the result queue
func (r *RabbitHandler) ConsumeResult() amqp.Delivery {
	return <-r.consumeChan
}

// Close closes the RabbitMQ connection
// and the channel used for consuming messages
func (r *RabbitHandler) Close() {
	r.rabbitMQ.Close()
}

// SendMoviesRabbit sends the movies to the filter and overview exchanges
func (r *RabbitHandler) SendMoviesRabbit(movies []*model.Movie) {
	alphaTasks := utils.GetAlphaStageTask(movies)
	muTasks := utils.GetMuStageTask(movies)

	r.publishTasksRabbit(alphaTasks, r.infraConfig.GetFilterExchange())
	r.publishTasksRabbit(muTasks, r.infraConfig.GetOverviewExchange())
}

// SendRatingsRabbit sends the ratings to the join exchange
// The ratings are shuffled by the join count hashing
func (r *RabbitHandler) SendRatingsRabbit(ratings []*model.Rating) {
	zetaTasks := utils.GetZetaStageRatingsTask(ratings, r.infraConfig.GetJoinCount())
	r.publishTasksRabbit(zetaTasks, r.infraConfig.GetJoinExchange())
}

// SendActorsRabbit sends the actors to the join exchange
// The actors are shuffled by the join count hashing
func (r *RabbitHandler) SendActorsRabbit(actors []*model.Actor) {
	iotaTasks := utils.GetIotaStageCreditsTask(actors, r.infraConfig.GetJoinCount())
	r.publishTasksRabbit(iotaTasks, r.infraConfig.GetJoinExchange())
}

// publishTasksRabbit publishes the tasks to the specified exchange
// The routing key is the task key in the tasks map
func (r *RabbitHandler) publishTasksRabbit(tasks map[string]*protocol.Task, exchange string) {
	for routingKey, task := range tasks {
		bytes, err := proto.Marshal(task)

		if err != nil {
			log.Errorf("Error marshalling %T task: %v", task.GetStage(), err)
			continue
		}

		r.rabbitMQ.Publish(exchange, routingKey, bytes)
	}
}