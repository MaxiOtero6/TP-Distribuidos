package rabbit

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
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

// Close closes the RabbitMQ connection
// and the channel used for consuming messages
func (r *RabbitHandler) Close() {
	r.rabbitMQ.Close()
}

// SendMoviesRabbit sends the movies to the filter and overview exchanges
func (r *RabbitHandler) SendMoviesRabbit(movies []*model.Movie) {
	alphaTasks := utils.GetAlphaStageTask(movies, r.infraConfig.GetFilterCount())
	// muTasks := utils.GetMuStageTask(movies, r.infraConfig.GetOverviewCount())
	r.publishTasksRabbit(alphaTasks, r.infraConfig.GetFilterExchange())
	// r.publishTasksRabbit(muTasks, r.infraConfig.GetOverviewExchange())
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

// GetResults retrieves the results from the result queue
// The results are unmarshalled and returned as a ResultsResponse
func (r *RabbitHandler) GetResults(clientId string) *protocol.ResultsResponse {
	unmarshallResult := func(msg amqp.Delivery) (*protocol.ResultsResponse_Result, error) {
		task := &protocol.Task{}
		err := proto.Unmarshal(msg.Body, task)
		
		if err != nil {
			return nil, err
		}
		
		result := &protocol.ResultsResponse_Result{}
		
		switch task.GetStage().(type) {
		case *protocol.Task_Result1:
			result.Message = &protocol.ResultsResponse_Result_Result1{
				Result1: task.GetResult1(),
			}
		case *protocol.Task_Result2:
			result.Message = &protocol.ResultsResponse_Result_Result2{
				Result2: task.GetResult2(),
			}
		case *protocol.Task_Result3:
			result.Message = &protocol.ResultsResponse_Result_Result3{
				Result3: task.GetResult3(),
			}
		case *protocol.Task_Result4:
			result.Message = &protocol.ResultsResponse_Result_Result4{
				Result4: task.GetResult4(),
			}
		case *protocol.Task_Result5:
			result.Message = &protocol.ResultsResponse_Result_Result5{
				Result5: task.GetResult5(),
			}
		default:
			return nil, fmt.Errorf("unknown task stage: %v", task.GetStage())
		}
		
		return result, nil
	}

	r.consumeChan = r.rabbitMQ.Consume(r.resultQueueName)

	results := &protocol.ResultsResponse{
		Results: make([]*protocol.ResultsResponse_Result, 0),
		Status:  protocol.MessageStatus_PENDING,
	}

	select {
	case msg := <-r.consumeChan:
		r, err := unmarshallResult(msg)

		if err != nil {
			log.Errorf("Error unmarshalling task: %v", err)
		} else {
			results.Results = append(results.Results, r)
		}

	default:
		r.rabbitMQ.CancelConsumer(r.resultQueueName)
		// Consume until channel closes to get inflight messages
		for {
			msg, ok := <-r.consumeChan

			if !ok {
				break
			}

			r, err := unmarshallResult(msg)

			if err != nil {
				log.Errorf("Error unmarshalling task: %v", err)
				continue
			}

			results.Results = append(results.Results, r)
		}
	}

	if len(results.Results) > 0 {
		results.Status = protocol.MessageStatus_SUCCESS
	}

	return results
}
