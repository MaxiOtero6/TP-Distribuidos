package server

import (
	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	common_model "github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/google/uuid"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	ID                  string
	serverSocket        *client_server_communication.Socket
	done                chan bool
	clientID            string
	clientSocket        *client_server_communication.Socket
	workerClusterConfig *common_model.WorkerClusterConfig
	rabbitMQ            *mom.RabbitMQ
	consumeChan         mom.ConsumerChan
	resultQueueName     string
	resultExchangeName  string
}

func NewServer(id string, address string, clusterConfig *common_model.WorkerClusterConfig) (*Server, error) {
	serverSocket, err := client_server_communication.CreateServerSocket(address)

	if err != nil {
		return nil, err
	}

	return &Server{
		ID:                  id,
		serverSocket:        serverSocket,
		done:                make(chan bool),
		workerClusterConfig: clusterConfig,
		rabbitMQ:            mom.NewRabbitMQ(),
	}, nil
}

// InitConfig initializes the server with the given exchanges, queues, and binds
func (s *Server) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	if len(binds) != 1 {
		log.Panicf("Expected exactly one binding to the results queue for servers, but got %d", len(binds))
	}

	// Do not bind the server to a queue without some clientId as routing key
	s.rabbitMQ.InitConfig(exchanges, queues, nil, s.ID)

	s.resultQueueName = binds[0]["queue"]
	s.resultExchangeName = binds[0]["exchange"]
}

// Register a new client with the server
// The client is identified by its ID
// The server binds the client ID to the result queue and exchange
func (s *Server) RegisterNewClient(clientId string) {
	// Check if the result exchange name is empty.
	// Do not check if the result queue name is empty, because it can be an anonymous queue
	if len(s.resultExchangeName) == 0 {
		log.Panicf("Result exchange name is empty, do you call InitConfig?")
	}

	s.rabbitMQ.BindQueue(s.resultQueueName, s.resultExchangeName, clientId)

	// If no consume channel for the result queue is set, create one
	// This is to avoid creating multiple consume channels for the same queue
	// and to ensure that the consume channel is created only once
	if s.consumeChan == nil {
		s.consumeChan = s.rabbitMQ.Consume(s.resultQueueName)
	}
}

func (s *Server) acceptConnections() {
	for {
		clientSocket, err := s.serverSocket.Accept()
		if err != nil {
			break
		}
		log.Infof("Client connected")

		s.handleConnection(clientSocket)
	}
}

func generateUniqueID() string {
	return uuid.NewString()
}

func (s *Server) getClientID() string {
	return s.clientID
}

func (s *Server) Run() {
	s.acceptConnections()
}

func (s *Server) Stop() {
	s.done <- true
	s.serverSocket.Close()
}
