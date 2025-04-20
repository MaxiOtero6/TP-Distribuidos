package server

import (
	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	common_model "github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/src/rabbit"
	"github.com/google/uuid"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	ID            string
	serverSocket  *client_server_communication.Socket
	done          chan bool
	clientID      string
	clientSocket  *client_server_communication.Socket
	rabbitHandler *rabbit.RabbitHandler
}

func NewServer(id string, address string, infraConfig *common_model.InfraConfig) (*Server, error) {
	serverSocket, err := client_server_communication.CreateServerSocket(address)

	if err != nil {
		return nil, err
	}

	return &Server{
		ID:            id,
		serverSocket:  serverSocket,
		done:          make(chan bool),
		rabbitHandler: rabbit.NewRabbitHandler(infraConfig),
	}, nil
}

// InitConfig initializes the server with the given exchanges, queues, and binds
func (s *Server) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	s.rabbitHandler.InitConfig(s.ID, exchanges, queues, binds)
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
	s.rabbitHandler.Close()
}
