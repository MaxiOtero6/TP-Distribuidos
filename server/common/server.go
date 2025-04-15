package common

import (
	"fmt"
	"time"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/server/model"
	"github.com/MaxiOtero6/TP-Distribuidos/server/utils"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Server struct {
	serverSocket *client_server_communication.Socket
	done         chan bool
	clientID     string
	clientSocket *client_server_communication.Socket
}

func NewServer(address string) (*Server, error) {
	serverSocket, err := client_server_communication.CreateServerSocket(address)
	if err != nil {
		return nil, err
	}

	return &Server{
		serverSocket: serverSocket,
		done:         make(chan bool),
	}, nil
}

func (s *Server) acceptConnections() {
	for {
		clientSocket, err := s.serverSocket.Accept()
		if err != nil {
			break
		}

		s.handleConnection(clientSocket)
	}
}

func (s *Server) handleMessage(clientSocket *client_server_communication.Socket, message *protocol.Message) error {
	clientServerMessage, ok := message.GetMessage().(*protocol.Message_ClientServerMessage)
	if !ok {
		return fmt.Errorf("unexpected message type: expected ServerClientMessage")

	}

	switch msg := clientServerMessage.ClientServerMessage.GetMessage().(type) {
	case *protocol.ClientServerMessage_Sync:
		s.handleConnectionMessage(clientSocket, msg.Sync)
	case *protocol.ClientServerMessage_Batch:
		s.handleBatchMessage(clientSocket, msg.Batch)
	case *protocol.ClientServerMessage_Finish:
		s.handleFinishMessage(msg.Finish)
	case *protocol.ClientServerMessage_Result:
		s.handleResultMessage(msg.Result)
	default:
		// Handle unknown message type
	}
	return nil
}

func (s *Server) handleConnection(clientSocket *client_server_communication.Socket) error {
	for {
		message, err := clientSocket.Read()
		if err != nil {
			return err
		}
		err = s.handleMessage(clientSocket, message)
		if err != nil {
			log.Errorf("Error handling message: %v", err)
			return err
		}

	}
	return nil
}

func (s *Server) handleConnectionMessage(clientSocket *client_server_communication.Socket, syncMessage *protocol.Sync) {
	clientID := generateUniqueID()

	// Enviar el ID al cliente
	idMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_SyncAck{
					SyncAck: &protocol.SyncAck{
						ClientId: clientID,
					},
				},
			},
		},
	}

	if err := clientSocket.Write(idMessage); err != nil {
		log.Errorf("Error sending ID to client: %v", err)
		return
	}

	s.clientID = clientID
	s.clientSocket = clientSocket

	log.Infof("Client connected with ID: %s", clientID)
}
func (s *Server) handleBatchMessage(clientSocket *client_server_communication.Socket, batchMessage *protocol.Batch) error {
	// Process the batch message
	// Tengo que recibir el batch
	// lo tengo que validar
	// para poder validarlo lo que tengo que hacer es en primero ver si el tipo de file es el esperado
	// y además quiero saber si

	err := s.validateBatchType(batchMessage)
	if err != nil {
		log.Errorf("Invalid batch type: %v", err)
		ackMessage := &protocol.Message{
			Message: &protocol.Message_ServerClientMessage{
				ServerClientMessage: &protocol.ServerClientMessage{
					Message: &protocol.ServerClientMessage_BatchAck{
						BatchAck: &protocol.BatchAck{
							Status: protocol.MessageStatus_FAIL,
						},
					},
				},
			},
		}
		if err := clientSocket.Write(ackMessage); err != nil {
			log.Errorf("Error sending batch acknowledgment: %v", err)
			return err
		}
	}

	// Parse the batch message based on its type

	if batchMessage.Type == protocol.FileType_MOVIES {
		s.processMovieBatch(batchMessage)
		//TODO valite erro, in case of error send ack with error
	} else if batchMessage.Type == protocol.FileType_CREDITS {
		s.processCreditBatch(batchMessage)
	} else if batchMessage.Type == protocol.FileType_RATINGS {
		s.processRatingsBatch(batchMessage)
	}

	// TODO Take a decesion about what to do with the batch and send it with rabbit

	log.Infof("Received batch message with type: %v", batchMessage.Type)

	ackMessage := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_BatchAck{
					BatchAck: &protocol.BatchAck{
						Status: protocol.MessageStatus_SUCCESS,
					},
				},
			},
		},
	}

	if err := clientSocket.Write(ackMessage); err != nil {
		log.Errorf("Error sending batch acknowledgment: %v", err)
		return err
	}

	return nil
}

func (s *Server) handleFinishMessage(finishMessage *protocol.Finish) {
	log.Infof("Received finish message from user: %v ", finishMessage.ClientId)
	//TODO
}

func (s *Server) handleResultMessage(resultMessage *protocol.Result) {
	log.Infof("Received result message from user: %v ", resultMessage.ClientId)
	//TODO
}

func (s *Server) processMovieBatch(batch *protocol.Batch) ([][]*model.Movie, error) {
	var movies [][]*model.Movie

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		movie := utils.ParseMovie(fields)
		if movie == nil {
			return nil, fmt.Errorf("Invalid movie data")
		}
		movies = append(movies, movie)

	}
	return movies, nil
}

func (s *Server) processCreditBatch(batch *protocol.Batch) ([][]*model.Actor, error) {
	var actors [][]*model.Actor

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		actor := utils.ParseCredit(fields)
		if actor == nil {
			return nil, fmt.Errorf("Invalid actor data")
		}
		actors = append(actors, actor)

	}
	return actors, nil
}

func (s *Server) processRatingsBatch(batch *protocol.Batch) ([][]*model.Rating, error) {
	var ratings [][]*model.Rating

	for _, row := range batch.Data {

		fields := utils.ParseLine(&row.Data)
		if fields == nil {
			return nil, fmt.Errorf("Invalid row data")
		}

		rating := utils.ParseRating(fields)
		if rating == nil {
			return nil, fmt.Errorf("Invalid rating data")
		}
		ratings = append(ratings, rating)

	}
	return ratings, nil
}

func (s *Server) validateBatchType(batchMessage *protocol.Batch) error {
	if batchMessage.Type != protocol.FileType_MOVIES &&
		batchMessage.Type != protocol.FileType_CREDITS &&
		batchMessage.Type != protocol.FileType_RATINGS {
		return fmt.Errorf("Invalid file type: %v", batchMessage.Type)
	}
	return nil
}

func generateUniqueID() string {
	return fmt.Sprintf("client-%d", time.Now().UnixNano())
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
