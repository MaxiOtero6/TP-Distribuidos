package server

import (
	"fmt"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

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
			return err
		}
	}

}

func (s *Server) handleConnectionMessage(clientSocket *client_server_communication.Socket, syncMessage *protocol.Sync) {
	clientID := generateUniqueID()

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

	s.rabbitHandler.RegisterNewClient(clientID)

	s.clientID = clientID
	s.clientSocket = clientSocket

	log.Infof("Client connected with ID: %s", clientID)
}

func (s *Server) handleBatchMessage(clientSocket *client_server_communication.Socket, batchMessage *protocol.Batch) error {
	//log.Debugf("Received batch message: %v ", batchMessage)
	clientId := batchMessage.GetClientId()

	switch batchMessage.Type {
	case protocol.FileType_MOVIES:
		movies := s.processMoviesBatch(batchMessage)
		s.rabbitHandler.SendMoviesRabbit(movies, clientId, batchMessage.GetEOF())
	case protocol.FileType_CREDITS:
		// actors := s.processCreditsBatch(batchMessage)
		// s.rabbitHandler.SendActorsRabbit(actors, clientId, batchMessage.GetEOF())
	case protocol.FileType_RATINGS:
		// ratings := s.processRatingsBatch(batchMessage)
		// s.rabbitHandler.SendRatingsRabbit(ratings, clientId, batchMessage.GetEOF())
	default:
		log.Errorf("Invalid batch type: %v", batchMessage.Type)

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

		return fmt.Errorf("invalid batch type: %v", batchMessage.Type)
	}

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

func (s *Server) handleResultMessage(resultMessage *protocol.Result) error {
	log.Infof("Received result message from user: %v ", resultMessage.ClientId)

	results := s.rabbitHandler.GetResults(resultMessage.ClientId)

	message := &protocol.Message{
		Message: &protocol.Message_ServerClientMessage{
			ServerClientMessage: &protocol.ServerClientMessage{
				Message: &protocol.ServerClientMessage_Results{
					Results: results,
				},
			},
		},
	}

	if err := s.clientSocket.Write(message); err != nil {
		log.Errorf("Error sending results: %v", err)
		return err
	}

	return nil
}
