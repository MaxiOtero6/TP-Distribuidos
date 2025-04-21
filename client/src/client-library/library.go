package library

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/utils"
	client_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm/protocol"
	"github.com/op/go-logging"
)

const DELAY_MULTIPLIER = 2
const MAX_DELAY = 64 // max retries = 6
const QUERIES_AMOUNT = 5

var log = logging.MustGetLogger("log")

var ErrSignalReceived = errors.New("signal received")

type ClientConfig struct {
	ServerAddress   string
	MaxAmount       int
	MoviesFilePath  string
	RatingsFilePath string
	CreditsFilePath string
	ClientId        string
}

type Library struct {
	socket        *client_communication.Socket
	fileNames     []string
	sleepTime     time.Duration
	responseCount int
	done          chan bool
	isRunning     bool
	config        ClientConfig
}

func NewLibrary(config ClientConfig) (*Library, error) {

	socket, err := client_communication.Connect(config.ServerAddress)
	if err != nil {
		return nil, err
	}

	return &Library{
		socket:        socket,
		config:        config,
		sleepTime:     1,
		isRunning:     true,
		responseCount: 0,
	}, nil
}

func (l *Library) ProcessData() {

	err := l.sendSync()
	if err != nil {
		log.Errorf("action: sendSync | result: fail | error: %v", err)
		return
	}

	err = l.sendAllFiles()
	if err != nil {
		log.Errorf("action: sendAllFiles | result: fail | error: %v", err)
		return
	}

	err = l.sendFinishMessage()
	if err != nil {
		log.Errorf("action: sendFinishMessage | result: fail | error: %v", err)
		return
	}

	err = l.fetchServerResults()
	if err != nil {
		if err == ErrSignalReceived {
			log.Infof("action: fetchServerResults | result: success | error: %v", err)
			return
		}
		log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
		return
	}
}

func (l *Library) sendAllFiles() error {
	err := l.sendMoviesFile()
	if err != nil {
		return err
	}

	err = l.sendCreditsFile()
	if err != nil {
		return err
	}
	err = l.sendRatingsFile()
	if err != nil {
		return err
	}

	return nil
}

// Read line by line the file with the name pass by parameter and
// Send each line to the server and wait for confirmation
// until an OEF or the client is shutdown
func (l *Library) sendFile(filename string, fileType protocol.FileType) error {

	parser, err := utils.NewParser(l.config.MaxAmount, filename, fileType)
	if err != nil {
		return err
	}

	defer parser.Close()

	for l.isRunning {

		batch, err := parser.ReadBatch()
		if err != nil {
			if err == io.EOF {
				log.Infof("End of file reached for: %s", filename)
				break
			}
			return err
		}
		if err := l.socket.Write(batch); err != nil {
			return err
		}
		log.Info("Batch sent successfully, waiting for server response...")
		err = l.waitForSuccessServerResponse()
		if err != nil {
			log.Criticalf("action: batch_send | result: fail")
			return err
		}

	}

	return nil

}

func (l *Library) sendMoviesFile() error {
	err := l.sendFile(l.config.MoviesFilePath, protocol.FileType_MOVIES)
	if err != nil {
		return err
	}
	return nil
}
func (l *Library) sendRatingsFile() error {
	err := l.sendFile(l.config.RatingsFilePath, protocol.FileType_RATINGS)
	if err != nil {
		return err
	}
	return nil
}
func (l *Library) sendCreditsFile() error {
	err := l.sendFile(l.config.CreditsFilePath, protocol.FileType_CREDITS)
	if err != nil {
		return err
	}
	return nil
}

func (l *Library) sendSync() error {

	if !l.isRunning {
		return nil
	}
	syncMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Sync{
					Sync: &protocol.Sync{},
				},
			},
		},
	}

	if err := l.socket.Write(syncMessage); err != nil {
		return err
	}

	if err := l.waitForSuccessServerResponse(); err != nil {
		return err
	}

	return nil
}
func (l *Library) sendFinishMessage() error {

	if !l.isRunning {
		return nil
	}

	finishMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Finish{
					Finish: &protocol.Finish{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(finishMessage); err != nil {
		return err
	}
	log.Infof("action: SendFinishMessage | result: success | client_id: %v", l.config.ClientId)
	return nil
}
func (l *Library) fetchServerResults() error {

	for l.isRunning {

		if l.sleepTime > MAX_DELAY {
			return fmt.Errorf("timeout")
		}

		if l.socket == nil {
			if err := l.connectToServer(); err != nil {
				return err
			}
		}

		err := l.sendResultMessage()
		if err != nil {
			return err
		}

		done, response, err := l.waitForAllResultsServerResponse()
		if err != nil {
			return err
		}

		l.processRequestResponseMessage(response)

		if !done {
			l.disconnectFromServer()
			// Exponential backoff
			l.sleepTime *= DELAY_MULTIPLIER
			time.Sleep(l.sleepTime * time.Second)
			continue
		}
		break
	}

	return nil
}
func (l *Library) processRequestResponseMessage(message []*protocol.Request_Query) {
	if len(message) == 0 {
		log.Infof("action: processResultMessage | result: fail | error: empty response")
		return
	}
	for _, query := range message {
		log.Infof("action:  | result: success | query number : %v | response: %v", query.Type, query.Message)
	}
}
func (l *Library) waitForResultServerResponse() (bool, []*protocol.Request_Query, error) {
	response, err := l.waitForServerResponse()
	if err != nil {
		return false, nil, err
	}

	switch resp := response.GetMessage().(type) {
	case *protocol.ServerClientMessage_Request:
		if resp.Request.Status == protocol.MessageStatus_SUCCESS {
			return true, resp.Request.Responses, nil
		} else if resp.Request.Status == protocol.MessageStatus_PENDING {
			return false, nil, nil
		}
	}
	return false, nil, fmt.Errorf("unexpected response type received")

}
func (l *Library) waitForAllResultsServerResponse() (bool, []*protocol.Request_Query, error) {
	done, responses, err := l.waitForResultServerResponse()
	if err != nil {
		return false, nil, err
	}
	if done {
		l.responseCount++
		if l.responseCount == QUERIES_AMOUNT {
			l.isRunning = false
		}
	}
	return done, responses, nil

}
func (l *Library) connectToServer() error {
	var err error
	l.socket, err = client_communication.Connect(l.config.ServerAddress)

	if err != nil {
		log.Infof("action: ConncetToServer | result: false | address: %v", l.config.ServerAddress)
		return err
	}

	log.Infof("action: ConncetToServer | result: success | address: %v", l.config.ServerAddress)
	return nil
}
func (l *Library) disconnectFromServer() {
	if l.socket != nil {
		l.socket.Close()
		log.Infof("action: disconnectFromServer | result: success | address: %v", l.config.ServerAddress)
	}
	l.socket = nil
}

func (l *Library) waitForServerResponse() (*protocol.ServerClientMessage, error) {
	response, err := l.socket.Read()
	if err != nil {
		if !l.isRunning {
			return nil, ErrSignalReceived
		}
		return nil, err
	}

	serverClientMessage, ok := response.GetMessage().(*protocol.Message_ServerClientMessage)
	if !ok {
		return nil, fmt.Errorf("unexpected message type: expected ServerClientMessage")
	}

	return serverClientMessage.ServerClientMessage, nil
}

func (l *Library) waitForSuccessServerResponse() error {
	response, err := l.waitForServerResponse()
	if err != nil {
		return err
	}

	switch resp := response.GetMessage().(type) {
	case *protocol.ServerClientMessage_BatchAck:
		if resp.BatchAck.Status == protocol.MessageStatus_SUCCESS {
			log.Infof("action: receiveBatchAckResponse | result: success | response: %v", resp.BatchAck.Status)

		} else {
			log.Errorf("action: receiveBatchAckResponse | result: fail | error: %v", resp.BatchAck.Status)
			return fmt.Errorf("server response was not succes")
		}

	case *protocol.ServerClientMessage_SyncAck:
		l.config.ClientId = resp.SyncAck.ClientId
		log.Infof("action: receiveSyncAckResponse | result: success | clientId: %v", l.config.ClientId)

	default:
		log.Errorf("action: receiveResponse | result: fail | error: Unexpected response type received")
		return fmt.Errorf("unexpected response type received")
	}

	return nil
}

func (l *Library) sendResultMessage() error {
	consultMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Result{
					Result: &protocol.Result{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}
	if err := l.socket.Write(consultMessage); err != nil {
		return err
	}
	return nil
}

func (l *Library) Stop() {
	l.isRunning = false
	l.disconnectFromServer()
}
