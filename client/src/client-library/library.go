package library

import (
	"errors"
	"fmt"
	"io"
	"math"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/model"
	"github.com/MaxiOtero6/TP-Distribuidos/client/src/client-library/utils"
	client_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

const SLEEP_TIME time.Duration = 1 * time.Second
const DELAY_MULTIPLIER int = 2
const MAX_RETRIES float64 = 6.0
const QUERIES_AMOUNT int = 5

var log = logging.MustGetLogger("log")

var ErrSignalReceived = errors.New("signal received")

type ClientConfig struct {
	ServerAddress   string
	BatchMaxSize    int
	MoviesFilePath  string
	RatingsFilePath string
	CreditsFilePath string
	ClientId        string
}

type Library struct {
	socket        *client_communication.Socket
	fileNames     []string
	retryNumber   float64
	responseCount int
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
		retryNumber:   0,
		isRunning:     true,
		responseCount: 0,
	}, nil
}

func (l *Library) ProcessData() (results *model.Results) {
	err := l.sendSync()
	if err != nil {
		if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: sendSync | result: fail | error: %v", err)
			return
		}
		log.Errorf("action: sendSync | result: fail | error: %v", err)
		return
	}

	err = l.sendAllFiles()
	if err != nil {
		if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: sendAllFiles | result: fail | error: %v", err)
			return
		}
		log.Errorf("action: sendAllFiles | result: fail | error: %v", err)
		return
	}

	results, err = l.fetchServerResults()

	if err != nil {
		if err == ErrSignalReceived {
			log.Infof("action: fetchServerResults | result: success | error: %v", err)
			return
		} else if err == client_communication.ErrConnectionClosed {
			log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
			return
		}

		log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
		return
	}

	if results == nil && l.isRunning {
		log.Errorf("action: fetchServerResults | result: fail | error: results is nil")
	}

	return results
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
	log.Infof("action: sendFile | result: start | file: %s", filename)

	parser, err := utils.NewParser(l.config.BatchMaxSize, filename, fileType)
	if err != nil {
		return err
	}

	defer parser.Close()

	for l.isRunning {

		batch, fileErr := parser.ReadBatch(l.config.ClientId)

		if fileErr != io.EOF && fileErr != nil {
			return err
		}

		if err := l.socket.Write(batch); err != nil {
			return err
		}

		log.Debugf("action: batch_send | result: success | clientId: %v |  file: %s", l.config.ClientId, filename)

		err = l.waitForSuccessServerResponse()
		if err != nil {
			log.Criticalf("action: batch_send | result: fail | clientId: %v | file: %s", l.config.ClientId, filename)
			return err
		}

		if fileErr == io.EOF {
			break
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
					Sync: &protocol.Sync{
						ClientId: l.config.ClientId,
					},
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
	if l.socket == nil {
		return nil
	}

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

func (l *Library) sendDisconnectMessage() error {
	if !l.isRunning {
		return nil
	}

	disconnectMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Disconnect{
					Disconnect: &protocol.Disconnect{
						ClientId: l.config.ClientId,
					},
				},
			},
		},
	}

	if err := l.socket.Write(disconnectMessage); err != nil {
		return err
	}
	log.Infof("action: SendDisconnectMessage | result: success | client_id: %v", l.config.ClientId)
	return nil
}

func (l *Library) fetchServerResults() (*model.Results, error) {
	resultParser := utils.NewResultParser()

	for l.isRunning && !resultParser.IsDone() {

		if l.retryNumber > MAX_RETRIES {
			return nil, fmt.Errorf("timeout")
		}

		if l.socket == nil {
			if err := l.connectToServer(); err != nil {
				return nil, err
			}
		}

		start := time.Now()
		err := l.sendResultMessage()
		if err != nil {
			return nil, err
		}

		ok, response, err := l.waitForResultServerResponse()

		if err != nil {
			return nil, err
		}

		jitter := time.Since(start)

		log.Debugf("action: waitForResultServerResponse | result: success | response: %v", response)

		if !ok {
			l.sendDisconnectMessage()
			l.disconnectFromServer()
			// Exponential backoff + jitter
			sleepTime := SLEEP_TIME*(time.Duration(math.Pow(2.0, l.retryNumber))) + jitter
			l.retryNumber++
			log.Warningf("action: waitForResultServerResponse | result: fail | retryNumber: %v | sleepTime: %v", l.retryNumber, sleepTime)
			time.Sleep(sleepTime)
			continue
		}

		resultParser.Save(response)
	}

	return resultParser.GetResults(), nil
}

func (l *Library) waitForResultServerResponse() (bool, *protocol.ResultsResponse, error) {
	response, err := l.waitForServerResponse()
	if err != nil {
		return false, nil, err
	}

	switch resp := response.GetMessage().(type) {
	case *protocol.ServerClientMessage_Results:
		if resp.Results.Status == protocol.MessageStatus_SUCCESS {
			return true, response.GetResults(), nil
		} else if resp.Results.Status == protocol.MessageStatus_PENDING {
			return false, nil, nil
		}
	}
	return false, nil, fmt.Errorf("unexpected response type received")

}

func (l *Library) connectToServer() error {
	var err error
	l.socket, err = client_communication.Connect(l.config.ServerAddress)

	if err != nil {
		log.Infof("action: ConnectToServer | result: false | address: %v", l.config.ServerAddress)
		return err
	}

	log.Infof("action: ConnectToServer | result: success | address: %v", l.config.ServerAddress)

	err = l.sendSync()
	if err != nil {
		return err
	}

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
	log.Debugf("action: waitForServerResponse | result: start")
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
			log.Debugf("action: receiveBatchAckResponse | result: success | response: %v", resp.BatchAck.Status)

		} else {
			log.Errorf("action: receiveBatchAckResponse | result: fail | error: %v", resp.BatchAck.Status)
			return fmt.Errorf("server response was not succes")
		}

	case *protocol.ServerClientMessage_SyncAck:
		l.config.ClientId = resp.SyncAck.ClientId
		log.Debugf("action: receiveSyncAckResponse | result: success | clientId: %v", l.config.ClientId)

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

	log.Debugf("action: sendResultMessage | result: success | clientId: %v", l.config.ClientId)

	return nil
}

func (l *Library) Stop() {
	l.sendFinishMessage()
	l.isRunning = false
	l.disconnectFromServer()
}

func (l *Library) DumpResultsToJson(filePath string, results *model.Results) {
	utils.DumpResultsToJson(filePath, results)
}
