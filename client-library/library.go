package library

import (
	"errors"
	"fmt"
	"io"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/client-library/utils"
	client_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-comm"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-comm/protocol"
	"github.com/op/go-logging"
)

const MOVIES_FILE = "movies_head_10k.csv"
const RATINGS_FILE = "ratings_head_10k.csv"
const CREDITS_FILE = "credits_head_10k.csv"

const DELAY_MULTIPLIER = 2
const MAX_DELAY = 64 // max retries = 6
const QUERIES_AMOUNT = 5

var log = logging.MustGetLogger("log")

var ErrSignalReceived = errors.New("signal received")

type Library struct {
	parser        *utils.Parser
	socket        *client_communication.Socket
	fileNames     []string
	clientId      string
	ServerAddress string
	sleepTime     time.Duration
	running       bool
	responseCount int
	done          chan bool
}

func NewLibrary(fileNames []string, maxBatch int, address string) (*Library, error) {
	if len(fileNames) == 0 {
		err := fmt.Errorf("no files provided")
		return nil, err
	}

	parser, err := utils.NewParser(maxBatch, fileNames[0])
	if err != nil {
		return nil, err
	}

	socket, err := client_communication.Connect(address)
	if err != nil {
		return nil, err
	}

	return &Library{
		parser:        parser,
		socket:        socket,
		fileNames:     fileNames,
		clientId:      "",
		ServerAddress: address,
		sleepTime:     1,
		running:       true,
		responseCount: 0,
		done:          make(chan bool, 1),
	}, nil
}

func (l *Library) isDone() bool {
	select {
	case <-l.done:
		return true
	default:
	}
	return false
}

func (l *Library) ProcessData() {

	err := l.sendSync()
	if err != nil {
		log.Errorf("action: sendSync | result: fail | error: %v", err)
	}

	err = l.sendAllBathcs()
	if err != nil {
		log.Errorf("action: newMethod | result: fail | error: %v", err)
	}

	err = l.sendFinishMessage()
	if err != nil {
		log.Errorf("action: sendFinishMessage | result: fail | error: %v", err)
	}

	err = l.fetchServerResults()
	if err != nil {
		log.Errorf("action: fetchServerResults | result: fail | error: %v", err)
	}
}

func (l *Library) sendAllBathcs() error {
	for len(l.fileNames) > 0 {

		currentFile := l.fileNames[0]
		fileType := l.mapFileNameToFileType(currentFile)

		if err := l.parser.LoadNewFile(currentFile); err != nil {
			return err
		}

		for {
			batch, err := l.parser.ReadBatch(fileType)
			if err != nil {
				if err == io.EOF {
					//TODO
					break
				}
				return err
			}
			if err := l.socket.Write(batch); err != nil {
				return err
			}

			err = l.waitForSuccessServerResponse()
			if err != nil {
				log.Criticalf("action: batch_send | result: fail | amount: %v", len(batch.GetBatch().Data))
				return err
			}

		}

		// Eliminar el archivo procesado de la lista
		l.fileNames = l.fileNames[1:]
	}
	return nil
}

func (l *Library) sendSync() error {

	done := l.isDone()
	if done {
		return ErrSignalReceived
	}

	syncMessage := &protocol.SendMessage{
		Message: &protocol.SendMessage_Sync{
			Sync: &protocol.Sync{},
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
	done := l.isDone()
	if done {
		return ErrSignalReceived
	}

	finishMessage := &protocol.SendMessage{
		Message: &protocol.SendMessage_Finish{
			Finish: &protocol.Finish{
				ClientId: l.clientId,
			},
		},
	}
	if err := l.socket.Write(finishMessage); err != nil {
		return err
	}
	return nil
}

func (l *Library) fetchServerResults() error {

	for l.running {
		done := l.isDone()
		if done {
			return ErrSignalReceived
		}

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
	case *protocol.ResponseMessage_Request:
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
			l.running = false
		}
	}
	return done, responses, nil

}

func (l *Library) connectToServer() error {
	var err error
	l.socket, err = client_communication.Connect(l.ServerAddress)

	if err != nil {
		return err
	}

	return nil
}

func (l *Library) disconnectFromServer() {
	if l.socket != nil {
		l.socket.Close()
		log.Infof("action: disconnectFromServer | result: success | address: %v", l.ServerAddress)
	}
	l.socket = nil
}

func (l *Library) waitForServerResponse() (*protocol.ResponseMessage, error) {
	response, err := l.socket.Read()

	if err != nil {
		return nil, err
	}

	return response, nil
}

func (l *Library) waitForSuccessServerResponse() error {
	response, err := l.waitForServerResponse()
	if err != nil {
		return err
	}
	switch resp := response.GetMessage().(type) {
	case *protocol.ResponseMessage_BatchAck:
		if resp.BatchAck.Status == protocol.MessageStatus_SUCCESS {
			log.Infof("action: receiveBatchAckResponse | result: success | response: %v", resp.BatchAck.Status)

		} else {
			log.Errorf("action: receiveBatchAckResponse | result: fail | error: %v", resp.BatchAck.Status)
			return fmt.Errorf("server response was not succes")
		}

	case *protocol.ResponseMessage_SyncAck:
		l.clientId = resp.SyncAck.ClientId
		log.Infof("action: receiveSyncAckResponse | result: success | clientId: %v", l.clientId)

	default:
		log.Errorf("action: receiveResponse | result: fail | error: Unexpected response type received")
		return fmt.Errorf("unexpected response type received")
	}

	return nil
}

func (l *Library) sendResultMessage() error {
	consultMessage := &protocol.SendMessage{
		Message: &protocol.SendMessage_Result{
			Result: &protocol.Result{
				ClientId: l.clientId,
			},
		},
	}
	if err := l.socket.Write(consultMessage); err != nil {
		return err
	}
	return nil
}

func (l *Library) mapFileNameToFileType(fileName string) protocol.FileType {
	var fileType protocol.FileType

	if fileName == MOVIES_FILE {
		fileType = protocol.FileType_MOVIES
	} else if fileName == RATINGS_FILE {
		fileType = protocol.FileType_RATINGS
	} else if fileName == CREDITS_FILE {
		fileType = protocol.FileType_CREDITS
	}

	return fileType
}

func (l *Library) Stop() {
	close(l.done)
	l.disconnectFromServer()
	l.parser.Close()
	l.running = false
}
