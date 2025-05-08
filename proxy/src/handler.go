package proxy

import (
	"errors"
	"strconv"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

var ErrSignalReceived = errors.New("signal received")
var log = logging.MustGetLogger("log")

type Handler struct {
	clientSocket  *client_server_communication.Socket
	isRunning     bool
	serverAddress []string
	currentIndex  int
}

func NewHandler(serverAddresses []string, addressIndex string, socketFd uintptr) (*Handler, error) {
	clientSocket, err := client_server_communication.NewClientSocketFromFile(socketFd)

	if err != nil {
		return nil, err
	}

	index, err := strconv.Atoi(addressIndex)
	if err != nil {
		return nil, errors.New("invalid addressIndex: must be an integer")
	}

	return &Handler{
		clientSocket:  clientSocket,
		isRunning:     true,
		serverAddress: serverAddresses,
		currentIndex:  index,
	}, nil
}

func (h *Handler) Run() error {
	defer h.clientSocket.Close()

	if err := h.handleConnection(); err != nil {
		if !h.isRunning {
			log.Infof("action: handleConnection | result: fail | error: %v", ErrSignalReceived)
		} else if err == client_server_communication.ErrConnectionClosed {
			log.Infof("action: handleConnection | result: fail | error: %v", err)
		} else {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
		return err
	}

	return nil
}

func (h *Handler) selectServer() (*client_server_communication.Socket, error) {
	if len(h.serverAddress) == 0 {
		return nil, errors.New("no servers available")
	}

	var connected bool
	var serverSocket *client_server_communication.Socket
	var err error

	for !connected && h.currentIndex < len(h.serverAddress) {

		serverAddress := h.serverAddress[h.currentIndex]
		log.Infof("action: selectServer | server: %s", serverAddress)

		serverSocket, err = client_server_communication.Connect(serverAddress)
		if err != nil {
			log.Errorf("action: selectServer | result: fail | error: %v", err)
			h.currentIndex++
			continue
		}

		connected = true
		log.Infof("action: selectServer | result: success | server: %s", serverAddress)
	}

	return serverSocket, nil
}

func (h *Handler) handleMessage(message *protocol.Message, serverSocket *client_server_communication.Socket) error {

	// Only send the message to the server
	err := serverSocket.Write(message)
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
		return err
	}

	message, err = serverSocket.Read()
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
	}

	err = h.clientSocket.Write(message)
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
	}

	return nil
}

func (h *Handler) handleConnection() error {

	serverSocket, err := h.selectServer()
	defer serverSocket.Close()

	if err != nil {
		log.Errorf("action: selectServer | result: fail | error: %v", err)
		return err
	}

	for h.isRunning {
		message, err := h.clientSocket.Read()
		if err != nil {
			return err
		}

		err = h.handleMessage(message, serverSocket)
		if err != nil {
			return err
		}
	}

	return nil
}

func (h *Handler) Stop() {
	h.isRunning = false

	if h.clientSocket != nil {
		h.clientSocket.Close()
		log.Infof("Client socket closed")
	}

}
