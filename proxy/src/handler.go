package proxy

import (
	"errors"
	"math/rand"
	"time"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

var ErrSignalReceived = errors.New("signal received")
var log = logging.MustGetLogger("log")

type Handler struct {
	socket        *client_server_communication.Socket
	isRunning     bool
	serverAddress []string
	currentIndex  int
}

func NewHandler(serverAddresses []string, socketFd uintptr) (*Handler, error) {
	socket, err := client_server_communication.NewClientSocketFromFile(socketFd)

	if err != nil {
		return nil, err
	}

	rand.Seed(time.Now().UnixNano())
	currentIndex := rand.Intn(len(serverAddresses))

	return &Handler{
		socket:        socket,
		isRunning:     true,
		serverAddress: serverAddresses,
		currentIndex:  currentIndex,
	}, nil
}

func (h *Handler) Run() error {
	defer h.socket.Close()

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

	serverAddress := h.serverAddress[h.currentIndex]
	h.currentIndex = (h.currentIndex + 1) % len(h.serverAddress)

	log.Infof("action: selectServer | server: %s", serverAddress)

	serverSocket, err := client_server_communication.Connect(serverAddress)
	if err != nil {
		log.Errorf("action: selectServer | result: fail | error: %v", err)
		return nil, err
	}

	log.Infof("action: selectServer | result: success | server: %s", serverAddress)

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

	err = h.socket.Write(message)
	if err != nil {
		log.Errorf("action: handleMessage | result: fail | error: %v", err)
	}

	return nil
}

func (h *Handler) handleConnection() error {

	serverSocket, err := h.selectServer()
	if err != nil {
		log.Errorf("action: selectServer | result: fail | error: %v", err)
		return err
	}

	for h.isRunning {
		message, err := h.socket.Read()
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

func (c *Handler) Stop() {
	c.isRunning = false

	if c.socket != nil {
		c.socket.Close()
		log.Infof("Client socket closed")
	}

}
