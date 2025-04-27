package server

import (
	"errors"
	"os"
	"os/exec"
	"syscall"

	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")
var ErrSignalReceived = errors.New("signal received")

type Server struct {
	ID           string
	serverSocket *client_server_communication.Socket
	isRunning    bool
	clients      []*exec.Cmd
}

func NewServer(id string, address string) (*Server, error) {
	serverSocket, err := client_server_communication.CreateServerSocket(address)
	if err != nil {
		return nil, err
	}

	return &Server{
		ID:           id,
		serverSocket: serverSocket,
		isRunning:    true,
		clients:      make([]*exec.Cmd, 0),
	}, nil
}

func (s *Server) acceptConnections() error {
	for s.isRunning {
		clientSocket, err := s.serverSocket.Accept()
		if err != nil {
			if !s.isRunning {
				log.Infof("action: ShutdownServer | result: success | error: %v", err)
				continue
			} else {
				log.Errorf("action: acceptConnections | result: fail | error: %v", err)
				return err
			}
		}

		defer clientSocket.Close()

		log.Infof("Client connected")

		err = s.handleConnection(clientSocket)
		if err != nil {
			log.Errorf("action: handleConnection | result: fail | error: %v", err)
		}
	}

	return nil
}

func (s *Server) handleConnection(clientSocket *client_server_communication.Socket) error {
	cmd := exec.Command(os.Args[0], "child") // Fork the current binary
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr

	socketFD, err := clientSocket.GetFileDescriptor()

	if err != nil {
		log.Errorf("action: getFileDescriptor | result: fail | error: %v", err)
		return err
	}

	cmd.ExtraFiles = []*os.File{socketFD}

	err = cmd.Start()
	if err != nil {
		log.Errorf("action: forkChildProcess | result: fail | error: %v", err)
		return err
	}

	log.Infof("Child process started with PID: %d", cmd.Process.Pid)

	s.clients = append(s.clients, cmd)

	return nil
}

func (s *Server) Run() error {
	return s.acceptConnections()
}

func (s *Server) Stop() {
	s.isRunning = false

	if s.serverSocket != nil {
		s.serverSocket.Close()
		s.serverSocket = nil
		log.Info("Server socket closed")
	}

	// signal
	for _, client := range s.clients {
		err := client.Process.Signal(syscall.SIGTERM)

		if err != nil {
			log.Errorf("Failed to send SIGTERM to child process with PID %d: %v", client.Process.Pid, err)
			continue
		}

		log.Infof("Sent SIGTERM to child process with PID %d", client.Process.Pid)
	}

	// wait
	for _, client := range s.clients {
		status, err := client.Process.Wait()

		if err != nil {
			log.Errorf("Failed to wait for child process with PID %d: %v", client.Process.Pid, err)
			continue
		}

		log.Infof("Child process with PID %d terminated, exit code: %d", client.Process.Pid, status)
	}
}
