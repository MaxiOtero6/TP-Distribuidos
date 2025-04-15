package client_server_communication

import (
	"bufio"
	"net"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm/protocol"
	"google.golang.org/protobuf/proto"
)

const COMMUNICATION_DELIMITER = '\n'

type Socket struct {
	conn     net.Conn
	reader   *bufio.Reader
	listener net.Listener
}

func Connect(address string) (*Socket, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	clientSocket := &Socket{
		conn:     conn,
		reader:   bufio.NewReader(conn),
		listener: nil,
	}

	return clientSocket, nil
}

func CreateServerSocket(address string) (*Socket, error) {
	listener, err := net.Listen("tcp", address)
	if err != nil {
		return nil, err
	}

	serverSocket := &Socket{
		conn:     nil,
		reader:   nil,
		listener: listener,
	}

	return serverSocket, nil
}

func (s *Socket) Accept() (*Socket, error) {
	conn, err := s.listener.Accept()
	if err != nil {
		return nil, err
	}

	clientSocket := &Socket{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	return clientSocket, nil
}

func (s *Socket) Read() (*protocol.ServerClientMessage, error) {
	message, err := s.reader.ReadBytes(COMMUNICATION_DELIMITER)
	if err != nil {
		return nil, err
	}

	message = message[:len(message)-1]

	var responseMessage protocol.ServerClientMessage
	err = proto.Unmarshal(message, &responseMessage)
	if err != nil {
		return nil, err
	}

	return &responseMessage, nil
}

func (s *Socket) Write(message *protocol.ClientServerMessage) error {
	message_bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	message_bytes = append(message_bytes, byte(COMMUNICATION_DELIMITER))

	bytes_written := 0

	for bytes_written < len(message_bytes) {
		n, err := s.conn.Write(message_bytes[bytes_written:])
		if err != nil {

			return err
		}
		bytes_written += n
	}

	return nil
}

func (s *Socket) Close() error {
	return s.conn.Close()
}
