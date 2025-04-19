package client_server_communication

import (
	"bufio"
	"net"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server-comm/protocol"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

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

func (s *Socket) Read() (*protocol.Message, error) {

	lengthBytes := make([]byte, 2)
	_, err := s.reader.Read(lengthBytes)
	if err != nil {
		log.Errorf("Error reading message length: %v", err)
		return nil, err
	}

	length := int(lengthBytes[0])<<8 | int(lengthBytes[1])
	log.Debugf("Message length: %d", length)

	message := make([]byte, length)

	messageReceivedLength := 0

	for messageReceivedLength < length {
		n, err := s.reader.Read(message[messageReceivedLength:])
		if err != nil {
			log.Errorf("Error reading message: %v", err)
			return nil, err
		}
		messageReceivedLength += n
	}

	// Deserializa el mensaje usando proto.Unmarshal
	var responseMessage protocol.Message
	err = proto.Unmarshal(message, &responseMessage)
	if err != nil {
		log.Errorf("Error deserializing message: %v", err)
		return nil, err
	}

	log.Debugf("Successfully deserialized message: %v", responseMessage)
	return &responseMessage, nil
}

func (s *Socket) Write(message *protocol.Message) error {

	log.Info("Writing message to socket")
	log.Infof("Message: %v", message)
	message_bytes, err := proto.Marshal(message)
	if err != nil {
		return err
	}

	log.Debugf("Sending message: %v", message_bytes)

	length := len(message_bytes)

	sendMessage := make([]byte, 2+length)

	// Write the length of the message in the first 2 bytes
	sendMessage[0] = byte(length >> 8)
	sendMessage[1] = byte(length & 0xFF)

	copy(sendMessage[2:], message_bytes)

	bytes_written := 0
	for bytes_written < len(sendMessage) {
		n, err := s.conn.Write(sendMessage[bytes_written:])
		if err != nil {
			log.Errorf("Error writing to socket: %v", err)
			return err
		}
		bytes_written += n
	}
	return nil
}

func (s *Socket) Close() error {
	return s.conn.Close()
}
