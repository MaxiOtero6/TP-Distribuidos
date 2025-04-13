package library

import (
	"bufio"
	"net"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-comm/protocol"
	"google.golang.org/protobuf/proto"
)

const COMMUNICATION_DELIMITER = '\n'

type Socket struct {
	conn   net.Conn
	reader *bufio.Reader
}

func Connect(address string) (*Socket, error) {
	conn, err := net.Dial("tcp", address)
	if err != nil {
		return nil, err
	}

	clientSocket := &Socket{
		conn:   conn,
		reader: bufio.NewReader(conn),
	}

	return clientSocket, nil
}

func (s *Socket) Read() (string, error) {
	message, err := s.reader.ReadString(COMMUNICATION_DELIMITER)
	if err != nil {
		return "", err
	}

	return message[:len(message)-1], nil
}

func (s *Socket) Write(message *protocol.Batch) error {
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
