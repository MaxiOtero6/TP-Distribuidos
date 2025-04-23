package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
)

const MAX_SIZE = 1024 * 80 // 80 KB
const COMMUNICATION_DELIMITER = '\n'

var log = logging.MustGetLogger("log")

type Parser struct {
	file         *os.File
	fileType     protocol.FileType
	maxBatch     int
	bufReader    *bufio.Reader
	leftoverLine string
}

func NewParser(maxBatch int, filename string, fileType protocol.FileType) (*Parser, error) {
	if filename == "" {
		return nil, fmt.Errorf("no file provided")
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	bufReader := bufio.NewReader(file)

	_, err = bufReader.ReadString(COMMUNICATION_DELIMITER)
	if err != nil && err != io.EOF {
		file.Close()
		return nil, fmt.Errorf("error reading first line: %v", err)
	}

	return &Parser{
		file:         file,
		fileType:     fileType,
		maxBatch:     maxBatch,
		bufReader:    bufReader,
		leftoverLine: "",
	}, nil
}

func (p *Parser) ReadBatch(clientId string) (*protocol.Message, error) {

	batchMessage := &protocol.Message{
		Message: &protocol.Message_ClientServerMessage{
			ClientServerMessage: &protocol.ClientServerMessage{
				Message: &protocol.ClientServerMessage_Batch{
					Batch: &protocol.Batch{
						Type:     p.fileType,
						Data:     make([]*protocol.Batch_Row, 0, p.maxBatch),
						ClientId: clientId,
					},
				},
			},
		},
	}

	batch := batchMessage.GetMessage().(*protocol.Message_ClientServerMessage).ClientServerMessage.GetMessage().(*protocol.ClientServerMessage_Batch).Batch
	totalSize := 0

	if p.leftoverLine != "" {
		if totalSize+len(p.leftoverLine) > MAX_SIZE {
			return batchMessage, nil
		}

		batch.Data = append(batch.Data, &protocol.Batch_Row{Data: p.leftoverLine})
		totalSize += len(p.leftoverLine)
		p.leftoverLine = ""
	}

	for range p.maxBatch {
		line, err := p.bufReader.ReadString(COMMUNICATION_DELIMITER)
		if err != nil {
			if err == io.EOF {
				if len(batch.Data) > 0 {
					return batchMessage, nil
				}

			}
			return nil, err
		}

		if totalSize+len(line) > MAX_SIZE {
			p.leftoverLine = line
			break
		}

		batch.Data = append(batch.Data, &protocol.Batch_Row{Data: line})
		totalSize += len(line)
	}

	return batchMessage, nil
}

func (p *Parser) Close() {
	if p.file != nil {
		p.file.Close()
		p.file = nil
		log.Info("Parser file closed successfully.")
	}
}
