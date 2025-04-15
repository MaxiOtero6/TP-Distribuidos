package utils

import (
	"bufio"
	"fmt"
	"io"
	"os"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-comm/protocol"
)

const MAX_SIZE = 8192 // 8KB
const COMMUNICATION_DELIMITER = '\n'

type Parser struct {
	file         *os.File
	maxBatch     int
	bufReader    *bufio.Reader
	leftoverLine string
}

func NewParser(maxBatch int, filename string) (*Parser, error) {
	if filename == "" {
		return nil, fmt.Errorf("no file provided")
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &Parser{
		file:         file,
		maxBatch:     maxBatch,
		bufReader:    bufio.NewReader(file),
		leftoverLine: "",
	}, nil
}

func (p *Parser) ReadBatch(fileType protocol.FileType) (*protocol.SendMessage, error) {
	sendMessage := &protocol.SendMessage{
		Message: &protocol.SendMessage_Batch{
			Batch: &protocol.Batch{
				Type: fileType,
				Data: make([]*protocol.Batch_Row, 0, p.maxBatch),
			},
		},
	}

	batch := sendMessage.GetBatch()
	totalSize := 0

	if p.leftoverLine != "" {
		if totalSize+len(p.leftoverLine) > MAX_SIZE {
			return sendMessage, nil
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
					return sendMessage, nil
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

	return sendMessage, nil
}

func (p *Parser) Close() {
	if p.file != nil {
		p.file.Close()
		p.file = nil
	}
}
