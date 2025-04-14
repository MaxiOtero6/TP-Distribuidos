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
	files        []string
	currentFile  *os.File
	currentIndex int
	maxBatch     int
	bufReader    *bufio.Reader
	leftoverLine string
	maxSize      int
}

func NewParser(maxBatch int, maxSize int, filename string) (*Parser, error) {
	if filename == "" {
		return nil, fmt.Errorf("no file provided")
	}

	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}

	return &Parser{
		files:        []string{filename},
		currentFile:  file,
		currentIndex: 0,
		maxBatch:     maxBatch,
		bufReader:    bufio.NewReader(file),
		leftoverLine: "",
		maxSize:      maxSize,
	}, nil
}

func (p *Parser) LoadNewFile(filename string) error {
	if p.currentFile != nil {
		p.currentFile.Close()
	}

	file, err := os.Open(filename)
	if err != nil {
		return err
	}

	p.files = []string{filename}
	p.currentFile = file
	p.bufReader = bufio.NewReader(file)
	p.leftoverLine = ""
	p.currentIndex = 0
	return nil
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
		if totalSize+len(p.leftoverLine) > p.maxSize {
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

		if totalSize+len(line) > p.maxSize {
			p.leftoverLine = line
			break
		}

		batch.Data = append(batch.Data, &protocol.Batch_Row{Data: line})
		totalSize += len(line)
	}

	return sendMessage, nil
}
