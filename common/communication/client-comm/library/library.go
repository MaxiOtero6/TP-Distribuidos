package library

import (
	"fmt"
	"io"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-comm/protocol"
)

const MOVIES_FILE = "movies_head_10k.csv"
const RATINGS_FILE = "ratings_head_10k.csv"
const CREDITS_FILE = "credits_head_10k.csv"

type Library struct {
	parser    *Parser
	socket    *Socket
	fileNames []string
}

func NewLibrary(maxBatch int, maxSize int, fileNames []string, address string) (*Library, error) {
	if len(fileNames) == 0 {
		return nil, fmt.Errorf("no files provided")
	}

	parser, err := NewParser(maxBatch, maxSize, fileNames[0]) // Inicializar con el primer archivo
	if err != nil {
		return nil, err
	}

	socket, err := Connect(address)
	if err != nil {
		return nil, err
	}

	return &Library{
		parser:    parser,
		socket:    socket,
		fileNames: fileNames,
	}, nil
}

func (l *Library) ProcessData() error {
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
		}

		// Eliminar el archivo procesado de la lista
		l.fileNames = l.fileNames[1:]
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
