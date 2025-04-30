package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

func SaveDataToFile[T proto.Message](dir string, clientId string, stage string, data map[string]T) error {

	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fileName := fmt.Sprintf("%s_%s.json", stage, clientId)
	filePath := filepath.Join(dir, fileName)

	marshaler := protojson.MarshalOptions{
		Indent:          "  ",  // Pretty print
		EmitUnpopulated: false, // Include unpopulated fields
	}

	var jsonArray []json.RawMessage
	for _, msg := range data {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonArray = append(jsonArray, marshaledData)
	}

	finalJSON, err := json.MarshalIndent(jsonArray, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON array: %w", err)
	}

	err = os.WriteFile(filePath, finalJSON, 0644)
	if err != nil {
		return fmt.Errorf("error writing data to file: %w", err)
	}
	return nil
}

func DeletePartialResults(dir string, clientId string) error {

	filePattern := fmt.Sprintf("*%s*.json", clientId)
	files, err := filepath.Glob(filepath.Join(dir, filePattern))
	if err != nil {
		return fmt.Errorf("error finding files with pattern %s: %w", filePattern, err)
	}

	for _, filePath := range files {
		err := os.Remove(filePath)
		if err != nil {
			if os.IsNotExist(err) {
				log.Infof("File %s does not exist, nothing to delete", filePath)
				continue
			}
			return fmt.Errorf("error deleting file %s: %w", filePath, err)
		}
		log.Infof("File %s deleted successfully", filePath)
	}

	return nil
}
