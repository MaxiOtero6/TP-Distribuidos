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

// saveDataToFile guarda los datos en un archivo JSON.
func saveDataToFile[T proto.Message](dir string, clientId string, stage string, data map[string]T) error {
	// Asegurarse de que el directorio exista
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	fileName := fmt.Sprintf("%s_%s.json", clientId, stage)
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

	log.Infof("Data for client %s at stage %s successfully saved to %s", clientId, stage, filePath)
	return nil
}

// loadComponentFromFileToJson carga los datos de un archivo JSON y los deserializa en la estructura proporcionada.
// func loadComponentFromFileToJson(dir string, clientId string, componentName string) ([]json.RawMessage, error) {

// 	filePath := filepath.Join(dir, fmt.Sprintf("%s_%s.json", clientId, componentName))

// 	// Read the file content
// 	fileContent, err := os.ReadFile(filePath)
// 	if err != nil {
// 		if os.IsNotExist(err) {
// 			return nil, nil
// 		}
// 		return nil, fmt.Errorf("failed to read file: %w", err)
// 	}

// 	var rawMessages []json.RawMessage
// 	if err := json.Unmarshal(fileContent, &rawMessages); err != nil {
// 		return nil, fmt.Errorf("error unmarshaling JSON array: %w", err)
// 	}

// 	log.Infof("Loaded complete data %s", rawMessages)

// 	return rawMessages, nil
// }

// func loadDelta1FromFile(dir string, clientId string, componentName string, data *[]*protocol.Delta_1_Data) error {

// 	jsonMessages, err := loadComponentFromFileToJson(dir, clientId, componentName)
// 	if err != nil {
// 		return err
// 	}

// 	decoder := protojson.UnmarshalOptions{
// 		DiscardUnknown: true, //Ignore unknown fields
// 	}

// 	for _, rawMessage := range jsonMessages {
// 		msg := &protocol.Delta_1_Data{}

// 		if err := decoder.Unmarshal(rawMessage, msg); err != nil {
// 			return fmt.Errorf("error unmarshaling JSON object to protobuf: %w", err)
// 		}
// 		*data = append(*data, msg)
// 	}

// 	log.Infof("Successfully loaded data for client %s at component %s", clientId, componentName)

// 	return nil
// }

// func loadDelta2FromFile(dir string, clientId string, componentName string, data map[string]*protocol.Delta_2_Data) error {
// 	jsonMessages, err := loadComponentFromFileToJson(dir, clientId, componentName)
// 	if err != nil {
// 		return err
// 	}

// 	decoder := protojson.UnmarshalOptions{
// 		DiscardUnknown: true, //Ignore unknown fields
// 	}

// 	for _, rawMessage := range jsonMessages {
// 		msg := &protocol.Delta_2_Data{}

// 		if err := decoder.Unmarshal(rawMessage, msg); err != nil {
// 			return fmt.Errorf("error unmarshaling JSON object to protobuf: %w", err)
// 		}
// 		data[msg.Country] = msg
// 	}

// 	log.Infof("Successfully loaded data for client %s at component %s", clientId, componentName)

// 	return nil
// }
