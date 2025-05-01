package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

var log = logging.MustGetLogger("log")

// func SaveDataToFile[T proto.Message](dir string, clientId string, stage string, fileType string, data map[string]T) error {

// 	err := os.MkdirAll(dir, os.ModePerm)
// 	if err != nil {
// 		return fmt.Errorf("failed to create directory: %w", err)
// 	}

// 	var fileName string
// 	if fileType == "" {
// 		fileName = fmt.Sprintf("%s_%s.json", stage, clientId)
// 	} else {
// 		fileName = fmt.Sprintf("%s_%s_%s.json", stage, fileType, clientId)
// 	}

// 	filePath := filepath.Join(dir, fileName)

// 	marshaler := protojson.MarshalOptions{
// 		Indent:          "  ",  // Pretty print
// 		EmitUnpopulated: false, // Include unpopulated fields
// 	}

// 	var jsonArray []json.RawMessage
// 	for _, msg := range data {
// 		marshaledData, err := marshaler.Marshal(msg)
// 		if err != nil {
// 			return fmt.Errorf("error marshaling data to JSON: %w", err)
// 		}
// 		jsonArray = append(jsonArray, marshaledData)
// 	}

// 	finalJSON, err := json.MarshalIndent(jsonArray, "", "  ")
// 	if err != nil {
// 		return fmt.Errorf("error marshaling JSON array: %w", err)
// 	}

// 	err = os.WriteFile(filePath, finalJSON, 0644)
// 	if err != nil {
// 		return fmt.Errorf("error writing data to file: %w", err)
// 	}
// 	return nil
// }

func processTypedMap[T proto.Message](typedMap map[string]T, filePath string, marshaler protojson.MarshalOptions) error {
	var jsonArray []json.RawMessage
	for _, msg := range typedMap {
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

func processTypedMap2[T proto.Message](v map[string][]T, filePath string, marshaler protojson.MarshalOptions) error {
	groupedData := make(map[string][]json.RawMessage)
	for key, slice := range v {
		for _, msg := range slice {
			marshaledData, err := marshaler.Marshal(msg)
			if err != nil {
				return fmt.Errorf("error marshaling data to JSON: %w", err)
			}
			//jsonArray = append(jsonArray, marshaledData)
			groupedData[key] = append(groupedData[key], marshaledData)
		}
	}

	// Serializar el mapa agrupado como JSON
	finalJSON, err := json.MarshalIndent(groupedData, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling grouped JSON: %w", err)
	}

	err = os.WriteFile(filePath, finalJSON, 0644)
	if err != nil {
		return fmt.Errorf("error writing grouped data to file: %w", err)
	}
	return nil
}

func SaveDataToFile(dir string, clientId string, stage string, sourceType string, data interface{}) error {
	err := os.MkdirAll(dir, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	var fileName string
	if sourceType == "" {
		fileName = fmt.Sprintf("%s_%s.json", stage, clientId)
	} else {
		fileName = fmt.Sprintf("%s_%s_%s.json", stage, sourceType, clientId)
	}

	filePath := filepath.Join(dir, fileName)

	marshaler := protojson.MarshalOptions{
		Indent:          "  ",  // Pretty print
		EmitUnpopulated: false, // Include unpopulated fields
	}

	switch v := data.(type) {
	case map[string]*protocol.Epsilon_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Lambda_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Theta_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Delta_2_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Delta_3_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Nu_2_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Nu_3_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Kappa_2_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Kappa_3_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Eta_2_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Eta_3_Data:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Zeta_Data_Movie:
		return processTypedMap(v, filePath, marshaler)

	case map[string]*protocol.Iota_Data_Movie:
		return processTypedMap(v, filePath, marshaler)

	case map[string][]*protocol.Zeta_Data_Rating:
		return processTypedMap2(v, filePath, marshaler)

	case map[string][]*protocol.Iota_Data_Actor:
		return processTypedMap2(v, filePath, marshaler)

	default:
		return fmt.Errorf("unsupported data type: %T", data)
	}
}

func DeletePartialResults(dir string, clientId string, stage string, sourceType string) error {

	filePattern := fmt.Sprintf("%s*%s*%s*.json", stage, sourceType, clientId)
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
