package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const CLEANUP_INTERVAL = 10 * time.Minute

const VALID_STATUS = "valid"
const INVALID_STATUS = "invalid"

const FINAL_FILE_NAME = "data.json"
const TEMPORARY_FILE_NAME = "data_temp.json"

var log = logging.MustGetLogger("log")

// var processTypedMapFirstTime = true

type OutputFile struct {
	Data       interface{} `json:"data"`
	Timestamps string      `json:"timestamps"`
	Status     string      `json:"status"`
}

func isTempFileValid(finalFile, tempFile string) (bool, bool, error) {
	var isValid bool
	var err error
	var isTempFileExists bool

	isTempFileExists, err = fileExists(tempFile)

	if err != nil {
		return false, false, fmt.Errorf("failed to check for temporary file: %w", err)
	}

	isFinalFileExist, err := fileExists(finalFile)
	if err != nil {
		return false, false, fmt.Errorf("failed to check for final file: %w", err)
	}

	if !isTempFileExists {
		isValid = false
		err = nil
	} else if isTempFileExists {
		if !isFinalFileExist {
			isValid, err = isFileStatusValid(tempFile)
		} else {
			isValid, err = canReplaceFile(finalFile, tempFile)
		}
	}

	return isValid, isTempFileExists, err
}

func fileExists(path string) (bool, error) {
	_, err := os.Stat(path)
	if err == nil {
		log.Infof("File exists: %s", path)
		return true, nil
	}
	if os.IsNotExist(err) {
		return false, nil
	}
	return false, err // otro error (permisos, etc.)
}

func isFileStatusValid(filePath string) (bool, error) {
	state, timestamp, err := getStatusAndTimestampStream(filePath)
	if err != nil {
		return false, err
	}
	return state == VALID_STATUS && timestamp != "", nil
}

func canReplaceFile(finalFile, tempFile string) (bool, error) {
	statusFinal, timestampFinal, err := getStatusAndTimestampStream(finalFile)
	if err != nil {
		return false, err
	}
	statusTemp, timestampTemp, err := getStatusAndTimestampStream(tempFile)
	if err != nil {
		return false, err
	}

	if statusFinal == VALID_STATUS && statusTemp == VALID_STATUS {
		return timestampFinal < timestampTemp, nil
	}
	return false, nil
}

func getStatusAndTimestampStream(filePath string) (string, string, error) {
	f, err := os.Open(filePath)
	if err != nil {
		return "", "", err
	}
	defer f.Close()

	dec := json.NewDecoder(f)
	var status, timestamps string

	// Esperamos el inicio del objeto
	t, err := dec.Token()
	if err != nil || t != json.Delim('{') {
		return "", "", err
	}

	// Iteramos por los campos
	for dec.More() {
		t, err := dec.Token()
		if err != nil {
			return "", "", err
		}
		key := t.(string)
		switch key {
		case "status":
			if err := dec.Decode(&status); err != nil {
				return "", "", err
			}
		case "timestamps":
			if err := dec.Decode(&timestamps); err != nil {
				return "", "", err
			}
		default:
			// Saltar el valor (puede ser grande)
			var discard interface{}
			if err := dec.Decode(&discard); err != nil {
				return "", "", err
			}
		}
	}
	return status, timestamps, nil
}

func handleLeftOverFiles(finalFile, tempFile string) error {
	isTempValid, isTempExist, err := isTempFileValid(finalFile, tempFile)
	if err != nil {
		return fmt.Errorf("failed to check temporary file validity: %w", err)
	}

	if isTempValid {
		//overwrite the final file with the temporary file
		err = os.Rename(tempFile, finalFile) // Atomic rename operation
		if err != nil {
			return fmt.Errorf("failed to rename temporary file to final file: %w", err)
		}
	} else {
		if isTempExist {
			// Delete the temporary file
			err = os.Remove(tempFile)
			if err != nil {
				return err
			}
		}
	}

	return nil
}

func SaveDataToFile(dir string, clientId string, stage string, sourceType string, data interface{}) error {

	dirPath := filepath.Join(dir, stage, sourceType, clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempFilePath := filepath.Join(dirPath, TEMPORARY_FILE_NAME)

	err = saveDataToFile(tempFilePath, data)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	return nil
}

func saveDataToFile(tempFilePath string, data interface{}) error {

	marshaler := protojson.MarshalOptions{
		Indent:          "  ",  // Pretty print
		EmitUnpopulated: false, // Include unpopulated fields
	}

	switch v := data.(type) {
	case map[string]*protocol.Epsilon_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Lambda_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Theta_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Delta_2_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Delta_3_Data:
		return processTypedMap(v, tempFilePath, marshaler)
		//return processTypedStruct(common.PartialData[v], tempFilePath, marshaler)

	case map[string]*protocol.Nu_2_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Nu_3_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Kappa_2_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Kappa_3_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Eta_2_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Eta_3_Data:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Zeta_Data_Movie:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string]*protocol.Iota_Data_Movie:
		return processTypedMap(v, tempFilePath, marshaler)

	case map[string][]*protocol.Zeta_Data_Rating:
		return processTypedMap2(v, tempFilePath, marshaler)

	case map[string][]*protocol.Iota_Data_Actor:
		return processTypedMap2(v, tempFilePath, marshaler)

	// Add explicit instantiations for the expected PartialData types
	case common.PartialData[*protocol.Delta_3_Data]:
		return processTypedStruct(v, tempFilePath, marshaler)
	case common.PartialData[*protocol.Eta_3_Data]:
		return processTypedStruct(v, tempFilePath, marshaler)
	case common.PartialData[*protocol.Kappa_3_Data]:
		return processTypedStruct(v, tempFilePath, marshaler)
	case common.PartialData[*protocol.Nu_3_Data]:
		return processTypedStruct(v, tempFilePath, marshaler)
	default:
		return fmt.Errorf("unsupported data type: %T", data)
	}
}

func LoadMergerPartialResultsFromDisk(dir, sourceType string) (map[string]*common.MergerPartialResults, error) {
	partialResults := make(map[string]*common.MergerPartialResults)

	// Delta3
	stageDir := filepath.Join(dir, common.DELTA_STAGE_3, sourceType)
	clientDirs, err := os.ReadDir(stageDir)
	if err == nil {
		for _, clientEntry := range clientDirs {
			if !clientEntry.IsDir() {
				continue
			}
			clientId := clientEntry.Name()
			data, err := LoadStageClientInfoFromDisk[map[string]*protocol.Delta_3_Data](dir, common.DELTA_STAGE_3, sourceType, clientId)
			if err == nil {
				partialResults[clientId] = &common.MergerPartialResults{
					Delta3: common.PartialData[*protocol.Delta_3_Data]{
						Data:  data,
						Ready: false,
					},
				}
			} else {
				log.Errorf("Error loading Delta3 data for client %s: %v", clientId, err)
			}
		}
	}

	// Eta3
	stageDir = filepath.Join(dir, common.ETA_STAGE_3, sourceType)
	clientDirs, err = os.ReadDir(stageDir)
	if err == nil {
		for _, clientEntry := range clientDirs {
			if !clientEntry.IsDir() {
				continue
			}
			clientId := clientEntry.Name()
			data, err := LoadStageClientInfoFromDisk[map[string]*protocol.Eta_3_Data](dir, common.ETA_STAGE_3, sourceType, clientId)
			if err == nil {
				partialResults[clientId] = &common.MergerPartialResults{
					Eta3: common.PartialData[*protocol.Eta_3_Data]{
						Data:  data,
						Ready: false,
					},
				}
			} else {
				log.Errorf("Error loading Eta3 data for client %s: %v", clientId, err)
			}
		}
	}

	// Kappa3
	stageDir = filepath.Join(dir, common.KAPPA_STAGE_3, sourceType)
	clientDirs, err = os.ReadDir(stageDir)
	if err == nil {
		for _, clientEntry := range clientDirs {
			if !clientEntry.IsDir() {
				continue
			}
			clientId := clientEntry.Name()
			data, err := LoadStageClientInfoFromDisk[map[string]*protocol.Kappa_3_Data](dir, common.KAPPA_STAGE_3, sourceType, clientId)
			if err == nil {
				partialResults[clientId] = &common.MergerPartialResults{
					Kappa3: common.PartialData[*protocol.Kappa_3_Data]{
						Data:  data,
						Ready: false,
					},
				}
			} else {
				log.Errorf("Error loading Kappa3 data for client %s: %v", clientId, err)
			}
		}
	}

	// Nu3Data
	stageDir = filepath.Join(dir, common.NU_STAGE_3, sourceType)
	clientDirs, err = os.ReadDir(stageDir)
	if err == nil {
		for _, clientEntry := range clientDirs {
			if !clientEntry.IsDir() {
				continue
			}
			clientId := clientEntry.Name()
			log.Infof("Loading Nu3Data for client %s", clientId)
			data, err := LoadStageClientInfoFromDisk[common.PartialData[*protocol.Nu_3_Data]](dir, common.NU_STAGE_3, sourceType, clientId)
			if err == nil {
				partialResults[clientId] = &common.MergerPartialResults{
					Nu3Data: common.PartialData[*protocol.Nu_3_Data]{
						Data:  data.Data,
						Ready: data.Ready,
					},
				}
			} else {
				log.Errorf("Error loading Nu3Data for client %s: %v", clientId, err)
			}
		}
	}

	return partialResults, nil
}

func LoadStageClientInfoFromDisk[T any](dir string, stage string, sourceType string, clientId string) (T, error) {
	var zero T

	dirPath := filepath.Join(dir, stage, sourceType, clientId)

	tempFilePath := filepath.Join(dirPath, TEMPORARY_FILE_NAME)
	finalFilePath := filepath.Join(dirPath, FINAL_FILE_NAME)

	err := handleLeftOverFiles(finalFilePath, tempFilePath)
	if err != nil {

		return zero, fmt.Errorf("failed to handle leftover files: %w", err)
	}

	file, err := os.Open(finalFilePath)
	if err != nil {
		return zero, fmt.Errorf("error opening file %s: %w", finalFilePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	switch stage {
	case common.EPSILON_STAGE:
		loadedData := make(map[string]*protocol.Epsilon_Data)
		result, err := decodeEpsilonData(decoder)

		if err != nil {
			return zero, err
		}

		loadedData = result
		return any(loadedData).(T), nil

	case common.LAMBDA_STAGE:
		loadedData := make(map[string]*protocol.Lambda_Data)
		decodedData, err := decodeLambdaData(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil
	case common.THETA_STAGE:
		loadedData := make(map[string]*protocol.Theta_Data)
		decodedData, err := decodeThetaData(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.DELTA_STAGE_2:
		loadedData := make(map[string]*protocol.Delta_2_Data)
		decodedData, err := decodeDelta2Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.DELTA_STAGE_3:
		loadedData := make(map[string]*protocol.Delta_3_Data)
		decodedData, err := decodeDelta3Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.NU_STAGE_2:
		loadedData := make(map[string]*protocol.Nu_2_Data)
		decodedData, err := decodeNu2Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.NU_STAGE_3:
		loadedData := common.PartialData[*protocol.Nu_3_Data]{}
		decodedData, err := decodeNu3Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.KAPPA_STAGE_2:
		loadedData := make(map[string]*protocol.Kappa_2_Data)
		decodedData, err := decodeKappa2Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.KAPPA_STAGE_3:
		loadedData := make(map[string]*protocol.Kappa_3_Data)
		decodedData, err := decodeKappa3Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.ETA_STAGE_2:
		loadedData := make(map[string]*protocol.Eta_2_Data)
		decodedData, err := decodeEta2Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.ETA_STAGE_3:
		loadedData := make(map[string]*protocol.Eta_3_Data)
		decodedData, err := decodeEta3Data(decoder)

		if err != nil {
			return zero, err
		}
		loadedData = decodedData
		return any(loadedData).(T), nil

	case common.ZETA_STAGE:
		var loadedData interface{}

		if sourceType == common.SMALL_TABLE_SOURCE {

			loadedData = make(map[string]*protocol.Zeta_Data_Movie)
			decodedData, err := decodeZetaDataMovie(decoder)

			if err != nil {
				return zero, err
			}
			loadedData = decodedData

		} else if sourceType == common.BIG_TABLE_SOURCE {
			loadedData = make(map[string][]*protocol.Zeta_Data_Rating)
			decodedData, err := decodeZetaDataRating(decoder)

			if err != nil {
				return zero, err
			}
			loadedData = decodedData
		} else {
			return zero, fmt.Errorf("unsupported source type: %s", sourceType)
		}

		return any(loadedData).(T), nil

	case common.IOTA_STAGE:
		if sourceType == common.SMALL_TABLE_SOURCE {
			loadedData := make(map[string]*protocol.Iota_Data_Movie)
			decodedData, err := decodeIotaDataMovie(decoder)

			if err != nil {
				return zero, err
			}
			loadedData = decodedData
			return any(loadedData).(T), nil
		} else if sourceType == common.BIG_TABLE_SOURCE {
			loadedData := make(map[string][]*protocol.Iota_Data_Actor)
			decodedData, err := decodeIotaDataActor(decoder)

			if err != nil {
				return zero, err
			}
			loadedData = decodedData
			return any(loadedData).(T), nil
		} else {
			return zero, fmt.Errorf("unsupported source type: %s", sourceType)
		}

	default:
		return zero, fmt.Errorf("unsupported data stage: %s", stage)
	}
}

func CommitPartialDataToFinal(dir string, stage interface{}, sourceType string, clientId string) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}

	dirPath := filepath.Join(dir, stringStage, sourceType, clientId)
	tempFilePath := filepath.Join(dirPath, TEMPORARY_FILE_NAME)
	finalFilePath := filepath.Join(dirPath, FINAL_FILE_NAME)

	log.Infof("Committing data from %s to %s", tempFilePath, finalFilePath)

	if err := os.Rename(tempFilePath, finalFilePath); err != nil {
		return fmt.Errorf("error renaming temp file: %w", err)
	}
	return nil
}

func DeletePartialResults(dir string, clientId string, stage string, sourceType string) error {
	dirPath := filepath.Join(dir, stage, sourceType, clientId)
	return os.RemoveAll(dirPath)
}

func StartCleanupRoutine(dir string) {
	ticker := time.NewTicker(CLEANUP_INTERVAL)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
			err := cleanUpOldFiles(dir)
			if err != nil {
				log.Errorf("Error during cleanup: %s", err)
			}

		}
	}
}

// func cleanUpOldFiles(dir string) error {
// 	files, err := os.ReadDir(dir)
// 	if err != nil {
// 		return fmt.Errorf("error reading directory %s: %w", dir, err)
// 	}
// 	for _, file := range files {
// 		if !file.IsDir() {
// 			continue
// 		}
// 		filePath := filepath.Join(dir, file.Name())
// 		info, err := os.Stat(filePath)
// 		if err != nil {
// 			log.Errorf("Failed to stat file %s: %s", file.Name(), err)
// 			continue
// 		}
// 		// Delete files older than CLEANUP_INTERVAL
// 		if time.Since(info.ModTime()) >= CLEANUP_INTERVAL {
// 			log.Debugf("Deleting old file: %s", filePath)
// 			if err := os.Remove(filePath); err != nil {
// 				log.Errorf("Failed to delete file %s: %s", filePath, err)
// 			}
// 		}
// 	}
// 	return nil
// }

func cleanUpOldFiles(dir string) error {
	return filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if d.IsDir() {
			return nil
		}
		info, err := d.Info()
		if err != nil {
			return nil
		}
		if time.Since(info.ModTime()) >= CLEANUP_INTERVAL {
			if err := os.Remove(path); err != nil {
				// log error si querés
			}
		}
		return nil
	})
}

func decodeDelta2Data(decoder *json.Decoder) (map[string]*protocol.Delta_2_Data, error) {
	var tempArray struct {
		Data []struct {
			Country       string `json:"country"`
			PartialBudget string `json:"partialBudget"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Delta_2_Data)
	for _, item := range tempArray.Data {
		partialBudget, err := strconv.ParseUint(item.PartialBudget, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing partialBudget: %w", err)
		}
		result[item.Country] = &protocol.Delta_2_Data{
			Country:       item.Country,
			PartialBudget: partialBudget,
		}
	}
	return result, nil
}

func decodeDelta3Data(decoder *json.Decoder) (map[string]*protocol.Delta_3_Data, error) {
	var tempArray struct {
		Data []struct {
			Country       string `json:"country"`
			PartialBudget string `json:"partialBudget"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Delta_3_Data)
	for _, item := range tempArray.Data {
		partialBudget, err := strconv.ParseUint(item.PartialBudget, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing partialBudget: %w", err)
		}
		result[item.Country] = &protocol.Delta_3_Data{
			Country:       item.Country,
			PartialBudget: partialBudget,
		}
	}
	return result, nil
}

func decodeZetaDataMovie(decoder *json.Decoder) (map[string]*protocol.Zeta_Data_Movie, error) {
	var tempArray struct {
		Data []struct {
			MovieId string `json:"movieId"`
			Title   string `json:"title"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Zeta_Data_Movie)
	for _, item := range tempArray.Data {
		result[item.MovieId] = &protocol.Zeta_Data_Movie{
			MovieId: item.MovieId,
			Title:   item.Title,
		}
	}
	return result, nil
}

func decodeZetaDataRating(decoder *json.Decoder) (map[string][]*protocol.Zeta_Data_Rating, error) {
	var tempMap struct {
		Data map[string][]struct {
			MovieId string  `json:"movieId"`
			Rating  float32 `json:"rating"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempMap); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string][]*protocol.Zeta_Data_Rating)
	for _, item := range tempMap.Data {
		for _, item := range item {
			result[item.MovieId] = append(result[item.MovieId], &protocol.Zeta_Data_Rating{
				MovieId: item.MovieId,
				Rating:  item.Rating,
			})

		}
	}
	return result, nil
}

func decodeEpsilonData(decoder *json.Decoder) (map[string]*protocol.Epsilon_Data, error) {
	var tempArray struct {
		Data []struct {
			ProdCountry     string `json:"prodCountry"`
			TotalInvestment string `json:"totalInvestment"`
		} `json:"data"`
	}

	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Epsilon_Data)
	for _, item := range tempArray.Data {
		totalInvestment, err := strconv.ParseUint(item.TotalInvestment, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing totalInvestment: %w", err)
		}
		result[item.ProdCountry] = &protocol.Epsilon_Data{
			ProdCountry:     item.ProdCountry,
			TotalInvestment: totalInvestment,
		}
	}
	return result, nil
}

func decodeLambdaData(decoder *json.Decoder) (map[string]*protocol.Lambda_Data, error) {
	var tempArray struct {
		Data []struct {
			ActorId        string `json:"actorId"`
			ActorName      string `json:"actorName"`
			Participations string `json:"participations"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Lambda_Data)
	for _, item := range tempArray.Data {
		participations, err := strconv.ParseUint(item.Participations, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing participations: %w", err)
		}
		result[item.ActorId] = &protocol.Lambda_Data{
			ActorId:        item.ActorId,
			ActorName:      item.ActorName,
			Participations: participations,
		}
	}
	return result, nil

}

func decodeThetaData(decoder *json.Decoder) (map[string]*protocol.Theta_Data, error) {
	var tempArray struct {
		Data []struct {
			Id        string  `json:"id"`
			Title     string  `json:"title"`
			AvgRating float32 `json:"avgRating"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Theta_Data)
	for _, item := range tempArray.Data {
		result[item.Id] = &protocol.Theta_Data{
			Id:        item.Id,
			Title:     item.Title,
			AvgRating: item.AvgRating,
		}
	}
	return result, nil
}

func decodeNu2Data(decoder *json.Decoder) (map[string]*protocol.Nu_2_Data, error) {
	var tempArray struct {
		Data []struct {
			Sentiment bool    `json:"sentiment"`
			Ratio     float32 `json:"ratio"`
			Count     uint32  `json:"count"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Nu_2_Data)
	for _, item := range tempArray.Data {
		result[strconv.FormatBool(item.Sentiment)] = &protocol.Nu_2_Data{
			Sentiment: item.Sentiment,
			Ratio:     item.Ratio,
			Count:     item.Count,
		}
	}
	return result, nil
}

func decodeNu3Data(decoder *json.Decoder) (common.PartialData[*protocol.Nu_3_Data], error) {
	var tempArray struct {
		Data []struct {
			Sentiment bool    `json:"sentiment"`
			Ratio     float32 `json:"ratio"`
			Count     uint32  `json:"count"`
		} `json:"data"`
		Ready bool `json:"ready"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return common.PartialData[*protocol.Nu_3_Data]{}, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := common.PartialData[*protocol.Nu_3_Data]{
		Data: make(map[string]*protocol.Nu_3_Data),
	}

	for _, item := range tempArray.Data {
		result.Data[strconv.FormatBool(item.Sentiment)] = &protocol.Nu_3_Data{
			Sentiment: item.Sentiment,
			Ratio:     item.Ratio,
			Count:     item.Count,
		}
	}
	result.Ready = tempArray.Ready
	return result, nil
}

func decodeKappa2Data(decoder *json.Decoder) (map[string]*protocol.Kappa_2_Data, error) {

	var tempArray struct {
		Data []struct {
			ActorId               string `json:"actorId"`
			ActorName             string `json:"actorName"`
			PartialParticipations string `json:"partialParticipations"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Kappa_2_Data)
	for _, item := range tempArray.Data {
		partialParticipations, err := strconv.ParseUint(item.PartialParticipations, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing partialParticipations: %w", err)
		}
		result[item.ActorId] = &protocol.Kappa_2_Data{
			ActorId:               item.ActorId,
			ActorName:             item.ActorName,
			PartialParticipations: partialParticipations,
		}
	}
	return result, nil
}

func decodeKappa3Data(decoder *json.Decoder) (map[string]*protocol.Kappa_3_Data, error) {
	var tempArray struct {
		Data []struct {
			ActorId               string `json:"actorId"`
			ActorName             string `json:"actorName"`
			PartialParticipations string `json:"partialParticipations"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Kappa_3_Data)
	for _, item := range tempArray.Data {
		partialParticipations, err := strconv.ParseUint(item.PartialParticipations, 10, 64)
		if err != nil {
			return nil, fmt.Errorf("error parsing partialParticipations: %w", err)
		}
		result[item.ActorId] = &protocol.Kappa_3_Data{
			ActorId:               item.ActorId,
			ActorName:             item.ActorName,
			PartialParticipations: partialParticipations,
		}
	}
	return result, nil
}

func decodeEta2Data(decoder *json.Decoder) (map[string]*protocol.Eta_2_Data, error) {
	var tempArray struct {
		Data []struct {
			MovieId string  `json:"movieId"`
			Title   string  `json:"title"`
			Rating  float64 `json:"rating"`
			Count   uint32  `json:"count"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Eta_2_Data)
	for _, item := range tempArray.Data {
		result[item.MovieId] = &protocol.Eta_2_Data{
			MovieId: item.MovieId,
			Title:   item.Title,
			Rating:  item.Rating,
			Count:   item.Count,
		}
	}
	return result, nil
}

func decodeEta3Data(decoder *json.Decoder) (map[string]*protocol.Eta_3_Data, error) {
	var tempArray struct {
		Data []struct {
			MovieId string  `json:"movieId"`
			Title   string  `json:"title"`
			Rating  float64 `json:"rating"`
			Count   uint32  `json:"count"`
		} `json:"data"`
	}

	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Eta_3_Data)
	for _, item := range tempArray.Data {

		result[item.MovieId] = &protocol.Eta_3_Data{
			MovieId: item.MovieId,
			Title:   item.Title,
			Rating:  item.Rating,
			Count:   item.Count,
		}
	}
	return result, nil
}

func decodeIotaDataMovie(decoder *json.Decoder) (map[string]*protocol.Iota_Data_Movie, error) {
	var tempMap struct {
		Data []struct {
			MovieId string `json:"movieId"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempMap); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Iota_Data_Movie)
	for _, item := range tempMap.Data {
		result[item.MovieId] = &protocol.Iota_Data_Movie{
			MovieId: item.MovieId,
		}
	}
	return result, nil
}

func decodeIotaDataActor(decoder *json.Decoder) (map[string][]*protocol.Iota_Data_Actor, error) {
	var tempMap struct {
		Data map[string][]struct {
			MovieId   string `json:"movieId"`
			ActorId   string `json:"actorId"`
			ActorName string `json:"actorName"`
		} `json:"data"`
	}
	if err := decoder.Decode(&tempMap); err != nil {
		return nil, fmt.Errorf("error decoding JSON map: %w", err)
	}
	result := make(map[string][]*protocol.Iota_Data_Actor)
	for movieId, items := range tempMap.Data {
		for _, item := range items {
			result[movieId] = append(result[movieId], &protocol.Iota_Data_Actor{
				MovieId:   item.MovieId,
				ActorId:   item.ActorId,
				ActorName: item.ActorName,
			})
		}
	}
	return result, nil
}

func writeToTempFile(tempFilePath string, output OutputFile) error {

	jsonBytes, err := json.MarshalIndent(output, "", "  ")
	if err != nil {
		return fmt.Errorf("error marshaling JSON array: %w", err)
	}

	tmpFile, err := os.OpenFile(tempFilePath, os.O_CREATE|os.O_WRONLY|os.O_TRUNC, 0644)
	if err != nil {
		return fmt.Errorf("error creating temp file: %w", err)
	}

	if _, err := tmpFile.Write(jsonBytes); err != nil {
		tmpFile.Close()
		return fmt.Errorf("error writing to temp file: %w", err)
	}
	if err := tmpFile.Sync(); err != nil {
		tmpFile.Close()
		return fmt.Errorf("error syncing temp file: %w", err)
	}
	if err := tmpFile.Close(); err != nil {
		return fmt.Errorf("error closing temp file: %w", err)
	}

	return nil
}

func processTypedMap[T proto.Message](typedMap map[string]T, tempFilePath string, marshaler protojson.MarshalOptions) error {

	log.Infof("Processing data to download to file: %s", tempFilePath)

	var jsonArray []json.RawMessage
	for _, msg := range typedMap {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonArray = append(jsonArray, marshaledData)
	}

	output := OutputFile{
		Data:       jsonArray,
		Timestamps: time.Now().UTC().Format(time.RFC3339),
		Status:     VALID_STATUS,
	}
	if err := writeToTempFile(tempFilePath, output); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func processTypedMap2[T proto.Message](v map[string][]T, tempFilePath string, marshaler protojson.MarshalOptions) error {
	groupedData := make(map[string][]json.RawMessage)
	for key, slice := range v {
		for _, msg := range slice {
			marshaledData, err := marshaler.Marshal(msg)
			if err != nil {
				return fmt.Errorf("error marshaling data to JSON: %w", err)
			}
			groupedData[key] = append(groupedData[key], marshaledData)
		}

	}
	output := OutputFile{
		Data:       groupedData,
		Timestamps: time.Now().UTC().Format(time.RFC3339),
		Status:     VALID_STATUS,
	}

	if err := writeToTempFile(tempFilePath, output); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func processTypedStruct[T proto.Message](typedStruct common.PartialData[T], tempFilePath string, marshaler protojson.MarshalOptions) error {

	log.Infof("Processing data to download to file: %s", tempFilePath)

	var jsonArray []json.RawMessage
	for _, msg := range typedStruct.Data {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonArray = append(jsonArray, marshaledData)
	}
	output := OutputFile{
		Data:       jsonArray,
		Timestamps: time.Now().UTC().Format(time.RFC3339),
		Status:     VALID_STATUS,
	}
	if err := writeToTempFile(tempFilePath, output); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func getStageNameFromInterface(stage interface{}) (string, error) {
	switch stage.(type) {
	case *protocol.Task_Alpha:
		return common.ALPHA_STAGE, nil
	case *protocol.Task_Beta:
		return common.BETA_STAGE, nil
	case *protocol.Task_Gamma:
		return common.GAMMA_STAGE, nil
	case *protocol.Task_Delta_1:
		return common.DELTA_STAGE_1, nil
	case *protocol.Task_Delta_2:
		return common.DELTA_STAGE_2, nil
	case *protocol.Task_Delta_3:
		return common.DELTA_STAGE_3, nil
	case *protocol.Task_Epsilon:
		return common.EPSILON_STAGE, nil
	case *protocol.Task_Zeta:
		return common.ZETA_STAGE, nil
	case *protocol.Task_Eta_1:
		return common.ETA_STAGE_1, nil
	case *protocol.Task_Eta_2:
		return common.ETA_STAGE_2, nil
	case *protocol.Task_Eta_3:
		return common.ETA_STAGE_3, nil
	case *protocol.Task_Theta:
		return common.THETA_STAGE, nil
	case *protocol.Task_Iota:
		return common.IOTA_STAGE, nil
	case *protocol.Task_Kappa_1:
		return common.KAPPA_STAGE_1, nil
	case *protocol.Task_Kappa_2:
		return common.KAPPA_STAGE_2, nil
	case *protocol.Task_Kappa_3:
		return common.KAPPA_STAGE_3, nil
	case *protocol.Task_Lambda:
		return common.LAMBDA_STAGE, nil
	case *protocol.Task_Mu:
		return common.MU_STAGE, nil
	case *protocol.Task_Nu_1:
		return common.NU_STAGE_1, nil
	case *protocol.Task_Nu_2:
		return common.NU_STAGE_2, nil
	case *protocol.Task_Nu_3:
		return common.NU_STAGE_3, nil
	case *protocol.Task_Result1:
		return common.RESULT_STAGE, nil
	case *protocol.Task_Result2:
		return common.RESULT_STAGE, nil
	case *protocol.Task_Result3:
		return common.RESULT_STAGE, nil
	case *protocol.Task_Result4:
		return common.RESULT_STAGE, nil
	case *protocol.Task_Result5:
		return common.RESULT_STAGE, nil
	case *protocol.Task_RingEOF:
		return common.RING_STAGE, nil
	default:
		return "", fmt.Errorf("unknown stage type")
	}
}

func compareProtobufMaps(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Verify that both are maps
	if expectedVal.Kind() != reflect.Map || actualVal.Kind() != reflect.Map {
		log.Error("The provided values are not maps")
		return false
	}

	// Verify that they have the same size
	if expectedVal.Len() != actualVal.Len() {
		log.Error("The maps have different lengths")
		return false
	}

	// Iterate over the keys of the expected map
	for _, key := range expectedVal.MapKeys() {
		expectedValue := expectedVal.MapIndex(key).Interface()
		actualValue := actualVal.MapIndex(key)

		// Verify that the key exists in the actual map
		if !actualValue.IsValid() {
			log.Errorf("Key %v not found in actual map", key)
			return false
		}

		// Verify that the values are equal using proto.Equal
		if !proto.Equal(expectedValue.(proto.Message), actualValue.Interface().(proto.Message)) {
			log.Errorf("Values for key %v do not match: expected %v, got %v", key, expectedValue, actualValue.Interface())
			return false
		}
	}

	return true
}

func CompareProtobufMapsOfArrays(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Verify that both are maps
	if expectedVal.Kind() != reflect.Map || actualVal.Kind() != reflect.Map {
		log.Error("The provided values are not maps")
		return false
	}

	// Verify that they have the same size
	if expectedVal.Len() != actualVal.Len() {
		log.Error("The maps have different lengths")
		return false
	}

	// Iterate over the keys of the expected map
	for _, key := range expectedVal.MapKeys() {
		expectedValue := expectedVal.MapIndex(key).Interface()
		actualValue := actualVal.MapIndex(key)

		// Verify that the key exists in the actual map
		if !actualValue.IsValid() {
			log.Errorf("Key %v not found in actual map", key)
			return false
		}

		// Verify that the values are slices
		expectedSlice := reflect.ValueOf(expectedValue)
		actualSlice := reflect.ValueOf(actualValue.Interface())

		if expectedSlice.Kind() != reflect.Slice || actualSlice.Kind() != reflect.Slice {
			log.Errorf("Values for key %v are not slices", key)
			return false
		}

		// Verify that the slices have the same length
		if expectedSlice.Len() != actualSlice.Len() {
			log.Errorf("Slices for key %v have different lengths", key)
			return false
		}

		// Compare the elements of the slices
		for i := 0; i < expectedSlice.Len(); i++ {
			if !proto.Equal(expectedSlice.Index(i).Interface().(proto.Message), actualSlice.Index(i).Interface().(proto.Message)) {
				log.Errorf("Elements at index %d for key %v do not match: expected %v, got %v", i, key, expectedSlice.Index(i).Interface(), actualSlice.Index(i).Interface())
				return false
			}
		}
	}

	return true

}

func compareStruct(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Si son punteros, obtener el valor al que apuntan
	if expectedVal.Kind() == reflect.Ptr {
		if expectedVal.IsNil() && actualVal.IsNil() {
			return true
		}
		if expectedVal.IsNil() || actualVal.IsNil() {
			return false
		}
		expectedVal = expectedVal.Elem()
	}
	if actualVal.Kind() == reflect.Ptr {
		actualVal = actualVal.Elem()
	}

	// Ambos deben ser structs
	if expectedVal.Kind() != reflect.Struct || actualVal.Kind() != reflect.Struct {
		log.Error("Both values must be structs")
		return false
	}

	t := expectedVal.Type()
	for i := 0; i < expectedVal.NumField(); i++ {
		fieldName := t.Field(i).Name
		expectedField := expectedVal.Field(i).Interface()
		actualField := actualVal.Field(i).Interface()

		// Si el campo es un mapa, usar compareProtobufMaps
		if expectedVal.Field(i).Kind() == reflect.Map {
			if !compareProtobufMaps(expectedField, actualField) {
				log.Errorf("Map field %s does not match", fieldName)
				return false
			}
			// Si el campo es un struct, comparar recursivamente
		} else if expectedVal.Field(i).Kind() == reflect.Struct {
			if !compareStruct(expectedField, actualField) {
				log.Errorf("Struct field %s does not match", fieldName)
				return false
			}
			// Para el resto, usar DeepEqual
		} else {
			if !reflect.DeepEqual(expectedField, actualField) {
				log.Errorf("Field %s does not match: expected %v, got %v", fieldName, expectedField, actualField)
				return false
			}
		}
	}
	return true
}

func CompareMergerPartialResultsMap(
	expected, actual map[string]*common.MergerPartialResults,
) bool {
	if len(expected) != len(actual) {
		log.Errorf("Different number of clients: expected %d, got %d", len(expected), len(actual))
		return false
	}
	for clientID, expectedResult := range expected {
		actualResult, ok := actual[clientID]
		if !ok {
			log.Errorf("Client %s not found in actual results", clientID)
			return false
		}
		// Comparar Delta3
		if !compareProtobufMaps(expectedResult.Delta3.Data, actualResult.Delta3.Data) {
			log.Errorf("Delta3 mismatch for client %s", clientID)
			return false
		}

		// Comparar Eta3
		if !compareProtobufMaps(expectedResult.Eta3.Data, actualResult.Eta3.Data) {
			log.Errorf("Eta3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Kappa3
		if !compareProtobufMaps(expectedResult.Kappa3.Data, actualResult.Kappa3.Data) {
			log.Errorf("Kappa3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Nu3Data
		if !compareProtobufMaps(expectedResult.Nu3Data.Data, actualResult.Nu3Data.Data) {
			log.Errorf("Nu3Data mismatch for client %s", clientID)
			return false
		}
	}
	return true
}
