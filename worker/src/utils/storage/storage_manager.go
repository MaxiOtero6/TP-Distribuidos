package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/topkheap"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

const CLEANUP_INTERVAL = 10 * time.Minute

const VALID_STATUS = "valid"
const INVALID_STATUS = "invalid"

const FINAL_DATA_FILE_NAME = "data"
const FINAL_METADATA_FILE_NAME = "metadata"
const TEMPORARY_DATA_FILE_NAME = "data_temp"
const TEMPORARY_METADATA_FILE_NAME = "metadata_temp"
const JSON_FILE_EXTENSION = ".json"

const MIN = "min"
const MAX = "max"

var log = logging.MustGetLogger("log")

type OutputFile struct {
	Data       interface{} `json:"data"`
	Timestamps string      `json:"timestamps"`
	Status     string      `json:"status"`
}

type MetadataFile struct {
	Timestamps     string `json:"timestamps"`
	Status         string `json:"status"`
	OmegaProcessed bool   `json:"omega_processed"`
	RingRound      uint32 `json:"ring_round"`
}

type MetaData struct {
	OmegaProcessed bool   `json:"omega_processed"`
	RingRound      uint32 `json:"ring_round"`
}

type FileStructure struct {
	Name string `json:"name"`
	Type string `json:"type"`
}

// Generic loader for PartialData[T] using protojson
func decodeJsonToProtoData[T proto.Message](jsonBytes []byte, newT func() T) (*common.PartialData[T], error) {
	var temp struct {
		Data      map[string]json.RawMessage `json:"data"`
		Timestamp string                     `json:"timestamp"`
		Status    string                     `json:"status"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return nil, err
	}
	result := &common.PartialData[T]{
		Data: make(map[string]T),
	}
	for k, raw := range temp.Data {
		msg := newT()
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return nil, err
		}
		result.Data[k] = msg
	}

	return result, nil
}

func decodeFromJsonMetadata(jsonBytes []byte) (MetadataFile, error) {
	var temp struct {
		OmegaProcessed bool   `json:"omega_processed"`
		RingRound      uint32 `json:"ring_round"`
		Timestamps     string `json:"timestamps"`
		Status         string `json:"status"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return MetadataFile{}, err
	}
	return MetadataFile{
		Timestamps:     temp.Timestamps,
		Status:         temp.Status,
		OmegaProcessed: temp.OmegaProcessed,
		RingRound:      temp.RingRound,
	}, nil
}

// Generic disk loader for any PartialData[T]
func loadStageClientData[T proto.Message](
	dir string, stage string, fileType common.FolderType, clientId string, newT func() T,
) (*common.PartialData[T], error) {
	var zero *common.PartialData[T]

	dirPath := filepath.Join(dir, stage, string(fileType), clientId)

	finalDataFilePath := filepath.Join(dirPath, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	finalMetadataFilePath := filepath.Join(dirPath, FINAL_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	tempDataFilePath := filepath.Join(dirPath, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)
	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	err := handleLeftOverFiles(finalDataFilePath, tempDataFilePath)
	if err != nil {
		return zero, fmt.Errorf("failed to handle leftover files: %w", err)
	}

	err = handleLeftOverFiles(finalMetadataFilePath, tempMetadataFilePath)
	if err != nil {
		return zero, fmt.Errorf("failed to handle leftover files: %w", err)
	}

	//Read data file
	jsonDataBytes, err := os.ReadFile(finalDataFilePath)
	if err != nil {
		return zero, fmt.Errorf("error reading file %s: %w", finalDataFilePath, err)
	}

	//TODO usar la metada para validar que es valido el archivo de logs
	partial, err := decodeJsonToProtoData(jsonDataBytes, newT)
	if err != nil {
		return zero, err
	}

	// Read metadata file
	jsonMetadataBytes, err := os.ReadFile(finalMetadataFilePath)
	if err != nil {
		return zero, fmt.Errorf("error reading file %s: %w", finalMetadataFilePath, err)
	}
	metadata, err := decodeFromJsonMetadata(jsonMetadataBytes)
	if err != nil {
		return zero, err
	}

	// Set metadata fields in the partial data
	partial.OmegaProcessed = metadata.OmegaProcessed
	partial.RingRound = metadata.RingRound

	return partial, nil
}

// Generic stage loader for any PartialData[T] into any result struct R
func loadStageData[T proto.Message, R any](
	dir string,
	stage string,
	fileType common.FolderType,
	partialResults map[string]*R,
	setter func(result *R, data *common.PartialData[T]),
	newT func() T,
) {

	log.Infof("Loading stage data from disk for stage: %s, fileType: %s", stage, fileType)

	stageDir := filepath.Join(dir, stage, string(fileType))
	clientDirs, err := os.ReadDir(stageDir)
	if err != nil {
		log.Errorf("Error reading directory %s: %v", stageDir, err)
		return
	}

	for _, clientEntry := range clientDirs {
		if !clientEntry.IsDir() {
			continue
		}
		clientId := clientEntry.Name()
		data, err := loadStageClientData(dir, stage, fileType, clientId, newT)
		if err != nil {
			log.Errorf("Error loading data for stage %s, client %s: %v", stage, clientId, err)
			continue
		}
		if _, ok := partialResults[clientId]; !ok {
			partialResults[clientId] = new(R)
		}
		setter(partialResults[clientId], data)
	}
}

func LoadMergerPartialResultsFromDisk(dir string) (map[string]*common.MergerPartialResults, error) {
	partialResults := make(map[string]*common.MergerPartialResults)
	folderType := common.GENERAL_FOLDER_TYPE

	loadStageData(dir, common.DELTA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Delta_3_Data]) {
		result.Delta3 = data
	}, func() *protocol.Delta_3_Data { return &protocol.Delta_3_Data{} })

	loadStageData(dir, common.ETA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Eta_3_Data]) {
		result.Eta3 = data
	}, func() *protocol.Eta_3_Data { return &protocol.Eta_3_Data{} })

	loadStageData(dir, common.KAPPA_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Kappa_3_Data]) {
		result.Kappa3 = data
	}, func() *protocol.Kappa_3_Data { return &protocol.Kappa_3_Data{} })

	loadStageData(dir, common.NU_STAGE_3, folderType, partialResults, func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Nu_3_Data]) {
		result.Nu3 = data
	}, func() *protocol.Nu_3_Data { return &protocol.Nu_3_Data{} })

	return partialResults, nil
}

func LoadReducerPartialResultsFromDisk(dir string) (map[string]*common.ReducerPartialResults, error) {
	partialResults := make(map[string]*common.ReducerPartialResults)
	fileType := common.GENERAL_FOLDER_TYPE

	loadStageData(dir, common.DELTA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Delta_2_Data]) {
		result.Delta2 = data
	}, func() *protocol.Delta_2_Data { return &protocol.Delta_2_Data{} })

	loadStageData(dir, common.ETA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Eta_2_Data]) {
		result.Eta2 = data
	}, func() *protocol.Eta_2_Data { return &protocol.Eta_2_Data{} })

	loadStageData(dir, common.KAPPA_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Kappa_2_Data]) {
		result.Kappa2 = data
	}, func() *protocol.Kappa_2_Data { return &protocol.Kappa_2_Data{} })

	loadStageData(dir, common.NU_STAGE_2, fileType, partialResults, func(result *common.ReducerPartialResults, data *common.PartialData[*protocol.Nu_2_Data]) {
		result.Nu2 = data
	}, func() *protocol.Nu_2_Data { return &protocol.Nu_2_Data{} })

	return partialResults, nil
}

func LoadTopperPartialResultsFromDisk(dir string) (map[string]*common.TopperPartialResults, error) {
	partialResults := make(map[string]*common.TopperPartialResults)
	folderType := common.GENERAL_FOLDER_TYPE

	loadStageData(dir, common.EPSILON_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Epsilon_Data]) {

			if result.EpsilonData == nil {
				result.EpsilonData = &common.TopperPartialData[uint64, *protocol.Epsilon_Data]{}
			}
			result.EpsilonData.OmegaProcessed = data.OmegaProcessed
			result.EpsilonData.RingRound = data.RingRound
			result.EpsilonData.Heap = partialDatatoHeap(common.TYPE_MAX, data.Data,
				func(item *protocol.Epsilon_Data) uint64 {
					return item.GetTotalInvestment()
				})

		},
		func() *protocol.Epsilon_Data { return &protocol.Epsilon_Data{} })

	loadStageData(dir, common.LAMBDA_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Lambda_Data]) {
			if result.LamdaData == nil {
				result.LamdaData = &common.TopperPartialData[uint64, *protocol.Lambda_Data]{}
			}

			result.LamdaData.RingRound = data.RingRound
			result.LamdaData.OmegaProcessed = data.OmegaProcessed
			result.LamdaData.Heap = partialDatatoHeap(common.TYPE_MAX, data.Data,
				func(item *protocol.Lambda_Data) uint64 {
					return item.GetParticipations()
				})

		},
		func() *protocol.Lambda_Data { return &protocol.Lambda_Data{} })

	loadStageData(dir, common.THETA_STAGE, folderType, partialResults,
		func(result *common.TopperPartialResults, data *common.PartialData[*protocol.Theta_Data]) {
			if result.ThetaData == nil {
				result.ThetaData = &common.ThetaPartialData{
					MinPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{},
					MaxPartialData: &common.TopperPartialData[float32, *protocol.Theta_Data]{},
				}
			}

			result.ThetaData.MinPartialData.OmegaProcessed = data.OmegaProcessed
			result.ThetaData.MinPartialData.RingRound = data.RingRound

			result.ThetaData.MaxPartialData.OmegaProcessed = data.OmegaProcessed
			result.ThetaData.MaxPartialData.RingRound = data.RingRound

			result.ThetaData.MaxPartialData.Heap = maxToHeap(data.Data[MAX],
				func(item *protocol.Theta_Data) float32 {
					return item.GetAvgRating()
				})

			result.ThetaData.MinPartialData.Heap = minToHeap(data.Data[MIN],
				func(item *protocol.Theta_Data) float32 {
					return item.GetAvgRating()
				})

		},
		func() *protocol.Theta_Data { return &protocol.Theta_Data{} })

	return partialResults, nil
}

func heapToPartialData[K topkheap.Ordered, V proto.Message](
	heap topkheap.TopKHeap[K, V],
	data map[string]V,
	keyFunc func(V) K,
) {

	for _, item := range heap.GetTopK() {
		key := keyFunc(item)
		data[fmt.Sprintf("%v", key)] = item
	}
}
func partialDatatoHeap[K topkheap.Ordered, V proto.Message](
	heapType string,
	data map[string]V,
	valueFunc func(V) K,
) topkheap.TopKHeap[K, V] {

	var heap topkheap.TopKHeap[K, V]

	if heapType == common.TYPE_MAX {
		heap = topkheap.NewTopKMaxHeap[K, V](len(data))
	} else {
		heap = topkheap.NewTopKMinHeap[K, V](len(data))
	}

	for _, item := range data {

		log.Infof("Adding item to heap: %v", item)
		heap.Insert(valueFunc(item), item)

	}

	return heap
}
func minToHeap[K topkheap.Ordered, V proto.Message](
	data V,
	keyFunc func(V) K,
) topkheap.TopKHeap[K, V] {
	heap := topkheap.NewTopKMinHeap[K, V](1)
	heap.Insert(keyFunc(data), data)
	return heap
}
func maxToHeap[K topkheap.Ordered, V proto.Message](
	data V,
	keyFunc func(V) K,
) topkheap.TopKHeap[K, V] {
	heap := topkheap.NewTopKMaxHeap[K, V](1)
	heap.Insert(keyFunc(data), data)
	return heap
}

func DeletePartialResults(dir string, stage interface{}, sourceType string, clientId string) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	//TODO hablar bien este caso con Maxi, no se si me falta algun caso en particular
	if stringStage == common.OMEGA_STAGE || stringStage == common.RING_STAGE {
		dirPath := filepath.Join(dir, stringStage, sourceType, clientId)
		return os.RemoveAll(dirPath)
	}
	return nil
}

func commitPartialMetadataToFinal(dirPath string) error {

	log.Infof("Committing metadata for path %s", dirPath)

	tempMetadataFileName := TEMPORARY_METADATA_FILE_NAME + JSON_FILE_EXTENSION
	finalMetadataFileName := FINAL_METADATA_FILE_NAME + JSON_FILE_EXTENSION

	return renameFile(dirPath, tempMetadataFileName, finalMetadataFileName)
}

func commitPartialDataToFinal(dir string, stage string, folderType common.FolderType, clientId string) error {

	if dir != "" {
		log.Infof("Committing data for stage %s, table type %s, client ID %s", stage, folderType, clientId)

		dirPath := filepath.Join(dir, stage, string(folderType), clientId)

		tempDataFileName := TEMPORARY_DATA_FILE_NAME + JSON_FILE_EXTENSION
		finalDataFileName := FINAL_DATA_FILE_NAME + JSON_FILE_EXTENSION

		return renameFile(dirPath, tempDataFileName, finalDataFileName)

	}
	return nil
}

func renameFile(dirPath, tempFileName, finalFileName string) error {
	tempDataFilePath := filepath.Join(dirPath, tempFileName)
	finalDataFilePath := filepath.Join(dirPath, finalFileName)

	log.Infof("Committing data from %s to %s", tempDataFilePath, finalDataFilePath)

	if err := os.Rename(tempDataFilePath, finalDataFilePath); err != nil {
		return fmt.Errorf("error renaming temp file: %w", err)
	}
	return nil
}

func SaveTopperThetaDataToFile[K topkheap.Ordered, T proto.Message](dir string, stage interface{}, clientId string, data []*common.TopperPartialData[K, T], keyFunc func(T) K) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	mapData := make(map[string]T)
	mapData[MAX] = data[0].Heap.GetTopK()[0]
	mapData[MIN] = data[1].Heap.GetTopK()[0]

	topperPartialData := &common.PartialData[T]{
		Data:           mapData,
		OmegaProcessed: data[0].OmegaProcessed,
		RingRound:      data[0].RingRound,
	}

	return saveGeneralDataToFile(dir, clientId, stringStage, common.GENERAL_FOLDER_TYPE, topperPartialData)

}
func SaveTopperDataToFile[K topkheap.Ordered, T proto.Message](dir string, stage interface{}, clientId string, data *common.TopperPartialData[K, T], keyFunc func(T) K) error {

	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	mapData := make(map[string]T)
	heapToPartialData(data.Heap, mapData, keyFunc)

	topperPartialData := &common.PartialData[T]{
		Data:           mapData,
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
	}

	err = saveGeneralDataToFile(dir, clientId, stringStage, common.GENERAL_FOLDER_TYPE, topperPartialData)
	if err != nil {
		return fmt.Errorf("error saving general data to file: %w", err)
	}

	return commitPartialDataToFinal(dir, stringStage, common.GENERAL_FOLDER_TYPE, clientId)
}
func SaveTopperMetadataToFile[K topkheap.Ordered, T proto.Message](dir string, clientId string, stage string, data *common.TopperPartialData[K, T]) error {

	dirPath := filepath.Join(dir, stage, clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	metadata := MetaData{
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
	}

	err = writeMetadataToTempFile(tempMetadataFilePath, metadata)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	err = commitPartialMetadataToFinal(dirPath)
	if err != nil {
		return fmt.Errorf("failed to commit metadata to final file: %w", err)
	}
	return nil
}

func SaveDataToFile[T proto.Message](dir string, clientId string, stage interface{}, tableType common.FolderType, data *common.PartialData[T]) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	err = saveGeneralDataToFile(dir, clientId, stringStage, tableType, data)
	if err != nil {
		return fmt.Errorf("error saving general data to file: %w", err)
	}

	return commitPartialDataToFinal(dir, stringStage, tableType, clientId)
}
func SaveMetadataToFile[T proto.Message](dir string, clientId string, stage string, tableType common.FolderType, data *common.PartialData[T]) error {

	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	metadata := MetaData{
		OmegaProcessed: data.OmegaProcessed,
		RingRound:      data.RingRound,
	}

	err = writeMetadataToTempFile(tempMetadataFilePath, metadata)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	err = commitPartialMetadataToFinal(dirPath)
	if err != nil {
		return fmt.Errorf("failed to commit metadata to final file: %w", err)
	}
	return nil
}
func saveGeneralDataToFile[T proto.Message](dir string, clientId string, stage string, tableType common.FolderType, data *common.PartialData[T]) error {

	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempDataFilePath := filepath.Join(dirPath, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)

	err = marshallGeneralPartialData(tempDataFilePath, data)

	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	return nil
}
func marshallGeneralPartialData[T proto.Message](tempDataFilePath string, data *common.PartialData[T]) error {

	marshaler := protojson.MarshalOptions{
		Indent:          "  ", // Pretty print
		EmitUnpopulated: true, // Include unpopulated fields
	}
	return processGeneralDataTypedStruct(data, tempDataFilePath, marshaler)
}
func processGeneralDataTypedStruct[T proto.Message](typedStruct *common.PartialData[T], tempDataFilePath string, marshaler protojson.MarshalOptions) error {

	log.Infof("Processing data to download to file: %s", tempDataFilePath)

	jsonMap := make(map[string]json.RawMessage)
	for key, msg := range typedStruct.Data {
		marshaledData, err := marshaler.Marshal(msg)
		if err != nil {
			return fmt.Errorf("error marshaling data to JSON: %w", err)
		}
		jsonMap[key] = marshaledData
	}

	outputData := OutputFile{
		Data:       jsonMap,
		Timestamps: time.Now().UTC().Format(time.RFC3339),
		Status:     VALID_STATUS,
	}

	//TODO add fragments to log

	if err := writeToTempFile(tempDataFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}

	return nil
}

func writeMetadataToTempFile(tempMetadataFilePath string, metadata MetaData) error {
	outputMetadata := MetadataFile{
		Timestamps:     time.Now().UTC().Format(time.RFC3339),
		Status:         VALID_STATUS,
		OmegaProcessed: metadata.OmegaProcessed,
		RingRound:      metadata.RingRound,
	}

	err := writeToTempFile(tempMetadataFilePath, outputMetadata)
	if err != nil {
		return fmt.Errorf("error writing metadata to temp file: %w", err)
	}
	return nil

}

func writeToTempFile[T any](tempFilePath string, output T) error {

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

// func decodeZetaDataMovie(decoder *json.Decoder) (map[string]*protocol.Zeta_Data_Movie, error) {
// 	//var tempArray ZetaMovieArrayJSON
// 	// if err := decoder.Decode(&tempArray); err != nil {
// 	// 	return nil, fmt.Errorf("error decoding JSON array: %w", err)
// 	// }

// 	// result := make(map[string]*protocol.Zeta_Data_Movie)
// 	// for _, item := range tempArray.Data {
// 	// 	result[item.MovieId] = &protocol.Zeta_Data_Movie{
// 	// 		MovieId: item.MovieId,
// 	// 		Title:   item.Title,
// 	// 	}
// 	// }
// 	// return result, nil
// }

// func decodeZetaDataRating(decoder *json.Decoder) (map[string][]*protocol.Zeta_Data_Rating, error) {
// 	// var tempMap ZetaRatingMapJSON

// 	// if err := decoder.Decode(&tempMap); err != nil {
// 	// 	return nil, fmt.Errorf("error decoding JSON array: %w", err)
// 	// }

// 	// result := make(map[string][]*protocol.Zeta_Data_Rating)
// 	// for _, item := range tempMap.Data {
// 	// 	for _, item := range item {
// 	// 		result[item.MovieId] = append(result[item.MovieId], &protocol.Zeta_Data_Rating{
// 	// 			MovieId: item.MovieId,
// 	// 			Rating:  item.Rating,
// 	// 		})

// 	// 	}
// 	// }
// 	// return result, nil
//}

// func decodeIotaDataMovie(decoder *json.Decoder) (map[string]*protocol.Iota_Data_Movie, error) {
// 	var tempMap struct {
// 		Data []struct {
// 			MovieId string `json:"movieId"`
// 		} `json:"data"`
// 	}
// 	if err := decoder.Decode(&tempMap); err != nil {
// 		return nil, fmt.Errorf("error decoding JSON array: %w", err)
// 	}
// 	result := make(map[string]*protocol.Iota_Data_Movie)
// 	for _, item := range tempMap.Data {
// 		result[item.MovieId] = &protocol.Iota_Data_Movie{
// 			MovieId: item.MovieId,
// 		}
// 	}
// 	return result, nil
// }

// func decodeIotaDataActor(decoder *json.Decoder) (map[string][]*protocol.Iota_Data_Actor, error) {
// 	var tempMap struct {
// 		Data map[string][]struct {
// 			MovieId   string `json:"movieId"`
// 			ActorId   string `json:"actorId"`
// 			ActorName string `json:"actorName"`
// 		} `json:"data"`
// 	}
// 	if err := decoder.Decode(&tempMap); err != nil {
// 		return nil, fmt.Errorf("error decoding JSON map: %w", err)
// 	}
// 	result := make(map[string][]*protocol.Iota_Data_Actor)
// 	for movieId, items := range tempMap.Data {
// 		for _, item := range items {
// 			result[movieId] = append(result[movieId], &protocol.Iota_Data_Actor{
// 				MovieId:   item.MovieId,
// 				ActorId:   item.ActorId,
// 				ActorName: item.ActorName,
// 			})
// 		}
// 	}
// 	return result, nil
// }

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
	case *protocol.Task_RingEOF:
		return common.RING_STAGE, nil
	case *protocol.Task_OmegaEOF:
		return common.OMEGA_STAGE, nil
	default:
		return "", fmt.Errorf("unknown stage type")
	}
}

// Compare functions
func compareTopperPartialResults(a, b *common.TopperPartialResults) bool {
	if a == nil && b == nil {
		log.Info("Both TopperPartialResults are nil")
		return true
	}
	if a == nil || b == nil {
		log.Error("One of the TopperPartialResults is nil")
		return false
	}

	// EpsilonData
	if (a.EpsilonData == nil) != (b.EpsilonData == nil) {
		log.Error("EpsilonData nil mismatch")
		return false
	}
	if a.EpsilonData != nil {
		if a.EpsilonData.OmegaProcessed != b.EpsilonData.OmegaProcessed {
			log.Errorf("EpsilonData.OmegaProcessed mismatch: %v vs %v", a.EpsilonData.OmegaProcessed, b.EpsilonData.OmegaProcessed)
			return false
		}
		if a.EpsilonData.RingRound != b.EpsilonData.RingRound {
			log.Errorf("EpsilonData.RingRound mismatch: %v vs %v", a.EpsilonData.RingRound, b.EpsilonData.RingRound)
			return false
		}
		// Comparación manual de heaps
		aItems := a.EpsilonData.Heap.GetTopK()
		bItems := b.EpsilonData.Heap.GetTopK()
		if len(aItems) != len(bItems) {
			log.Errorf("Epsilon Heap lengths: %d vs %d", len(aItems), len(bItems))
			return false
		}
		for i := range aItems {
			if !proto.Equal(aItems[i], bItems[i]) {
				log.Errorf("Epsilon Heap mismatch at index %d", i)
				log.Infof("a.EpsilonData.Heap[%d]: %+v", i, aItems[i])
				log.Infof("b.EpsilonData.Heap[%d]: %+v", i, bItems[i])
				return false
			}
		}
	}

	// LamdaData
	if (a.LamdaData == nil) != (b.LamdaData == nil) {
		log.Error("LamdaData nil mismatch")
		return false
	}
	if a.LamdaData != nil {
		if a.LamdaData.OmegaProcessed != b.LamdaData.OmegaProcessed {
			log.Errorf("LamdaData.OmegaProcessed mismatch: %v vs %v", a.LamdaData.OmegaProcessed, b.LamdaData.OmegaProcessed)
			return false
		}
		if a.LamdaData.RingRound != b.LamdaData.RingRound {
			log.Errorf("LamdaData.RingRound mismatch: %v vs %v", a.LamdaData.RingRound, b.LamdaData.RingRound)
			return false
		}
		aItems := a.LamdaData.Heap.GetTopK()
		bItems := b.LamdaData.Heap.GetTopK()
		if len(aItems) != len(bItems) {
			log.Errorf("Lambda Heap lengths: %d vs %d", len(aItems), len(bItems))
			return false
		}
		for i := range aItems {
			if !proto.Equal(aItems[i], bItems[i]) {
				log.Errorf("Lambda Heap mismatch at index %d", i)
				log.Infof("a.LamdaData.Heap[%d]: %+v", i, aItems[i])
				log.Infof("b.LamdaData.Heap[%d]: %+v", i, bItems[i])
				return false
			}
		}
	}

	// ThetaData
	if (a.ThetaData == nil) != (b.ThetaData == nil) {
		log.Error("ThetaData nil mismatch")
		return false
	}
	if a.ThetaData != nil {
		// MinPartialData
		if (a.ThetaData.MinPartialData == nil) != (b.ThetaData.MinPartialData == nil) {
			log.Error("ThetaData.MinPartialData nil mismatch")
			return false
		}
		if a.ThetaData.MinPartialData != nil {
			if a.ThetaData.MinPartialData.OmegaProcessed != b.ThetaData.MinPartialData.OmegaProcessed {
				log.Errorf("ThetaData.MinPartialData.OmegaProcessed mismatch: %v vs %v", a.ThetaData.MinPartialData.OmegaProcessed, b.ThetaData.MinPartialData.OmegaProcessed)
				return false
			}
			if a.ThetaData.MinPartialData.RingRound != b.ThetaData.MinPartialData.RingRound {
				log.Errorf("ThetaData.MinPartialData.RingRound mismatch: %v vs %v", a.ThetaData.MinPartialData.RingRound, b.ThetaData.MinPartialData.RingRound)
				return false
			}
			aItems := a.ThetaData.MinPartialData.Heap.GetTopK()
			bItems := b.ThetaData.MinPartialData.Heap.GetTopK()
			if len(aItems) != len(bItems) {
				log.Errorf("Theta Min Heap lengths: %d vs %d", len(aItems), len(bItems))
				return false
			}
			for i := range aItems {
				if !proto.Equal(aItems[i], bItems[i]) {
					log.Errorf("Theta Min Heap mismatch at index %d", i)
					log.Infof("a.ThetaData.MinPartialData.Heap[%d]: %+v", i, aItems[i])
					log.Infof("b.ThetaData.MinPartialData.Heap[%d]: %+v", i, bItems[i])
					return false
				}
			}
		}
		// MaxPartialData
		if (a.ThetaData.MaxPartialData == nil) != (b.ThetaData.MaxPartialData == nil) {
			log.Error("ThetaData.MaxPartialData nil mismatch")
			return false
		}
		if a.ThetaData.MaxPartialData != nil {
			if a.ThetaData.MaxPartialData.OmegaProcessed != b.ThetaData.MaxPartialData.OmegaProcessed {
				log.Errorf("ThetaData.MaxPartialData.OmegaProcessed mismatch: %v vs %v", a.ThetaData.MaxPartialData.OmegaProcessed, b.ThetaData.MaxPartialData.OmegaProcessed)
				return false
			}
			if a.ThetaData.MaxPartialData.RingRound != b.ThetaData.MaxPartialData.RingRound {
				log.Errorf("ThetaData.MaxPartialData.RingRound mismatch: %v vs %v", a.ThetaData.MaxPartialData.RingRound, b.ThetaData.MaxPartialData.RingRound)
				return false
			}
			aItems := a.ThetaData.MaxPartialData.Heap.GetTopK()
			bItems := b.ThetaData.MaxPartialData.Heap.GetTopK()
			if len(aItems) != len(bItems) {
				log.Errorf("Theta Max Heap lengths: %d vs %d", len(aItems), len(bItems))
				return false
			}
			for i := range aItems {
				if !proto.Equal(aItems[i], bItems[i]) {
					log.Errorf("Theta Max Heap mismatch at index %d", i)
					log.Infof("a.ThetaData.MaxPartialData.Heap[%d]: %+v", i, aItems[i])
					log.Infof("b.ThetaData.MaxPartialData.Heap[%d]: %+v", i, bItems[i])
					return false
				}
			}
		}
	}

	log.Info("TopperPartialResults are equal")
	return true
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
		if !compareStruct(expectedResult.Delta3, actualResult.Delta3) {
			log.Errorf("Delta3 mismatch for client %s", clientID)
			return false
		}

		// Comparar Eta3
		if !compareStruct(expectedResult.Eta3, actualResult.Eta3) {
			log.Errorf("Eta3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Kappa3
		if !compareStruct(expectedResult.Kappa3, actualResult.Kappa3) {
			log.Errorf("Kappa3 mismatch for client %s", clientID)
			return false
		}
		// Comparar Nu3
		if !compareStruct(expectedResult.Nu3, actualResult.Nu3) {
			log.Errorf("Nu3 mismatch for client %s", clientID)
			return false
		}
	}
	return true
}
func CompareReducerPartialResultsMap(
	expected, actual map[string]*common.ReducerPartialResults,
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
		// Comparar Delta2
		if !compareStruct(expectedResult.Delta2, actualResult.Delta2) {
			log.Errorf("Delta2 mismatch for client %s", clientID)
			return false
		}

		// Comparar Eta2
		if !compareStruct(expectedResult.Eta2, actualResult.Eta2) {
			log.Errorf("Eta2 mismatch for client %s", clientID)
			return false
		}
		// Comparar Kappa2
		if !compareStruct(expectedResult.Kappa2, actualResult.Kappa2) {
			log.Errorf("Kappa2 mismatch for client %s", clientID)
			return false
		}
		// Comparar Nu2
		if !compareStruct(expectedResult.Nu2, actualResult.Nu2) {
			log.Errorf("Nu2 mismatch for client %s", clientID)
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
			log.Infof("Comparing field %s", fieldName)
			log.Infof("Expected: %v, Actual: %v", expectedField, actualField)
			if !reflect.DeepEqual(expectedField, actualField) {
				log.Errorf("Field %s does not match: expected %v, got %v", fieldName, expectedField, actualField)
				return false
			}
		}
	}
	return true
}
