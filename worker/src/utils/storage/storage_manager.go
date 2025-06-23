package storage

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
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
	Timestamps          string `json:"timestamps"`
	Status              string `json:"status"`
	OmegaProcessed      bool   `json:"omega_processed"`
	RingRound           uint32 `json:"ring_round"`
	SendedTaskCount     int    `json:"sended_task_count"`
	SmallTableTaskCount int    `json:"small_table_task_count"`
	Ready               bool   `json:"ready"`
}

type MetaData struct {
	OmegaProcessed      bool   `json:"omega_processed"`
	RingRound           uint32 `json:"ring_round"`
	SendedTaskCount     int    `json:"sended_task_count"`
	SmallTableTaskCount int    `json:"small_table_task_count"`
	Ready               bool   `json:"ready"`
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

func decodeGeneralFromJsonMetadata(jsonBytes []byte) (MetadataFile, error) {
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

func decodeJoinerMetadataFromJson(jsonBytes []byte) (MetadataFile, error) {
	var temp MetadataFile
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return MetadataFile{}, err
	}
	return temp, nil
}

func decodeJsonToJoinerBigTableData[B proto.Message](
	jsonBytes []byte,
	newB func() B,
) (*common.JoinerTableData[common.BigTableData[B]], error) {
	var temp struct {
		Data      map[uint32]map[string][]json.RawMessage `json:"data"`
		Timestamp string                                  `json:"timestamp"`
		Status    string                                  `json:"status"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return nil, err
	}
	result := &common.JoinerTableData[common.BigTableData[B]]{
		Data: make(common.BigTableData[B]),
	}
	for outerKey, innerMap := range temp.Data {
		result.Data[outerKey] = make(map[string][]B)
		for innerKey, rawSlice := range innerMap {
			for _, raw := range rawSlice {
				msg := newB()
				if err := protojson.Unmarshal(raw, msg); err != nil {
					return nil, err
				}
				result.Data[outerKey][innerKey] = append(result.Data[outerKey][innerKey], msg)
			}
		}
	}
	return result, nil
}

func decodeJsonToJoinerSmallTableData[S proto.Message](
	jsonBytes []byte,
	newS func() S,
) (*common.JoinerTableData[common.SmallTableData[S]], error) {
	var temp struct {
		Data      map[string]json.RawMessage `json:"data"`
		Timestamp string                     `json:"timestamp"`
		Status    string                     `json:"status"`
	}
	if err := json.Unmarshal(jsonBytes, &temp); err != nil {
		return nil, err
	}
	result := &common.JoinerTableData[common.SmallTableData[S]]{
		Data: make(common.SmallTableData[S]),
	}
	for k, raw := range temp.Data {
		msg := newS()
		if err := protojson.Unmarshal(raw, msg); err != nil {
			return nil, err
		}
		result.Data[k] = msg
	}
	return result, nil
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
	metadata, err := decodeGeneralFromJsonMetadata(jsonMetadataBytes)
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

func LoadJoinerPartialResultsFromDisk(
	dir string,
) (map[string]*common.JoinerPartialResults, error) {
	partialResults := make(map[string]*common.JoinerPartialResults)

	// IOTA
	err := loadJoinerStageToPartialResults(
		dir,
		common.IOTA_STAGE,
		func() *protocol.Iota_Data_Movie { return &protocol.Iota_Data_Movie{} },
		func() *protocol.Iota_Data_Actor { return &protocol.Iota_Data_Actor{} },
		func(jpr *common.JoinerPartialResults) *common.JoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor] {
			if jpr.IotaData == nil {
				jpr.IotaData = &common.JoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{}
			}
			return jpr.IotaData
		},
		partialResults,
	)
	if err != nil {
		return nil, fmt.Errorf("error loading IOTA joiner stage: %w", err)
	}

	// ZETA
	err = loadJoinerStageToPartialResults(
		dir,
		common.ZETA_STAGE,
		func() *protocol.Zeta_Data_Movie { return &protocol.Zeta_Data_Movie{} },
		func() *protocol.Zeta_Data_Rating { return &protocol.Zeta_Data_Rating{} },
		func(jpr *common.JoinerPartialResults) *common.JoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating] {
			if jpr.ZetaData == nil {
				jpr.ZetaData = &common.JoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{}
			}
			return jpr.ZetaData
		},
		partialResults,
	)
	if err != nil {
		return nil, fmt.Errorf("error loading ZETA joiner stage: %w", err)
	}

	return partialResults, nil

}

// Esta función carga los datos de un stage y los setea en el campo correspondiente de JoinerPartialResults
func loadJoinerStageToPartialResults[S proto.Message, B proto.Message](
	dir string,
	stage string,
	newS func() S,
	newB func() B,
	getStageData func(*common.JoinerPartialResults) *common.JoinerStageData[S, B],
	partialResults map[string]*common.JoinerPartialResults,
) error {
	// SmallTable
	smallTableDir := filepath.Join(dir, stage, string(common.JOINER_SMALL_FOLDER_TYPE))
	smallClients, err := os.ReadDir(smallTableDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error reading small table dir: %w", err)
	}
	for _, entry := range smallClients {
		if !entry.IsDir() {
			continue
		}
		clientId := entry.Name()
		dataPath := filepath.Join(smallTableDir, clientId, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
		metadataPath := filepath.Join(smallTableDir, clientId, FINAL_METADATA_FILE_NAME+JSON_FILE_EXTENSION)
		jsonBytes, err := os.ReadFile(dataPath)
		if err != nil {
			log.Errorf("Error reading small table data for client %s: %v", clientId, err)
			continue
		}
		smallTable, err := decodeJsonToJoinerSmallTableData(jsonBytes, newS)
		if err != nil {
			log.Errorf("Error decoding small table for client %s: %v", clientId, err)
			continue
		}
		if _, ok := partialResults[clientId]; !ok {
			partialResults[clientId] = &common.JoinerPartialResults{}
		}
		getStageData(partialResults[clientId]).SmallTable = smallTable

		// Leer y aplicar metadata
		jsonMeta, err := os.ReadFile(metadataPath)
		if err == nil {
			metadata, err := decodeJoinerMetadataFromJson(jsonMeta)
			if err != nil {
				log.Errorf("Error decoding metadata for client %s: %v", clientId, err)
				continue
			}

			smallTable.OmegaProcessed = metadata.OmegaProcessed
			smallTable.Ready = metadata.Ready
			stageData := getStageData(partialResults[clientId])
			stageData.SendedTaskCount = metadata.SendedTaskCount
			stageData.SmallTableTaskCount = metadata.SmallTableTaskCount

		}
	}

	// BigTable
	bigTableDir := filepath.Join(dir, stage, string(common.JOINER_BIG_FOLDER_TYPE))
	bigClients, err := os.ReadDir(bigTableDir)
	if err != nil && !os.IsNotExist(err) {
		return fmt.Errorf("error reading big table dir: %w", err)
	}
	for _, entry := range bigClients {
		if !entry.IsDir() {
			continue
		}
		clientId := entry.Name()
		dataPath := filepath.Join(bigTableDir, clientId, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION)
		metadataPath := filepath.Join(bigTableDir, clientId, FINAL_METADATA_FILE_NAME+JSON_FILE_EXTENSION)
		jsonBytes, err := os.ReadFile(dataPath)
		if err != nil {
			log.Errorf("Error reading big table data for client %s: %v", clientId, err)
			continue
		}
		bigTable, err := decodeJsonToJoinerBigTableData(jsonBytes, newB)
		if err != nil {
			log.Errorf("Error decoding big table for client %s: %v", clientId, err)
			continue
		}
		if _, ok := partialResults[clientId]; !ok {
			partialResults[clientId] = &common.JoinerPartialResults{}
		}
		getStageData(partialResults[clientId]).BigTable = bigTable

		// Leer y aplicar metadata
		jsonMeta, err := os.ReadFile(metadataPath)
		if err == nil {
			metadata, err := decodeJoinerMetadataFromJson(jsonMeta)
			if err != nil {
				log.Errorf("Error decoding metadata for client %s: %v", clientId, err)
				continue
			}

			bigTable.OmegaProcessed = metadata.OmegaProcessed
			bigTable.Ready = metadata.Ready
			stageData := getStageData(partialResults[clientId])
			stageData.SendedTaskCount = metadata.SendedTaskCount
			stageData.RingRound = metadata.RingRound
		}
	}

	return nil
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

func DeletePartialResults(dir string, stage interface{}, tableType string, clientId string) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	dirPath := filepath.Join(dir, stringStage, tableType, clientId)
	return os.RemoveAll(dirPath)
}

// TODO --> DELETE LOGS FILES TOO
func TryDeletePartialData(dir string, stage interface{}, folderType string, clientId string, isReadyToDelete bool) error {
	stringStage, err := getStageNameFromInterface(stage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}
	dirPath := filepath.Join(dir, stringStage, string(folderType), clientId)

	// TODO delete logs file too
	err = os.Remove(filepath.Join(dirPath, FINAL_DATA_FILE_NAME+JSON_FILE_EXTENSION))
	if err != nil {
		return fmt.Errorf("error deleting partial data for stage %s, folder type %s, client ID %s: %w", stringStage, folderType, clientId, err)
	}

	log.Debugf("Deleted partial data for stage %s, folder type %s, client ID %s", stringStage, folderType, clientId)

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

func SaveJoinerMetadataToFile[T proto.Message, B proto.Message](dir string, clientId string, stage string, tableType common.FolderType, data *common.JoinerStageData[T, B]) error {
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempMetadataFilePath := filepath.Join(dirPath, TEMPORARY_METADATA_FILE_NAME+JSON_FILE_EXTENSION)

	var metadata MetaData

	switch tableType {
	case common.JOINER_SMALL_FOLDER_TYPE:
		if data.SmallTable == nil {
			return fmt.Errorf("small table data is nil for client %s in stage %s", clientId, stage)
		}
		metadata = MetaData{
			OmegaProcessed:      data.SmallTable.OmegaProcessed,
			Ready:               data.SmallTable.Ready,
			SendedTaskCount:     data.SendedTaskCount,
			SmallTableTaskCount: data.SmallTableTaskCount,
		}
	case common.JOINER_BIG_FOLDER_TYPE:
		if data.BigTable == nil {
			return fmt.Errorf("big table data is nil for client %s in stage %s", clientId, stage)
		}
		metadata = MetaData{
			OmegaProcessed:  data.BigTable.OmegaProcessed,
			Ready:           data.BigTable.Ready,
			SendedTaskCount: data.SendedTaskCount,
			RingRound:       data.RingRound,
		}
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
func SaveJoinerTableToFile(
	dir string,
	clientId string,
	stage interface{},
	tableType common.FolderType,
	data any,
) error {

	stringStage, err := getStageNameFromInterface(stage)
	log.Infof(stringStage)
	if err != nil {
		return fmt.Errorf("error getting stage name: %w", err)
	}

	err = saveJoinerDataToFile(dir, clientId, stringStage, string(tableType), data)
	if err != nil {
		return fmt.Errorf("error saving joiner small table data to file: %w", err)
	}
	return commitPartialDataToFinal(dir, stringStage, tableType, clientId)
}
func saveJoinerDataToFile(
	dir string,
	clientId string,
	stage string,
	tableType string,
	data any,
) error {
	dirPath := filepath.Join(dir, stage, string(tableType), clientId)
	err := os.MkdirAll(dirPath, os.ModePerm)
	if err != nil {
		return fmt.Errorf("failed to create directory: %w", err)
	}

	tempDataFilePath := filepath.Join(dirPath, TEMPORARY_DATA_FILE_NAME+JSON_FILE_EXTENSION)

	err = marshallJoinerAnyTableData(tempDataFilePath, data)
	if err != nil {
		return fmt.Errorf("failed to save data to temporary file: %w", err)
	}

	return nil
}
func marshallJoinerAnyTableData(
	tempDataFilePath string,
	data any,
) error {
	marshaler := protojson.MarshalOptions{
		Indent:          "  ",
		EmitUnpopulated: true,
	}

	switch d := data.(type) {
	case *common.JoinerTableData[common.SmallTableData[*protocol.Zeta_Data_Movie]]:
		return processJoinerTableData(d, tempDataFilePath, marshaler)
	case *common.JoinerTableData[common.SmallTableData[*protocol.Iota_Data_Movie]]:
		return processJoinerTableData(d, tempDataFilePath, marshaler)
	case *common.JoinerTableData[common.BigTableData[*protocol.Zeta_Data_Rating]]:
		return processJoinerBigTableData(d, tempDataFilePath, marshaler)
	case *common.JoinerTableData[common.BigTableData[*protocol.Iota_Data_Actor]]:
		return processJoinerBigTableData(d, tempDataFilePath, marshaler)

	default:
		return fmt.Errorf("unsupported joiner table data type: %T", data)
	}
}

func processJoinerTableData[S proto.Message](
	typedStruct *common.JoinerTableData[common.SmallTableData[S]],
	tempDataFilePath string,
	marshaler protojson.MarshalOptions,
) error {
	log.Infof("Processing joiner small table data to file: %s", tempDataFilePath)

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

	if err := writeToTempFile(tempDataFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}
	return nil
}
func processJoinerBigTableData[B proto.Message](
	typedStruct *common.JoinerTableData[common.BigTableData[B]],
	tempDataFilePath string,
	marshaler protojson.MarshalOptions,
) error {
	log.Infof("Processing joiner big table data to file: %s", tempDataFilePath)

	jsonMap := make(map[uint32]map[string][]json.RawMessage)
	for outerKey, innerMap := range typedStruct.Data {
		jsonMap[outerKey] = make(map[string][]json.RawMessage)
		for innerKey, slice := range innerMap {
			for _, msg := range slice {
				marshaledData, err := marshaler.Marshal(msg)
				if err != nil {
					return fmt.Errorf("error marshaling data to JSON: %w", err)
				}
				jsonMap[outerKey][innerKey] = append(jsonMap[outerKey][innerKey], marshaledData)
			}
		}
	}

	outputData := OutputFile{
		Data:       jsonMap,
		Timestamps: time.Now().UTC().Format(time.RFC3339),
		Status:     VALID_STATUS,
	}

	if err := writeToTempFile(tempDataFilePath, outputData); err != nil {
		return fmt.Errorf("error writing to temp file: %w", err)
	}
	return nil
}

func writeMetadataToTempFile(tempMetadataFilePath string, metadata MetaData) error {
	outputMetadata := MetadataFile{
		Timestamps:          time.Now().UTC().Format(time.RFC3339),
		Status:              VALID_STATUS,
		OmegaProcessed:      metadata.OmegaProcessed,
		RingRound:           metadata.RingRound,
		Ready:               metadata.Ready,
		SendedTaskCount:     metadata.SendedTaskCount,
		SmallTableTaskCount: metadata.SmallTableTaskCount,
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
