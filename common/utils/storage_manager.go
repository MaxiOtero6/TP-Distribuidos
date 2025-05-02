package utils

import (
	"encoding/json"
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/op/go-logging"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

// Query 1
const ALPHA_STAGE string = "alpha"
const BETA_STAGE string = "beta"

// Query 2
const GAMMA_STAGE string = "gamma"
const DELTA_STAGE_1 string = "delta_1"
const DELTA_STAGE_2 string = "delta_2"
const DELTA_STAGE_3 string = "delta_3"
const EPSILON_STAGE string = "epsilon"

// Query 3
const ZETA_STAGE string = "zeta"
const ETA_STAGE_1 string = "eta_1"
const ETA_STAGE_2 string = "eta_2"
const ETA_STAGE_3 string = "eta_3"
const THETA_STAGE string = "theta"

// Query 4
const IOTA_STAGE string = "iota"
const KAPPA_STAGE_1 string = "kappa_1"
const KAPPA_STAGE_2 string = "kappa_2"
const KAPPA_STAGE_3 string = "kappa_3"
const LAMBDA_STAGE string = "lambda"

// Query 5
const MU_STAGE string = "mu"
const NU_STAGE_1 string = "nu_1"
const NU_STAGE_2 string = "nu_2"
const NU_STAGE_3 string = "nu_3"

// Source const
const BIG_TABLE_SOURCE string = "bigTable"
const SMALL_TABLE_SOURCE string = "smallTable"
const ANY_SOURCE string = ""

var log = logging.MustGetLogger("log")

func decodeDelta2Data(decoder *json.Decoder) (map[string]*protocol.Delta_2_Data, error) {
	var tempArray []struct {
		Country       string `json:"country"`
		PartialBudget string `json:"partialBudget"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Delta_2_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		Country       string `json:"country"`
		PartialBudget string `json:"partialBudget"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Delta_3_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		MovieId string `json:"movieId"`
		Title   string `json:"title"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Zeta_Data_Movie)
	for _, item := range tempArray {
		result[item.MovieId] = &protocol.Zeta_Data_Movie{
			MovieId: item.MovieId,
			Title:   item.Title,
		}
	}
	return result, nil
}

func decodeEpsilonData(decoder *json.Decoder) (map[string]*protocol.Epsilon_Data, error) {
	var tempArray []struct {
		ProdCountry     string `json:"prodCountry"`
		TotalInvestment string `json:"totalInvestment"`
	}

	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Epsilon_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		ActorId        string `json:"actorId"`
		ActorName      string `json:"actorName"`
		Participations string `json:"participations"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Lambda_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		Id        string  `json:"id"`
		Title     string  `json:"title"`
		AvgRating float32 `json:"avgRating"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Theta_Data)
	for _, item := range tempArray {
		result[item.Id] = &protocol.Theta_Data{
			Id:        item.Id,
			Title:     item.Title,
			AvgRating: item.AvgRating,
		}
	}
	return result, nil
}

func decodeNu2Data(decoder *json.Decoder) (map[string]*protocol.Nu_2_Data, error) {
	var tempArray []struct {
		Sentiment bool    `json:"sentiment"`
		Ratio     float32 `json:"ratio"`
		Count     uint32  `json:"count"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Nu_2_Data)
	for _, item := range tempArray {
		result[strconv.FormatBool(item.Sentiment)] = &protocol.Nu_2_Data{
			Sentiment: item.Sentiment,
			Ratio:     item.Ratio,
			Count:     item.Count,
		}
	}
	return result, nil
}

func decodeNu3Data(decoder *json.Decoder) (map[string]*protocol.Nu_3_Data, error) {
	var tempArray []struct {
		Sentiment bool    `json:"sentiment"`
		Ratio     float32 `json:"ratio"`
		Count     uint32  `json:"count"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Nu_3_Data)

	for _, item := range tempArray {
		result[strconv.FormatBool(item.Sentiment)] = &protocol.Nu_3_Data{
			Sentiment: item.Sentiment,
			Ratio:     item.Ratio,
			Count:     item.Count,
		}
	}
	return result, nil
}

func decodeKappa2Data(decoder *json.Decoder) (map[string]*protocol.Kappa_2_Data, error) {

	var tempArray []struct {
		ActorId               string `json:"actorId"`
		ActorName             string `json:"actorName"`
		PartialParticipations string `json:"partialParticipations"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Kappa_2_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		ActorId               string `json:"actorId"`
		ActorName             string `json:"actorName"`
		PartialParticipations string `json:"partialParticipations"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Kappa_3_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		MovieId string  `json:"movieId"`
		Title   string  `json:"title"`
		Rating  float64 `json:"rating"`
		Count   uint32  `json:"count"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Eta_2_Data)
	for _, item := range tempArray {
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
	var tempArray []struct {
		MovieId string  `json:"movieId"`
		Title   string  `json:"title"`
		Rating  float64 `json:"rating"`
		Count   uint32  `json:"count"`
	}

	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Eta_3_Data)
	for _, item := range tempArray {

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
	var tempArray []struct {
		MovieId string `json:"movieId"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}
	result := make(map[string]*protocol.Iota_Data_Movie)
	for _, item := range tempArray {
		result[item.MovieId] = &protocol.Iota_Data_Movie{
			MovieId: item.MovieId,
		}
	}
	return result, nil
}

func decodeIotaDataActor(decoder *json.Decoder) (map[string]*protocol.Iota_Data_Actor, error) {
	var tempArray []struct {
		MovieId   string `json:"movieId"`
		ActorId   string `json:"actorId"`
		ActorName string `json:"actorName"`
	}
	if err := decoder.Decode(&tempArray); err != nil {
		return nil, fmt.Errorf("error decoding JSON array: %w", err)
	}

	result := make(map[string]*protocol.Iota_Data_Actor)
	for _, item := range tempArray {
		result[item.MovieId] = &protocol.Iota_Data_Actor{
			MovieId:   item.MovieId,
			ActorId:   item.ActorId,
			ActorName: item.ActorName,
		}
	}
	return result, nil
}

func LoadDataFromFile(dir string, clientId string, stage string, sourceType string) (interface{}, error) {
	var fileName string
	if sourceType == "" {
		fileName = fmt.Sprintf("%s_%s.json", stage, clientId)
	} else {
		fileName = fmt.Sprintf("%s_%s_%s.json", stage, sourceType, clientId)
	}

	filePath := filepath.Join(dir, fileName)

	file, err := os.Open(filePath)
	if err != nil {
		return nil, fmt.Errorf("error opening file %s: %w", filePath, err)
	}
	defer file.Close()

	decoder := json.NewDecoder(file)

	switch stage {
	case EPSILON_STAGE:
		loadedData := make(map[string]*protocol.Epsilon_Data)
		decodedData, err := decodeEpsilonData(decoder)

		if err != nil {
			return nil, err
		}

		loadedData = decodedData
		return loadedData, nil

	case LAMBDA_STAGE:
		loadedData := make(map[string]*protocol.Lambda_Data)
		decodedData, err := decodeLambdaData(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil
	case THETA_STAGE:
		loadedData := make(map[string]*protocol.Theta_Data)
		decodedData, err := decodeThetaData(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case DELTA_STAGE_2:
		loadedData := make(map[string]*protocol.Delta_2_Data)
		decodedData, err := decodeDelta2Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case DELTA_STAGE_3:
		loadedData := make(map[string]*protocol.Delta_3_Data)
		decodedData, err := decodeDelta3Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case NU_STAGE_2:
		loadedData := make(map[string]*protocol.Nu_2_Data)
		decodedData, err := decodeNu2Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case NU_STAGE_3:
		loadedData := make(map[string]*protocol.Nu_3_Data)
		decodedData, err := decodeNu3Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case KAPPA_STAGE_2:
		loadedData := make(map[string]*protocol.Kappa_2_Data)
		decodedData, err := decodeKappa2Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case KAPPA_STAGE_3:
		loadedData := make(map[string]*protocol.Kappa_3_Data)
		decodedData, err := decodeKappa3Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case ETA_STAGE_2:
		loadedData := make(map[string]*protocol.Eta_2_Data)
		decodedData, err := decodeEta2Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case ETA_STAGE_3:
		loadedData := make(map[string]*protocol.Eta_3_Data)
		decodedData, err := decodeEta3Data(decoder)

		if err != nil {
			return nil, err
		}
		loadedData = decodedData
		return loadedData, nil

	case ZETA_STAGE:
		if sourceType == SMALL_TABLE_SOURCE {
			loadedData := make(map[string]*protocol.Zeta_Data_Movie)
			decodedData, err := decodeZetaDataMovie(decoder)

			if err != nil {
				return nil, err
			}
			loadedData = decodedData
			return loadedData, nil
		} else {
			return nil, fmt.Errorf("unsupported source type: %s", sourceType)
		}

	case IOTA_STAGE:
		if sourceType == SMALL_TABLE_SOURCE {
			loadedData := make(map[string]*protocol.Iota_Data_Movie)
			decodedData, err := decodeIotaDataMovie(decoder)

			if err != nil {
				return nil, err
			}
			loadedData = decodedData
			return loadedData, nil
		} else if sourceType == BIG_TABLE_SOURCE {
			return nil, fmt.Errorf("unsupported source type: %s", sourceType)
		}

	default:
		return nil, fmt.Errorf("unsupported data stage: %s", stage)
	}

	return nil, fmt.Errorf("unsupported data stage: %s", stage)
}

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

func compareProtobufMaps(expected, actual interface{}) bool {
	expectedVal := reflect.ValueOf(expected)
	actualVal := reflect.ValueOf(actual)

	// Verificar que ambos sean mapas
	if expectedVal.Kind() != reflect.Map || actualVal.Kind() != reflect.Map {
		log.Error("The provided values are not maps")
		return false
	}

	// Verificar que tengan el mismo tamaño
	if expectedVal.Len() != actualVal.Len() {
		log.Error("The maps have different lengths")
		return false
	}

	// Iterar sobre las claves del mapa esperado
	for _, key := range expectedVal.MapKeys() {
		expectedValue := expectedVal.MapIndex(key).Interface()
		actualValue := actualVal.MapIndex(key)

		// Verificar que la clave exista en el mapa actual
		if !actualValue.IsValid() {
			log.Errorf("Key %v not found in actual map", key)
			return false
		}

		// Usar proto.Equal para comparar los valores
		if !proto.Equal(expectedValue.(proto.Message), actualValue.Interface().(proto.Message)) {
			log.Errorf("Values for key %v do not match: expected %v, got %v", key, expectedValue, actualValue.Interface())
			return false
		}
	}

	return true
}
