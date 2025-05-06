package storage

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/stretchr/testify/assert"
)

const CLIENT_ID = "test_client"
const DIR = "prueba"

func assertSerializationWithCustomComparison(
	t *testing.T,
	testCases []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	},
) {
	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			// Crear un directorio temporal para la prueba
			tempDir, err := os.MkdirTemp("", "test_serialization")
			if err != nil {
				t.Fatalf("Failed to create temp directory: %v", err)
			}
			defer os.RemoveAll(tempDir) // Limpiar el directorio temporal después de la prueba

			// Guardar los datos en un archivo
			err = SaveDataToFile(tempDir, tc.clientID, tc.stage, tc.source, tc.data)
			assert.NoError(t, err, "Failed to save data to file")

			//Leer los datos del archivo
			loadedData, err := LoadDataFromFile(tempDir, tc.clientID, tc.stage, tc.source)
			assert.NoError(t, err, "Failed to load data from file")

			// Usar la función de comparación personalizada
			assert.True(t, tc.comparator(tc.data, loadedData), "Loaded data does not match original data")
		})
	}
}

func TestDelta2PersistenceWithExistingFunctions(t *testing.T) {

	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "0755")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}

	defer os.RemoveAll(tempDir) // Clean up the temp directory after the test

	originalData := map[string]*protocol.Delta_2_Data{
		"country1": {
			Country:       "country1",
			PartialBudget: 100,
		},
		"country2": {
			Country:       "country2",
			PartialBudget: 200,
		},
		"country3": {
			Country:       "country3",
			PartialBudget: 300,
		},
	}

	err = SaveDataToFile(tempDir, CLIENT_ID, common.DELTA_STAGE_2, common.ANY_SOURCE, originalData)
	assert.NoError(t, err, "Failed to save delta2 data")

	expectedJSON := `[
        {
            "country": "country1",
            "partialBudget": "100"
        },
        {
            "country": "country2",
            "partialBudget": "200"
        },
        {
            "country": "country3",
            "partialBudget": "300"
        }
    ]`

	filePath := tempDir + "/" + common.DELTA_STAGE_2 + "_" + CLIENT_ID + ".json"
	log.Infof("File path: %s", filePath)
	fileContent, err := os.ReadFile(filePath)
	assert.NoError(t, err, "Failed to read the saved file")

	// Comparar el contenido del archivo con el JSON esperado
	//assert.JSONEq(t, expectedJSON, string(fileContent), "File content does not match expected JSON")

	// Deserializar ambos JSON
	var expected, actual []map[string]interface{}
	err = json.Unmarshal([]byte(expectedJSON), &expected)
	assert.NoError(t, err, "Failed to unmarshal expected JSON")

	err = json.Unmarshal(fileContent, &actual)
	assert.NoError(t, err, "Failed to unmarshal actual JSON")

	// Validar que ambos contengan los mismos elementos, sin importar el orden
	assert.ElementsMatch(t, expected, actual, "File content does not match expected JSON")

}

func TestSerializationAndDeserializationForDeltaStage(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Delta_2_Data",
			data: map[string]*protocol.Delta_2_Data{
				"country1": {Country: "country1", PartialBudget: 100},
				"country2": {Country: "country2", PartialBudget: 200},
				"country3": {Country: "country3", PartialBudget: 300},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.DELTA_STAGE_2,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Delta_3_Data",
			data: map[string]*protocol.Delta_3_Data{
				"country1": {Country: "country1", PartialBudget: 100},
				"country2": {Country: "country2", PartialBudget: 200},
				"country3": {Country: "country3", PartialBudget: 300},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.DELTA_STAGE_3,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}

func TestSerializationAndDeserializationForNuStage(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Nu_2_Data",
			data: map[string]*protocol.Nu_2_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.NU_STAGE_2,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Nu_3_Data",
			data: map[string]*protocol.Nu_3_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.NU_STAGE_3,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}

func TestSerializationAndDeserializationForEtaStage(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Eta_2_Data",
			data: map[string]*protocol.Eta_2_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},
				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.ETA_STAGE_2,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},

		{
			name: "Eta_3_Data",
			data: map[string]*protocol.Eta_3_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},
				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.ETA_STAGE_3,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}

func TestSerializationAndDeserializationForSmallTableSource(t *testing.T) {

	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Iota_Data_movie",
			data: map[string]*protocol.Iota_Data_Movie{
				"movieId1": {MovieId: "movieId1"},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.IOTA_STAGE,
			source:     common.SMALL_TABLE_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Zeta_Data_Movie",
			data: map[string]*protocol.Zeta_Data_Movie{
				"movie1": {MovieId: "movie1", Title: "Movie One"},
				"movie2": {MovieId: "movie2", Title: "Movie Two"},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.ZETA_STAGE,
			source:     common.SMALL_TABLE_SOURCE,
			comparator: compareProtobufMaps,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}

func TestSerializationAndDeserializationForAllStages(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Epsilon_Data",
			data: map[string]*protocol.Epsilon_Data{
				"country1": {ProdCountry: "country1", TotalInvestment: 1000},
				"country2": {ProdCountry: "country2", TotalInvestment: 2000},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.EPSILON_STAGE,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Lambda_Data",
			data: map[string]*protocol.Lambda_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", Participations: 10},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", Participations: 20},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.LAMBDA_STAGE,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Theta_Data",
			data: map[string]*protocol.Theta_Data{
				"movie1": {Id: "movie1", Title: "Movie One", AvgRating: 4.5},
				"movie2": {Id: "movie2", Title: "Movie Two", AvgRating: 3.8},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.THETA_STAGE,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Kappa_2_Data",
			data: map[string]*protocol.Kappa_2_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 5},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 15},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.KAPPA_STAGE_2,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
		{
			name: "Kappa_3_Data",
			data: map[string]*protocol.Kappa_3_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.KAPPA_STAGE_3,
			source:     common.ANY_SOURCE,
			comparator: compareProtobufMaps,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}

func TestSerializationAndDeserializationForBigTableSource(t *testing.T) {
	testCases := []struct {
		name       string
		data       interface{}
		dir        string
		clientID   string
		stage      string
		source     string
		comparator func(expected, actual interface{}) bool
	}{
		{
			name: "Iota_Data_Actor",
			data: map[string][]*protocol.Iota_Data_Actor{
				"movieId1": {
					{MovieId: "movieId1", ActorId: "actorId1", ActorName: "Actor One"},
					{MovieId: "movieId2", ActorId: "actorId2", ActorName: "Actor Two"},
				},
				"movieId2": {
					{MovieId: "movieId1", ActorId: "actorId3", ActorName: "Actor Three"},
					{MovieId: "movieId2", ActorId: "actorId4", ActorName: "Actor Four"},
				},
			},

			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.IOTA_STAGE,
			source:     common.BIG_TABLE_SOURCE,
			comparator: CompareProtobufMapsOfArrays,
		},
		{
			name: "Zeta_Data_Actor",
			data: map[string][]*protocol.Zeta_Data_Rating{
				"movieId1": {
					{MovieId: "movieId1", Rating: 4.5},
					{MovieId: "movieId1", Rating: 3.5},
				},
				"movieId2": {
					{MovieId: "movieId2", Rating: 5.0},
					{MovieId: "movieId2", Rating: 4.0},
				},
				"movieId3": {
					{MovieId: "movieId3", Rating: 4.0},
					{MovieId: "movieId3", Rating: 3.0},
				},
			},
			dir:        DIR,
			clientID:   CLIENT_ID,
			stage:      common.ZETA_STAGE,
			source:     common.BIG_TABLE_SOURCE,
			comparator: CompareProtobufMapsOfArrays,
		},
	}

	assertSerializationWithCustomComparison(t, testCases)
}
