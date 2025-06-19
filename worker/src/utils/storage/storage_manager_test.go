package storage

import (
	"os"
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/stretchr/testify/assert"
	"google.golang.org/protobuf/proto"
)

const CLIENT_ID = "test_client"
const ANOTHER_CLIENT_ID = "another_test_client"
const DIR = "prueba"

// func assertSerializationWithCustomComparison[T proto.Message](
// 	t *testing.T,
// 	testCases []struct {
// 		name       string
// 		data       *common.PartialData[T]
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual any) bool
// 	},
// ) {
// 	for _, tc := range testCases {
// 		t.Run(tc.name, func(t *testing.T) {
// 			// Crear un directorio temporal para la prueba
// 			tempDir, err := os.MkdirTemp("", "test_serialization")
// 			if err != nil {
// 				t.Fatalf("Failed to create temp directory: %v", err)
// 			}
// 			defer os.RemoveAll(tempDir) // Limpiar el directorio temporal después de la prueba

// 			stringStage, err := getStageNameFromInterface(tc.stage)

// 			// Guardar los datos en un archivo
// 			err = SaveDataToFile(tempDir, tc.clientID, stringStage, tc.source, tc.data)
// 			assert.NoError(t, err, "Failed to save data to file")
// 			err = CommitPartialDataToFinal(tempDir, tc.stage, tc.source, tc.clientID)
// 			assert.NoError(t, err, "Failed to commit partial data to final")

// 			//Leer los datos del archivo
// 			loadedData, err := LoadStageClientInfoFromDisk[any](tempDir, stringStage, tc.source, tc.clientID)
// 			log.Info("fail with error : ", err)
// 			assert.NoError(t, err, "Failed to load data from file")
// 			log.Infof("Loaded data: %v", loadedData)
// 			// Usar la función de comparación personalizada
// 			assert.True(t, tc.comparator(tc.data, loadedData), "Loaded data does not match original data")
// 		})
// 	}
// }

/*
func assertDeserializationOfWorkerInfo[T proto.Message](
	t *testing.T,
	testCases []struct {
		name        string
		partialData *common.MergerPartialResults
		dir         string
		clientID    string
		source      string
		stage       interface{}
		comparator  func(expected, actual map[string]*common.MergerPartialResults) bool
		setter      func(result *common.MergerPartialResults, data *common.PartialData[T])
	},
) {

	// Crear un directorio temporal para todas las pruebas
	tempDir, err := os.MkdirTemp("", "test_deserialization")
	log.Infof("Temporary directory created: %s", tempDir)
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Limpiar el directorio temporal después de la prueba

	// Guardar y commitear todos los datos
	for _, tc := range testCases {
		stringStage, err := getStageNameFromInterface(tc.stage)
		assert.NoError(t, err, "Failed to get stage name")
		err = SaveDataToFile(tempDir, tc.clientID, stringStage, tc.source, tc.data)
		assert.NoError(t, err, "Failed to save data to file")
		err = CommitPartialDataToFinal(tempDir, tc.stage, tc.source, tc.clientID)
		assert.NoError(t, err, "Failed to commit partial data to final")
	}

	// Validar todos los datos al final
	expected := createExpectedResult(testCases)

	actualResult, err := LoadMergerPartialResultsFromDisk(tempDir)
	assert.NoError(t, err, "Failed to load data from file")
	log.Infof("Loaded data: %v", actualResult[CLIENT_ID].Delta3)

	assert.True(t, CompareMergerPartialResultsMap(expected, actualResult), "Loaded merger partial results do not match expected")

}
*/

func loadDataToFile[T proto.Message](t *testing.T, tc struct {
	name     string
	data     *common.PartialData[T]
	dir      string
	clientID string
	source   string
	stage    interface{}
}) {
	stringStage, err := getStageNameFromInterface(tc.stage)
	assert.NoError(t, err, "Failed to get stage name")
	err = SaveDataToFile(tc.dir, tc.clientID, stringStage, tc.source, tc.data)
	assert.NoError(t, err, "Failed to save data to file")
	err = CommitPartialDataToFinal(tc.dir, tc.stage, tc.source, tc.clientID)
	assert.NoError(t, err, "Failed to commit partial data to final")

}

func loadMergerPartialResultsFromDisk(t *testing.T, tempDir string) map[string]*common.MergerPartialResults {
	actualResult, err := LoadMergerPartialResultsFromDisk(tempDir)
	assert.NoError(t, err, "Failed to load data from file")

	return actualResult
}

// func createExpectedResult[T proto.Message](
// 	testCases []struct {
// 		name       string
// 		data       *common.PartialData[T]
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual map[string]*common.MergerPartialResults) bool
// 	}) map[string]*common.MergerPartialResults {

// 	expected := make(map[string]*common.MergerPartialResults)
// 	for _, tc := range testCases {

// 		switch tc.stage.(type) {
// 		case *protocol.Task_Delta_3:

// 			expected[tc.clientID] = &common.MergerPartialResults{
// 				Delta3: tc.data,
// 			}
// 			log.Info("Delta3 data: ", expected[tc.clientID].Delta3)

// 		case *protocol.Task_Eta_3:
// 			expected[tc.clientID] = &common.MergerPartialResults{
// 				Eta3: tc.data.(common.PartialData[*protocol.Eta_3_Data]),
// 			}
// 			log.Info("Eta3 data: ", expected[tc.clientID].Eta3)

// 		case *protocol.Task_Kappa_3:
// 			expected[tc.clientID] = &common.MergerPartialResults{
// 				Kappa3: tc.data.(common.PartialData[*protocol.Kappa_3_Data]),
// 			}
// 			log.Info("Kappa3 data: ", expected[tc.clientID].Kappa3)

// 		case *protocol.Task_Nu_3:
// 			expected[tc.clientID] = &common.MergerPartialResults{
// 				Nu3: tc.data.(common.PartialData[*protocol.Nu_3_Data]),
// 			}
// 			log.Info("Nu3 data: ", expected[tc.clientID].Nu3)
// 		}

// 	}
// 	return expected
// }

func createExpectedResult[T proto.Message](
	testCases []struct {
		name       string
		data       *common.PartialData[T]
		dir        string
		clientID   string
		source     string
		stage      interface{}
		comparator func(expected, actual map[string]*common.MergerPartialResults) bool
		setter     func(result *common.MergerPartialResults, data *common.PartialData[T])
	}) map[string]*common.MergerPartialResults {

	expected := make(map[string]*common.MergerPartialResults)
	for _, tc := range testCases {
		if _, ok := expected[tc.clientID]; !ok {
			expected[tc.clientID] = &common.MergerPartialResults{}
		}
		tc.setter(expected[tc.clientID], tc.data)
	}
	return expected
}

// func TestSerializationAndDeserializationForDeltaStage(t *testing.T) {
// 	testCases := []struct {
// 		name       string
// 		data       *common.PartialData[*protocol.Delta_3_Data]
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 		setter     func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Delta_3_Data])
// 	}{
// 		{
// 			name: "Delta_2_Data",
// 			data: map[string]*protocol.Delta_2_Data{
// 				"country1": {Country: "country1", PartialBudget: 100},
// 				"country2": {Country: "country2", PartialBudget: 200},
// 				"country3": {Country: "country3", PartialBudget: 300},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Delta_2{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Delta_3_Data",
// 			data: common.PartialData[*protocol.Delta_3_Data]{
// 				Data: map[string]*protocol.Delta_3_Data{
// 					"country1": {Country: "country1", PartialBudget: 100},
// 					"country2": {Country: "country2", PartialBudget: 200},
// 					"country3": {Country: "country3", PartialBudget: 300},
// 				},
// 				OmegaProcessed: false,
// 				RingRound:      0,
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Delta_3{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareStruct,
// 			setter: func(result *common.MergerPartialResults, data *common.PartialData[*protocol.Delta_3_Data]) {
// 				result.Delta3 = data
// 			},
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

// func TestSerializationAndDeserializationForNuStage(t *testing.T) {
// 	testCases := []struct {
// 		name       string
// 		data       interface{}
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 	}{
// 		{
// 			name: "Nu_2_Data",
// 			data: map[string]*protocol.Nu_2_Data{
// 				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
// 				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Nu_2{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Nu_3_Data",

// 			// data: map[string]*protocol.Nu_3_Data{
// 			// 	"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
// 			// 	"false": {Sentiment: false, Ratio: 0.5, Count: 200},
// 			// },
// 			data: common.PartialData[*protocol.Nu_3_Data]{
// 				Data: map[string]*protocol.Nu_3_Data{
// 					"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
// 					"false": {Sentiment: false, Ratio: 0.5, Count: 200},
// 				},
// 				OmegaProcessed: false,
// 				RingRound:      0,
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Nu_3{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareStruct,
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

// func TestSerializationAndDeserializationForEtaStage(t *testing.T) {
// 	testCases := []struct {
// 		name       string
// 		data       interface{}
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 	}{
// 		{
// 			name: "Eta_2_Data",
// 			data: map[string]*protocol.Eta_2_Data{
// 				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},
// 				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
// 				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Eta_2{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},

// 		{
// 			name: "Eta_3_Data",
// 			data: common.PartialData[*protocol.Eta_3_Data]{
// 				Data: map[string]*protocol.Eta_3_Data{
// 					"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},
// 					"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
// 					"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
// 				},
// 				OmegaProcessed: false,
// 				RingRound:      0,
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Eta_3{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareStruct,
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

// func TestSerializationAndDeserializationForSmallTableSource(t *testing.T) {

// 	testCases := []struct {
// 		name       string
// 		data       interface{}
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 	}{
// 		{
// 			name: "Iota_Data_movie",
// 			data: map[string]*protocol.Iota_Data_Movie{
// 				"movieId1": {MovieId: "movieId1"},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Iota{},
// 			source:     common.SMALL_TABLE_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Zeta_Data_Movie",
// 			data: map[string]*protocol.Zeta_Data_Movie{
// 				"movie1": {MovieId: "movie1", Title: "Movie One"},
// 				"movie2": {MovieId: "movie2", Title: "Movie Two"},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Zeta{},
// 			source:     common.SMALL_TABLE_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

// func TestSerializationAndDeserializationForAllStages(t *testing.T) {
// 	testCases := []struct {
// 		name       string
// 		data       interface{}
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 	}{
// 		{
// 			name: "Epsilon_Data",
// 			data: map[string]*protocol.Epsilon_Data{
// 				"country1": {ProdCountry: "country1", TotalInvestment: 1000},
// 				"country2": {ProdCountry: "country2", TotalInvestment: 2000},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Epsilon{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Lambda_Data",
// 			data: map[string]*protocol.Lambda_Data{
// 				"actor1": {ActorId: "actor1", ActorName: "Actor One", Participations: 10},
// 				"actor2": {ActorId: "actor2", ActorName: "Actor Two", Participations: 20},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Lambda{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Theta_Data",
// 			data: map[string]*protocol.Theta_Data{
// 				"movie1": {Id: "movie1", Title: "Movie One", AvgRating: 4.5},
// 				"movie2": {Id: "movie2", Title: "Movie Two", AvgRating: 3.8},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Theta{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Kappa_2_Data",
// 			data: map[string]*protocol.Kappa_2_Data{
// 				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 5},
// 				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 15},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Kappa_2{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareProtobufMaps,
// 		},
// 		{
// 			name: "Kappa_3_Data",
// 			data: common.PartialData[*protocol.Kappa_3_Data]{
// 				Data: map[string]*protocol.Kappa_3_Data{
// 					"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
// 					"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
// 				},
// 				OmegaProcessed: false,
// 				RingRound:      0,
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Kappa_3{},
// 			source:     common.ANY_SOURCE,
// 			comparator: compareStruct,
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

// func TestSerializationAndDeserializationForBigTableSource(t *testing.T) {
// 	testCases := []struct {
// 		name       string
// 		data       interface{}
// 		dir        string
// 		clientID   string
// 		stage      interface{}
// 		source     string
// 		comparator func(expected, actual interface{}) bool
// 	}{
// 		{
// 			name: "Iota_Data_Actor",
// 			data: map[string][]*protocol.Iota_Data_Actor{
// 				"movieId1": {
// 					{MovieId: "movieId1", ActorId: "actorId1", ActorName: "Actor One"},
// 					{MovieId: "movieId2", ActorId: "actorId2", ActorName: "Actor Two"},
// 				},
// 				"movieId2": {
// 					{MovieId: "movieId1", ActorId: "actorId3", ActorName: "Actor Three"},
// 					{MovieId: "movieId2", ActorId: "actorId4", ActorName: "Actor Four"},
// 				},
// 			},

// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Iota{},
// 			source:     common.BIG_TABLE_SOURCE,
// 			comparator: CompareProtobufMapsOfArrays,
// 		},
// 		{
// 			name: "Zeta_Data_Actor",
// 			data: map[string][]*protocol.Zeta_Data_Rating{
// 				"movieId1": {
// 					{MovieId: "movieId1", Rating: 4.5},
// 					{MovieId: "movieId1", Rating: 3.5},
// 				},
// 				"movieId2": {
// 					{MovieId: "movieId2", Rating: 5.0},
// 					{MovieId: "movieId2", Rating: 4.0},
// 				},
// 				"movieId3": {
// 					{MovieId: "movieId3", Rating: 4.0},
// 					{MovieId: "movieId3", Rating: 3.0},
// 				},
// 			},
// 			dir:        DIR,
// 			clientID:   CLIENT_ID,
// 			stage:      &protocol.Task_Zeta{},
// 			source:     common.BIG_TABLE_SOURCE,
// 			comparator: CompareProtobufMapsOfArrays,
// 		},
// 	}

// 	assertSerializationWithCustomComparison(t, testCases)
// }

func TestSerializationAndDeserializationOfJoiner(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mergerPartialResults := &common.MergerPartialResults{
		Delta3: &common.PartialData[*protocol.Delta_3_Data]{
			Data: map[string]*protocol.Delta_3_Data{
				"Country1": {Country: "Country1", PartialBudget: 10000},
				"Country2": {Country: "Country2", PartialBudget: 20000},
				"Country3": {Country: "Country3", PartialBudget: 30000},
			},

			OmegaProcessed: false,
			RingRound:      0,
		},
		Eta3: &common.PartialData[*protocol.Eta_3_Data]{
			Data: map[string]*protocol.Eta_3_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},

				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 3.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
		Kappa3: &common.PartialData[*protocol.Kappa_3_Data]{
			Data: map[string]*protocol.Kappa_3_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
		Nu3: &common.PartialData[*protocol.Nu_3_Data]{
			Data: map[string]*protocol.Nu_3_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
	}

	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Delta3",
		data:     mergerPartialResults.Delta3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Delta_3{},
	})

	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Eta3",
		data:     mergerPartialResults.Eta3,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Eta_3{},
	})
	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Kappa3",
		data:     mergerPartialResults.Kappa3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Kappa_3{},
	})
	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_3_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Nu3",
		data:     mergerPartialResults.Nu3,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Nu_3{},
	})
	actualResult := loadMergerPartialResultsFromDisk(t, tempDir)

	expected := make(map[string]*common.MergerPartialResults)
	expected[CLIENT_ID] = &common.MergerPartialResults{
		Delta3: mergerPartialResults.Delta3,
		Kappa3: mergerPartialResults.Kappa3,
		Nu3:    mergerPartialResults.Nu3,
	}
	expected[ANOTHER_CLIENT_ID] = &common.MergerPartialResults{
		Eta3: mergerPartialResults.Eta3,
	}
	assert.True(t, CompareMergerPartialResultsMap(expected, actualResult), "Loaded merger partial results do not match expected")

}

func TestSerializationAndDeserializationOfReducer(t *testing.T) {

	tempDir, err := os.MkdirTemp("", "test_serialization")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir)

	mergerPartialResults := &common.ReducerPartialResults{
		Delta2: &common.PartialData[*protocol.Delta_2_Data]{
			Data: map[string]*protocol.Delta_2_Data{
				"Country1": {Country: "Country1", PartialBudget: 10000},
				"Country2": {Country: "Country2", PartialBudget: 20000},
				"Country3": {Country: "Country3", PartialBudget: 30000},
			},

			OmegaProcessed: false,
			RingRound:      0,
		},
		Eta2: &common.PartialData[*protocol.Eta_2_Data]{
			Data: map[string]*protocol.Eta_2_Data{
				"MovieId1": {MovieId: "MovieId1", Title: "Title1", Rating: 4.5, Count: 100},

				"MovieId2": {MovieId: "MovieId2", Title: "Title2", Rating: 2.5, Count: 200},
				"MovieId3": {MovieId: "MovieId3", Title: "Title3", Rating: 5.0, Count: 300},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
		Kappa2: &common.PartialData[*protocol.Kappa_2_Data]{
			Data: map[string]*protocol.Kappa_2_Data{
				"actor1": {ActorId: "actor1", ActorName: "Actor One", PartialParticipations: 8},
				"actor2": {ActorId: "actor2", ActorName: "Actor Two", PartialParticipations: 12},
			},
			OmegaProcessed: true,
			RingRound:      0,
		},
		Nu2: &common.PartialData[*protocol.Nu_2_Data]{
			Data: map[string]*protocol.Nu_2_Data{
				"true":  {Sentiment: true, Ratio: 0.5, Count: 100},
				"false": {Sentiment: false, Ratio: 0.5, Count: 200},
			},
			OmegaProcessed: false,
			RingRound:      0,
		},
	}

	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Delta_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Delta2",
		data:     mergerPartialResults.Delta2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Delta_2{},
	})

	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Eta_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Eta2",
		data:     mergerPartialResults.Eta2,
		dir:      tempDir,
		clientID: ANOTHER_CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Eta_2{},
	})
	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Kappa_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Kappa2",
		data:     mergerPartialResults.Kappa2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Kappa_2{},
	})
	loadDataToFile(t, struct {
		name     string
		data     *common.PartialData[*protocol.Nu_2_Data]
		dir      string
		clientID string
		source   string
		stage    interface{}
	}{
		name:     "MergerPartialResults_Nu2",
		data:     mergerPartialResults.Nu2,
		dir:      tempDir,
		clientID: CLIENT_ID,
		source:   "",
		stage:    &protocol.Task_Nu_2{},
	})
	actualResult, err := LoadReducerPartialResultsFromDisk(tempDir)
	if err != nil {
		t.Fatalf("Failed to load reducer partial results: %v", err)
	}

	expected := make(map[string]*common.ReducerPartialResults)
	expected[CLIENT_ID] = &common.ReducerPartialResults{
		Delta2: mergerPartialResults.Delta2,
		Kappa2: mergerPartialResults.Kappa2,
		Nu2:    mergerPartialResults.Nu2,
	}
	expected[ANOTHER_CLIENT_ID] = &common.ReducerPartialResults{
		Eta2: mergerPartialResults.Eta2,
	}
	assert.True(t, CompareReducerPartialResultsMap(expected, actualResult), "Loaded reducer partial results do not match expected")

}
