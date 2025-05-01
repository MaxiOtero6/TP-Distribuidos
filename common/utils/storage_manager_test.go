package utils

import (
	"encoding/json"
	"os"
	"testing"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/stretchr/testify/assert"
)

const CLIENT_ID = "test_client"
const DIR = "prueba"
const DELTA_STAGE_2 = "delta2"
const ANY_SOURCE = ""

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

	err = SaveDataToFile(tempDir, CLIENT_ID, DELTA_STAGE_2, ANY_SOURCE, originalData)
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

	filePath := tempDir + "/" + CLIENT_ID + "_" + DELTA_STAGE_2 + ".json"
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

func TestLoadDataFromFile(t *testing.T) {
	// Create a temporary directory for the test
	tempDir, err := os.MkdirTemp("", "test_load_data")
	if err != nil {
		t.Fatalf("Failed to create temp directory: %v", err)
	}
	defer os.RemoveAll(tempDir) // Clean up the temp directory after the test

	// Original data to save
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

	// Save the data to a file
	err = SaveDataToFile(tempDir, CLIENT_ID, DELTA_STAGE_2, ANY_SOURCE, originalData)
	assert.NoError(t, err, "Failed to save data to file")

	// Load the data back from the file
	loadedData := make(map[string]*protocol.Delta_2_Data)
	err = LoadDataFromFile(tempDir, CLIENT_ID, DELTA_STAGE_2, ANY_SOURCE, &loadedData)
	assert.NoError(t, err, "Failed to load data from file")

	// Assert that the loaded data matches the original data
	assert.Equal(t, originalData, loadedData, "Loaded data does not match original data")
}
