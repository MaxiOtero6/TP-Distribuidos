package eof_test

import (
	"fmt"
	"math/rand"
	"testing"

	"github.com/stretchr/testify/assert"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
)

func TestMergeRingEOFFragments(t *testing.T) {
	ringEOF := &protocol.RingEOF{
		StageFragmentes: []*protocol.StageFragment{
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 1},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{},
				End:          &protocol.FragmentIdentifier{TaskNumber: 98, TaskFragmentNumber: 1},
				LastFragment: true,
			},
			{
				CreatorId:    "c27939b0-4bdd-4a18-a08e-9978cf93a96f",
				Start:        &protocol.FragmentIdentifier{TaskNumber: 2},
				End:          &protocol.FragmentIdentifier{TaskNumber: 100},
				LastFragment: true,
			},
		},
	}

	i := model.NewInfraConfig(
		"0",
		&model.WorkerClusterConfig{
			FilterCount: 1,
			MapCount:    1,
			ReduceCount: 3,
		},
		&model.RabbitConfig{
			FilterExchange: "filterExchange",
			JoinExchange:   "joinExchange",
			ResultExchange: "resultExchange",
			MapExchange:    "mapExchange",
			ReduceExchange: "reduceExchange",
			BroadcastID:    "",
		},
		"",
	)

	nextStageFunc := func(stage string, clientId string, _ *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
		return []common.NextStageData{
			{
				Stage:       stage,
				Exchange:    "",
				WorkerCount: 1,
				RoutingKey:  "",
			},
		}, nil
	}

	hashFunc := utils.GetWorkerIdFromHash

	eofHandler := eof.NewStatefulEofHandler(model.ReducerAction, i, nextStageFunc, hashFunc)
	eofHandler.MergeStageFragmentsPublic(ringEOF, []model.TaskFragmentIdentifier{})

	assert.Equal(t, 1, len(ringEOF.StageFragmentes), "Expected only one merged fragment")

	// Aquí se pueden agregar los asserts para verificar el resultado después de aplicar la lógica de merge
}

func TestFullTest(t *testing.T) {

	i := model.NewInfraConfig(
		"0",
		&model.WorkerClusterConfig{
			FilterCount: 1,
			MapCount:    1,
			ReduceCount: 3,
		},
		&model.RabbitConfig{
			FilterExchange: "filterExchange",
			JoinExchange:   "joinExchange",
			ResultExchange: "resultExchange",
			MapExchange:    "mapExchange",
			ReduceExchange: "reduceExchange",
			BroadcastID:    "",
		},
		"",
	)

	nextStageFunc := func(stage string, clientId string, _ *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
		return []common.NextStageData{
			{
				Stage:       stage,
				Exchange:    "",
				WorkerCount: 1,
				RoutingKey:  "",
			},
		}, nil
	}

	hashFunc := utils.GetWorkerIdFromHash

	eofHandler := eof.NewStatefulEofHandler(model.ReducerAction, i, nextStageFunc, hashFunc)

	// random de 5 a 10 workers
	CANT_WORKERS := rand.Intn(5) + 5
	CANT_TASKS := rand.Intn(300) + 100

	ringEOF := &protocol.RingEOF{
		Stage: "test_stage",
	}

	tasks_identifiers := make([]model.TaskFragmentIdentifier, 0)

	for i := range CANT_TASKS {
		fragments := rand.Intn(CANT_WORKERS) + 1

		for j := range fragments {
			tasks_identifiers = append(tasks_identifiers, model.TaskFragmentIdentifier{
				CreatorId:          "pepe",
				TaskNumber:         uint32(i),
				TaskFragmentNumber: uint32(j),
				LastFragment:       j == fragments-1,
			})
		}
	}

	SHUFFLE_TIMES := 1000

	// shuffle tasks_identifiers
	for range SHUFFLE_TIMES {
		for i := range tasks_identifiers {
			j := rand.Intn(i + 1)
			tasks_identifiers[i], tasks_identifiers[j] = tasks_identifiers[j], tasks_identifiers[i]
		}
	}

	for i := range tasks_identifiers {
		tempTaskIdentifiers := tasks_identifiers[0 : i+1]
		eofHandler.MergeStageFragmentsPublic(ringEOF, tempTaskIdentifiers)
	}

	assert.Len(t, ringEOF.StageFragmentes, 1, "Expected only one merged fragment after processing all task identifiers")
	fmt.Println("Final merged fragments:", ringEOF.StageFragmentes)

	// This test is a placeholder to ensure the test suite runs without errors.
	// It can be expanded with more specific tests as needed.
	// assert.True(t, true, "This is a placeholder test to ensure the test suite runs without errors.")
}
