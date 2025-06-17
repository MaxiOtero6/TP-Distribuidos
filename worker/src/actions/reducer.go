package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
)

const REDUCER_STAGES_COUNT uint = 4
const REDUCER_FILE_TYPE string = ""

type ReducerPartialResults struct {
	toDeleteCount uint
	delta2        *PartialData[protocol.Delta_2_Data]
	eta2          *PartialData[protocol.Eta_2_Data]
	kappa2        *PartialData[protocol.Kappa_2_Data]
	nu2           *PartialData[protocol.Nu_2_Data]
}

// Reducer is a struct that implements the Action interface.
type Reducer struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*ReducerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	eofHandler     *eof.StatefulEofHandler
}

func (r *Reducer) makePartialResults(clientId string) {
	if _, ok := r.partialResults[clientId]; ok {
		return
	}

	r.partialResults[clientId] = &ReducerPartialResults{
		toDeleteCount: REDUCER_STAGES_COUNT,
		delta2:        NewPartialData[protocol.Delta_2_Data](),
		eta2:          NewPartialData[protocol.Eta_2_Data](),
		kappa2:        NewPartialData[protocol.Kappa_2_Data](),
		nu2:           NewPartialData[protocol.Nu_2_Data](),
	}
}

// NewReduce creates a new Reduce instance.
// It initializes the worker count and returns a pointer to the Reduce struct.
func NewReducer(infraConfig *model.InfraConfig) *Reducer {
	eofHandler := eof.NewStatefulEofHandler(
		model.ReducerAction,
		infraConfig,
		reducerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	reducer := &Reducer{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*ReducerPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		eofHandler:     eofHandler,
	}

	go storage.StartCleanupRoutine(infraConfig.GetDirectory())

	return reducer
}

func (r *Reducer) delta2Stage(data []*protocol.Delta_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := r.partialResults[clientId].delta2

	aggregationFunc := func(existing *protocol.Delta_2_Data, input *protocol.Delta_2_Data) {
		existing.PartialBudget += input.GetPartialBudget()
	}

	identifierFunc := func(input *protocol.Delta_2_Data) string {
		return input.GetCountry()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (r *Reducer) eta2Stage(data []*protocol.Eta_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := r.partialResults[clientId].eta2

	aggregationFunc := func(existing *protocol.Eta_2_Data, input *protocol.Eta_2_Data) {
		existing.Rating += input.GetRating()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Eta_2_Data) string {
		return input.GetMovieId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (r *Reducer) kappa2Stage(data []*protocol.Kappa_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := r.partialResults[clientId].kappa2

	aggregationFunc := func(existing *protocol.Kappa_2_Data, input *protocol.Kappa_2_Data) {
		existing.PartialParticipations += input.GetPartialParticipations()
	}

	identifierFunc := func(input *protocol.Kappa_2_Data) string {
		return input.GetActorId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (r *Reducer) nu2Stage(data []*protocol.Nu_2_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := r.partialResults[clientId].nu2

	aggregationFunc := func(existing *protocol.Nu_2_Data, input *protocol.Nu_2_Data) {
		existing.Ratio += input.GetRatio()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Nu_2_Data) string {
		return strconv.FormatBool(input.GetSentiment())
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (r *Reducer) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return reducerNextStageData(
		stage,
		clientId,
		r.infraConfig,
		r.itemHashFunc,
	)
}

func reducerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.DELTA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.DELTA_STAGE_3,
				Exchange:    infraConfig.GetMergeExchange(),
				WorkerCount: infraConfig.GetMergeCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetMergeCount(), clientId+common.DELTA_STAGE_3),
			},
		}, nil
	case common.ETA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_3,
				Exchange:    infraConfig.GetMergeExchange(),
				WorkerCount: infraConfig.GetMergeCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetMergeCount(), clientId+common.ETA_STAGE_3),
			},
		}, nil
	case common.KAPPA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_3,
				Exchange:    infraConfig.GetMergeExchange(),
				WorkerCount: infraConfig.GetMergeCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetMergeCount(), clientId+common.KAPPA_STAGE_3),
			},
		}, nil
	case common.NU_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_3,
				Exchange:    infraConfig.GetMergeExchange(),
				WorkerCount: infraConfig.GetMergeCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetMergeCount(), clientId+common.NU_STAGE_3),
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetEofExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  utils.GetNextNodeId(infraConfig.GetNodeId(), infraConfig.GetReduceCount()),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *Reducer) delta2Results(tasks common.Tasks, clientId string) {
	partialDataMap := r.partialResults[clientId].delta2.data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(_ int, data *protocol.Delta_2_Data) *protocol.Delta_3_Data {
		return &protocol.Delta_3_Data{
			Country:       data.GetCountry(),
			PartialBudget: data.GetPartialBudget(),
		}
	})

	identifierFunc := func(data *protocol.Delta_3_Data) string {
		return data.GetCountry()
	}

	taskDataCreator := func(stage string, data []*protocol.Delta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Delta_3{
				Delta_3: &protocol.Delta_3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := r.nextStageData(common.DELTA_STAGE_2, clientId)
	taskNumber, _ := strconv.Atoi(r.infraConfig.GetNodeId())

	AddResults(
		tasks,
		results,
		nextStageData[0],
		clientId,
		taskNumber,
		0,
		true,
		r.itemHashFunc,
		identifierFunc,
		taskDataCreator,
	)
}

func (r *Reducer) eta2Results(tasks common.Tasks, clientId string) {
	partialDataMap := r.partialResults[clientId].eta2.data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(_ int, data *protocol.Eta_2_Data) *protocol.Eta_3_Data {
		return &protocol.Eta_3_Data{
			MovieId: data.GetMovieId(),
			Title:   data.GetTitle(),
			Rating:  data.GetRating(),
			Count:   data.GetCount(),
		}
	})

	identifierFunc := func(data *protocol.Eta_3_Data) string {
		return data.GetMovieId()
	}

	taskDataCreator := func(stage string, data []*protocol.Eta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_3{
				Eta_3: &protocol.Eta_3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := r.nextStageData(common.ETA_STAGE_2, clientId)
	taskNumber, _ := strconv.Atoi(r.infraConfig.GetNodeId())

	AddResults(
		tasks,
		results,
		nextStageData[0],
		clientId,
		taskNumber,
		0,
		true,
		r.itemHashFunc,
		identifierFunc,
		taskDataCreator,
	)
}

func (r *Reducer) kappa2Results(tasks common.Tasks, clientId string) {
	partialDataMap := r.partialResults[clientId].kappa2.data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(_ int, data *protocol.Kappa_2_Data) *protocol.Kappa_3_Data {
		return &protocol.Kappa_3_Data{
			ActorId:               data.GetActorId(),
			ActorName:             data.GetActorName(),
			PartialParticipations: data.GetPartialParticipations(),
		}
	})

	identifierFunc := func(data *protocol.Kappa_3_Data) string {
		return data.GetActorId()
	}

	taskDataCreator := func(stage string, data []*protocol.Kappa_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_3{
				Kappa_3: &protocol.Kappa_3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := r.nextStageData(common.KAPPA_STAGE_2, clientId)
	taskNumber, _ := strconv.Atoi(r.infraConfig.GetNodeId())

	AddResults(
		tasks,
		results,
		nextStageData[0],
		clientId,
		taskNumber,
		0,
		true,
		r.itemHashFunc,
		identifierFunc,
		taskDataCreator,
	)
}

func (r *Reducer) nu2Results(tasks common.Tasks, clientId string) {
	partialDataMap := r.partialResults[clientId].nu2.data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(_ int, data *protocol.Nu_2_Data) *protocol.Nu_3_Data {
		return &protocol.Nu_3_Data{
			Sentiment: data.GetSentiment(),
			Ratio:     data.GetRatio(),
			Count:     data.GetCount(),
		}
	})

	identifierFunc := func(data *protocol.Nu_3_Data) string {
		return strconv.FormatBool(data.GetSentiment())
	}

	taskDataCreator := func(stage string, data []*protocol.Nu_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_3{
				Nu_3: &protocol.Nu_3{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := r.nextStageData(common.NU_STAGE_2, clientId)
	taskNumber, _ := strconv.Atoi(r.infraConfig.GetNodeId())

	AddResults(
		tasks,
		results,
		nextStageData[0],
		clientId,
		taskNumber,
		0,
		true,
		r.itemHashFunc,
		identifierFunc,
		taskDataCreator,
	)
}

func (r *Reducer) AddResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.DELTA_STAGE_2:
		r.delta2Results(tasks, clientId)
	case common.ETA_STAGE_2:
		r.eta2Results(tasks, clientId)
	case common.KAPPA_STAGE_2:
		r.kappa2Results(tasks, clientId)
	case common.NU_STAGE_2:
		r.nu2Results(tasks, clientId)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (r *Reducer) getTaskIdentifiers(clientId string, stage string) ([]common.TaskFragmentIdentifier, error) {
	partialResults := r.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_2:
		return utils.MapKeys(partialResults.delta2.taskFragments), nil
	case common.ETA_STAGE_2:
		return utils.MapKeys(partialResults.eta2.taskFragments), nil
	case common.KAPPA_STAGE_2:
		return utils.MapKeys(partialResults.kappa2.taskFragments), nil
	case common.NU_STAGE_2:
		return utils.MapKeys(partialResults.nu2.taskFragments), nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *Reducer) participatesInResults(clientId string, stage string) bool {
	partialResults, ok := r.partialResults[clientId]
	if !ok {
		return false
	}

	switch stage {
	case common.DELTA_STAGE_2:
		return len(partialResults.delta2.data) > 0
	case common.ETA_STAGE_2:
		return len(partialResults.eta2.data) > 0
	case common.KAPPA_STAGE_2:
		return len(partialResults.kappa2.data) > 0
	case common.NU_STAGE_2:
		return len(partialResults.nu2.data) > 0
	default:
		log.Errorf("Invalid stage: %s", stage)
		return false
	}
}

func (r *Reducer) getOmegaProcessed(clientId string, stage string) (bool, error) {
	partialResults := r.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_2:
		return partialResults.delta2.omegaProcessed, nil
	case common.ETA_STAGE_2:
		return partialResults.eta2.omegaProcessed, nil
	case common.KAPPA_STAGE_2:
		return partialResults.kappa2.omegaProcessed, nil
	case common.NU_STAGE_2:
		return partialResults.nu2.omegaProcessed, nil
	default:
		return false, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *Reducer) getRingRound(clientId string, stage string) (uint32, error) {
	partialResults := r.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_2:
		return partialResults.delta2.ringRound, nil
	case common.ETA_STAGE_2:
		return partialResults.eta2.ringRound, nil
	case common.KAPPA_STAGE_2:
		return partialResults.kappa2.ringRound, nil
	case common.NU_STAGE_2:
		return partialResults.nu2.ringRound, nil
	default:
		return 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

// Actualizar funciones para usar las constantes de etapas del paquete common
func (r *Reducer) updateOmegaProcessed(clientId string, stage string) {
	switch stage {
	case common.DELTA_STAGE_2:
		r.partialResults[clientId].delta2.omegaProcessed = true
	case common.ETA_STAGE_2:
		r.partialResults[clientId].eta2.omegaProcessed = true
	case common.KAPPA_STAGE_2:
		r.partialResults[clientId].kappa2.omegaProcessed = true
	case common.NU_STAGE_2:
		r.partialResults[clientId].nu2.omegaProcessed = true
	}
}

func (r *Reducer) updateRingRound(clientId string, stage string, round uint32) {
	switch stage {
	case common.DELTA_STAGE_2:
		r.partialResults[clientId].delta2.ringRound = round
	case common.ETA_STAGE_2:
		r.partialResults[clientId].eta2.ringRound = round
	case common.KAPPA_STAGE_2:
		r.partialResults[clientId].kappa2.ringRound = round
	case common.NU_STAGE_2:
		r.partialResults[clientId].nu2.ringRound = round
	}
}

func (r *Reducer) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	omegaReady, err := r.getOmegaProcessed(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get omega ready for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if omegaReady {
		log.Debugf("Omega EOF for stage %s has already been processed for client %s", data.GetStage(), clientId)
		return tasks
	}

	r.updateOmegaProcessed(clientId, data.GetStage())

	r.eofHandler.HandleOmegaEOF(tasks, data, clientId)

	return tasks
}

func (r *Reducer) ringEOFStage(data *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	taskIdentifiers, err := r.getTaskIdentifiers(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", data.GetStage(), err)
		return tasks
	}

	ringRound, err := r.getRingRound(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get ring round for stage %s: %s", data.GetStage(), err)
		return tasks
	}
	if ringRound >= data.GetRoundNumber() {
		log.Debugf("Ring EOF for stage %s and client %s has already been processed for round %d", data.GetStage(), clientId, ringRound)
		return tasks
	}

	r.updateRingRound(clientId, data.GetStage(), data.GetRoundNumber())

	participatesInResults := r.participatesInResults(clientId, data.GetStage())
	ready := r.eofHandler.HandleRingEOF(tasks, data, clientId, taskIdentifiers, participatesInResults)

	if ready {
		err = r.AddResultsToNextStage(tasks, data.GetStage(), clientId)

		if err != nil {
			log.Errorf("Failed to add results to next stage for stage %s: %s", data.GetStage(), err)
			return nil
		}
	}

	return tasks
}

func (r *Reducer) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	r.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Delta_2:
		data := v.Delta_2.GetData()
		return r.delta2Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Eta_2:
		data := v.Eta_2.GetData()
		return r.eta2Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Kappa_2:
		data := v.Kappa_2.GetData()
		return r.kappa2Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Nu_2:
		data := v.Nu_2.GetData()
		return r.nu2Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return r.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return r.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

// func (r *Reducer) deleteStage(clientId string, stage string) error {

// 	log.Debugf("Deleting stage %s for client %s", stage, clientId)

// 	if anStage, ok := r.partialResults[clientId]; ok {
// 		switch stage {
// 		case common.DELTA_STAGE_2:
// 			anStage.delta2 = nil
// 		case common.ETA_STAGE_2:
// 			anStage.eta2 = nil
// 		case common.KAPPA_STAGE_2:
// 			anStage.kappa2 = nil
// 		case common.NU_STAGE_2:
// 			anStage.nu2 = nil
// 		default:
// 			log.Errorf("Invalid stage: %s", stage)
// 			return fmt.Errorf("invalid stage: %s", stage)

// 		}
// 	}
// 	return nil
// }
