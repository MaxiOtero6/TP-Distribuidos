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

const MERGER_STAGES_COUNT uint = 4

type MergerPartialResults struct {
	toDeleteCount uint
	delta3        PartialData[protocol.Delta_3_Data]
	eta3          PartialData[protocol.Eta_3_Data]
	kappa3        PartialData[protocol.Kappa_3_Data]
	nu3Data       PartialData[protocol.Nu_3_Data]
}

// Merger is a struct that implements the Action interface.
type Merger struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*MergerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatefulEofHandler
}

func (m *Merger) makePartialResults(clientId string) {
	if _, ok := m.partialResults[clientId]; ok {
		return
	}

	m.partialResults[clientId] = &MergerPartialResults{
		delta3: PartialData[protocol.Delta_3_Data]{
			data:  make(map[string]*protocol.Delta_3_Data),
			ready: false,
		},
		eta3: PartialData[protocol.Eta_3_Data]{
			data:  make(map[string]*protocol.Eta_3_Data),
			ready: false,
		},
		kappa3: PartialData[protocol.Kappa_3_Data]{
			data:  make(map[string]*protocol.Kappa_3_Data),
			ready: false,
		},
		nu3Data: PartialData[protocol.Nu_3_Data]{
			data:  make(map[string]*protocol.Nu_3_Data),
			ready: false,
		},
	}
}

// NewMerger creates a new Merger instance.
// It initializes the worker count and returns a pointer to the Merger struct.
func NewMerger(infraConfig *model.InfraConfig) *Merger {
	eofHandler := eof.NewStatefulEofHandler(
		model.MergerAction,
		infraConfig,
		mergerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	merger := &Merger{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*MergerPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
	}
	go storage.StartCleanupRoutine(infraConfig.GetDirectory())
	return merger
}

func (m *Merger) delta3Stage(data []*protocol.Delta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := &m.partialResults[clientId].delta3

	aggregationFunc := func(existing *protocol.Delta_3_Data, input *protocol.Delta_3_Data) {
		existing.PartialBudget += input.GetPartialBudget()
	}

	identifierFunc := func(input *protocol.Delta_3_Data) string {
		return input.GetCountry()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (m *Merger) eta3Stage(data []*protocol.Eta_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := &m.partialResults[clientId].eta3

	aggregationFunc := func(existing *protocol.Eta_3_Data, input *protocol.Eta_3_Data) {
		existing.Rating += input.GetRating()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Eta_3_Data) string {
		return input.GetMovieId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (m *Merger) kappa3Stage(data []*protocol.Kappa_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := &m.partialResults[clientId].kappa3

	aggregationFunc := func(existing *protocol.Kappa_3_Data, input *protocol.Kappa_3_Data) {
		existing.PartialParticipations += input.GetPartialParticipations()
	}

	identifierFunc := func(input *protocol.Kappa_3_Data) string {
		return input.GetActorId()
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func (m *Merger) nu3Stage(data []*protocol.Nu_3_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) (tasks common.Tasks) {
	partialData := &m.partialResults[clientId].nu3Data

	aggregationFunc := func(existing *protocol.Nu_3_Data, input *protocol.Nu_3_Data) {
		existing.Ratio += input.GetRatio()
		existing.Count += input.GetCount()
	}

	identifierFunc := func(input *protocol.Nu_3_Data) string {
		return strconv.FormatBool(input.GetSentiment())
	}

	ProcessStage(partialData, data, clientId, taskIdentifier, aggregationFunc, identifierFunc)
	return nil
}

func mergerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.DELTA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.EPSILON_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.EPSILON_STAGE),
			},
		}, nil
	case common.ETA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.THETA_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.THETA_STAGE),
			},
		}, nil
	case common.KAPPA_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.LAMBDA_STAGE,
				Exchange:    infraConfig.GetTopExchange(),
				WorkerCount: infraConfig.GetTopCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetTopCount(), clientId+common.LAMBDA_STAGE),
			},
		}, nil
	case common.NU_STAGE_3:
		return []common.NextStageData{
			{
				Stage:       common.RESULT_STAGE,
				Exchange:    infraConfig.GetResultExchange(),
				WorkerCount: 1,
				RoutingKey:  clientId,
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetEofExchange(),
				WorkerCount: infraConfig.GetReduceCount(),
				RoutingKey:  utils.GetNextNodeId(infraConfig.GetNodeId(), infraConfig.GetMergeCount()),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return mergerNextStageData(stage, clientId, m.infraConfig, m.itemHashFunc)
}

func (m *Merger) delta3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].delta3.data
	partialData := utils.MapValues(partialDataMap)
	results := utils.MapSlice(partialData, func(data *protocol.Delta_3_Data) *protocol.Epsilon_Data {
		return &protocol.Epsilon_Data{
			ProdCountry:     data.GetCountry(),
			TotalInvestment: data.GetPartialBudget(),
		}
	})

	identifierFunc := func(data *protocol.Epsilon_Data) string {
		return data.GetProdCountry()
	}

	taskDataCreator := func(stage string, data []*protocol.Epsilon_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.DELTA_STAGE_3, clientId)
	hashFunc := func(workersCount int, item string) string {
		return m.itemHashFunc(workersCount, clientId+common.EPSILON_STAGE)
	}

	AddResults(tasks, results, nextStageData[0], clientId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (m *Merger) eta3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].eta3.data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(data *protocol.Eta_3_Data) *protocol.Theta_Data {
		return &protocol.Theta_Data{
			Id:        data.GetMovieId(),
			Title:     data.GetTitle(),
			AvgRating: float32(data.GetRating()) / float32(data.GetCount()),
		}
	})

	identifierFunc := func(data *protocol.Theta_Data) string {
		return data.GetId()
	}

	taskDataCreator := func(stage string, data []*protocol.Theta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.ETA_STAGE_3, clientId)
	hashFunc := func(workersCount int, item string) string {
		return m.itemHashFunc(workersCount, clientId+common.THETA_STAGE)
	}

	AddResults(tasks, results, nextStageData[0], clientId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (m *Merger) kappa3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].kappa3.data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(data *protocol.Kappa_3_Data) *protocol.Lambda_Data {
		return &protocol.Lambda_Data{
			ActorId:        data.GetActorId(),
			ActorName:      data.GetActorName(),
			Participations: data.GetPartialParticipations(),
		}
	})

	identifierFunc := func(data *protocol.Lambda_Data) string {
		return data.GetActorId()
	}

	taskDataCreator := func(stage string, data []*protocol.Lambda_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.KAPPA_STAGE_3, clientId)
	hashFunc := func(workersCount int, item string) string {
		return m.itemHashFunc(workersCount, item)
	}

	AddResults(tasks, results, nextStageData[0], clientId, 0, hashFunc, identifierFunc, taskDataCreator)
}

func (m *Merger) nu3Results(tasks common.Tasks, clientId string) {
	partialDataMap := m.partialResults[clientId].nu3Data.data
	partialData := utils.MapValues(partialDataMap)

	results := utils.MapSlice(partialData, func(data *protocol.Nu_3_Data) *protocol.Result5_Data {
		return &protocol.Result5_Data{
			Sentiment: data.GetSentiment(),
			Ratio:     data.GetRatio() / float32(data.GetCount()),
		}
	})

	identifierFunc := func(data *protocol.Result5_Data) string {
		return strconv.FormatBool(data.GetSentiment())
	}

	taskDataCreator := func(stage string, data []*protocol.Result5_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	nextStageData, _ := m.getNextStageData(common.NU_STAGE_3, clientId)
	hashFunc := func(workersCount int, item string) string {
		return clientId
	}

	AddResults(tasks, results, nextStageData[0], clientId, 0, hashFunc, identifierFunc, taskDataCreator)
}

// Adding EOF handler to Merger
func (m *Merger) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	omegaReady, err := m.getOmegaProcessed(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get omega ready for stage %s: %s", data.GetStage(), err)
		return nil
	}
	if omegaReady {
		log.Debugf("Omega EOF for stage %s has already been processed for client %s", data.GetStage(), clientId)
		return nil
	}

	taskIdentifiers, err := m.getTaskIdentifiers(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", data.GetStage(), err)
		return nil
	}

	return m.eofHandler.HandleOmegaEOF(data, clientId, taskIdentifiers)
}

func (m *Merger) ringEOFStage(data *protocol.RingEOF, clientId string) common.Tasks {
	taskIdentifiers, err := m.getTaskIdentifiers(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get task identifiers for stage %s: %s", data.GetStage(), err)
		return nil
	}

	ringRound, err := m.getRingRound(clientId, data.GetStage())
	if err != nil {
		log.Errorf("Failed to get ring round for stage %s: %s", data.GetStage(), err)
		return nil
	}
	if ringRound > data.GetRoundNumber() {
		log.Debugf("Ring EOF for stage %s and client %s has already been processed for round %d", data.GetStage(), clientId, ringRound)
		return nil
	}

	tasks, ready := m.eofHandler.HandleRingEOF(data, clientId, taskIdentifiers)

	if ready {
		err = m.addResultsToNextStage(tasks, data.GetStage(), clientId)
		if err != nil {
			log.Errorf("Failed to add results to next stage for stage %s: %s", data.GetStage(), err)
			return nil
		}
	}

	return tasks
}

// Adjusting Execute to pass taskIdentifier
func (m *Merger) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	m.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Delta_3:
		data := v.Delta_3.GetData()
		return m.delta3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Eta_3:
		data := v.Eta_3.GetData()
		return m.eta3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Kappa_3:
		data := v.Kappa_3.GetData()
		return m.kappa3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Nu_3:
		data := v.Nu_3.GetData()
		return m.nu3Stage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return m.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Merger) deleteStage(clientId string, stage string) error {

	log.Debugf("Deleting stage %s for client %s", stage, clientId)

	if anStage, ok := m.partialResults[clientId]; ok {
		switch stage {
		case common.DELTA_STAGE_3:
			anStage.delta3.data = nil
		case common.ETA_STAGE_3:
			anStage.eta3.data = nil
		case common.KAPPA_STAGE_3:
			anStage.kappa3.data = nil
		case common.NU_STAGE_3:
			anStage.nu3Data.data = nil
		default:
			log.Errorf("Invalid stage: %s", stage)
			return fmt.Errorf("invalid stage: %s", stage)
		}
	}
	return nil
}

// Implementing getTaskIdentifiers for Merger
func (m *Merger) getTaskIdentifiers(clientId string, stage string) ([]*protocol.TaskIdentifier, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return utils.MapValues(partialResults.delta3.taskFragments), nil
	case common.ETA_STAGE_3:
		return utils.MapValues(partialResults.eta3.taskFragments), nil
	case common.KAPPA_STAGE_3:
		return utils.MapValues(partialResults.kappa3.taskFragments), nil
	case common.NU_STAGE_3:
		return utils.MapValues(partialResults.nu3Data.taskFragments), nil
	default:
		return nil, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
	switch stage {
	case common.DELTA_STAGE_3:
		m.delta3Results(tasks, clientId)
	case common.ETA_STAGE_3:
		m.eta3Results(tasks, clientId)
	case common.KAPPA_STAGE_3:
		m.kappa3Results(tasks, clientId)
	case common.NU_STAGE_3:
		m.nu3Results(tasks, clientId)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	return nil
}

func (m *Merger) getOmegaProcessed(clientId string, stage string) (bool, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return partialResults.delta3.omegaProcessed, nil
	case common.ETA_STAGE_3:
		return partialResults.eta3.omegaProcessed, nil
	case common.KAPPA_STAGE_3:
		return partialResults.kappa3.omegaProcessed, nil
	case common.NU_STAGE_3:
		return partialResults.nu3Data.omegaProcessed, nil
	default:
		return false, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) getRingRound(clientId string, stage string) (uint32, error) {
	partialResults := m.partialResults[clientId]
	switch stage {
	case common.DELTA_STAGE_3:
		return partialResults.delta3.ringRound, nil
	case common.ETA_STAGE_3:
		return partialResults.eta3.ringRound, nil
	case common.KAPPA_STAGE_3:
		return partialResults.kappa3.ringRound, nil
	case common.NU_STAGE_3:
		return partialResults.nu3Data.ringRound, nil
	default:
		return 0, fmt.Errorf("invalid stage: %s", stage)
	}
}
