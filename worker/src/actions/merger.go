package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

const MERGER_STAGES_COUNT uint = 4

type MergerPartialResults struct {
	toDeleteCount uint
	delta3        map[string]*protocol.Delta_3_Data
	eta3          map[string]*protocol.Eta_3_Data
	kappa3        map[string]*protocol.Kappa_3_Data
	nu3Data       map[string]*protocol.Nu_3_Data
}

// Merger is a struct that implements the Action interface.
type Merger struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*MergerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
}

func (m *Merger) makePartialResults(clientId string) {
	if _, ok := m.partialResults[clientId]; ok {
		return
	}

	m.partialResults[clientId] = &MergerPartialResults{
		delta3:  make(map[string]*protocol.Delta_3_Data),
		eta3:    make(map[string]*protocol.Eta_3_Data),
		kappa3:  make(map[string]*protocol.Kappa_3_Data),
		nu3Data: make(map[string]*protocol.Nu_3_Data),
	}
}

// NewMerger creates a new Merger instance.
// It initializes the worker count and returns a pointer to the Merger struct.
func NewMerger(infraConfig *model.InfraConfig) *Merger {
	return &Merger{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*MergerPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
	}
}

/*
delta3Stage merge the total investment by country

This function is nil-safe, meaning it will not panic if the input is nil.

Return example

	{
		"topExchange": {
			"epsilon": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (m *Merger) delta3Stage(data []*protocol.Delta_3_Data, clientId string) (tasks Tasks) {
	dataMap := m.partialResults[clientId].delta3

	// Sum up the partial budgets by country
	for _, country := range data {
		prodCountry := country.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_3_Data{
				Country:       prodCountry,
				PartialBudget: 0,
			}
		}

		dataMap[prodCountry].PartialBudget += country.GetPartialBudget()
	}

	log.Infof("delta3Stage: %v", dataMap)

	err := utils.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, DELTA_STAGE_3, ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", DELTA_STAGE_3, err)
	}

	return nil
}

/*
 */
func (m *Merger) eta3Stage(data []*protocol.Eta_3_Data, clientId string) (tasks Tasks) {
	dataMap := m.partialResults[clientId].eta3

	// Sum up the partial ratings and counts for each movie
	for _, e3Data := range data {
		movieId := e3Data.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_3_Data{
				MovieId: movieId,
				Title:   e3Data.GetTitle(),
				Rating:  0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += e3Data.GetRating()
		dataMap[movieId].Count += e3Data.GetCount()
	}

	err := utils.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, ETA_STAGE_3, ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", ETA_STAGE_3, err)
	}

	return nil
}

/*
 */
func (m *Merger) kappa3Stage(data []*protocol.Kappa_3_Data, clientId string) (tasks Tasks) {
	dataMap := m.partialResults[clientId].kappa3

	// Sum up the partial participations by actor
	for _, k3Data := range data {
		actorId := k3Data.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_3_Data{
				ActorId:               actorId,
				ActorName:             k3Data.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += k3Data.GetPartialParticipations()
	}

	err := utils.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, KAPPA_STAGE_3, ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", KAPPA_STAGE_3, err)
	}

	return nil
}

/*
 */
func (m *Merger) nu3Stage(data []*protocol.Nu_3_Data, clientId string) (tasks Tasks) {
	dataMap := m.partialResults[clientId].nu3Data

	// Sum up the budget and revenue by sentiment
	for _, nu3Data := range data {
		sentiment := fmt.Sprintf("%t", nu3Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_3_Data{
				Sentiment: nu3Data.GetSentiment(),
				Ratio:     0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += nu3Data.GetRatio()
		dataMap[sentiment].Count += nu3Data.GetCount()
	}

	err := utils.SaveDataToFile(m.infraConfig.GetDirectory(), clientId, NU_STAGE_3, ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", NU_STAGE_3, err)
	}

	return nil
}

func (m *Merger) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case DELTA_STAGE_3:
		return EPSILON_STAGE, m.infraConfig.GetTopExchange(), m.infraConfig.GetTopCount(), nil
	case ETA_STAGE_3:
		return THETA_STAGE, m.infraConfig.GetTopExchange(), m.infraConfig.GetTopCount(), nil
	case KAPPA_STAGE_3:
		return LAMBDA_STAGE, m.infraConfig.GetTopExchange(), m.infraConfig.GetTopCount(), nil
	case NU_STAGE_3:
		return RESULT_STAGE, m.infraConfig.GetResultExchange(), 1, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (m *Merger) getNextNodeId(nodeId string) (string, error) {
	nodeIdInt, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert nodeId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (nodeIdInt+1)%m.infraConfig.GetMergeCount())
	return nextNodeId, nil
}

func (m *Merger) delta3Results(tasks Tasks, clientId string) {
	dataMap := m.partialResults[clientId].delta3

	TOP_EXCHANGE := m.infraConfig.GetTopExchange()

	if _, ok := tasks[TOP_EXCHANGE]; !ok {
		tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[TOP_EXCHANGE][EPSILON_STAGE] = make(map[string]*protocol.Task)
	epsilonData := make(map[string][]*protocol.Epsilon_Data)

	// Asign the data to the corresponding worker
	nodeId := m.itemHashFunc(m.infraConfig.GetTopCount(), EPSILON_STAGE)

	for _, eData := range dataMap {
		epsilonData[nodeId] = append(epsilonData[nodeId], &protocol.Epsilon_Data{
			ProdCountry:     eData.GetCountry(),
			TotalInvestment: eData.GetPartialBudget(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range epsilonData {
		tasks[TOP_EXCHANGE][EPSILON_STAGE][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Epsilon{
				Epsilon: &protocol.Epsilon{
					Data: data,
				},
			},
		}
	}
}

func (m *Merger) eta3Results(tasks Tasks, clientId string) {
	dataMap := m.partialResults[clientId].eta3

	TOP_EXCHANGE := m.infraConfig.GetTopExchange()

	if _, ok := tasks[TOP_EXCHANGE]; !ok {
		tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[TOP_EXCHANGE][THETA_STAGE] = make(map[string]*protocol.Task)
	thetaData := make(map[string][]*protocol.Theta_Data)

	// Asign the data to the corresponding worker
	nodeId := m.itemHashFunc(m.infraConfig.GetTopCount(), THETA_STAGE)

	for _, e3Data := range dataMap {
		avgRating := float32(e3Data.GetRating()) / float32(e3Data.GetCount())
		thetaData[nodeId] = append(thetaData[nodeId], &protocol.Theta_Data{
			Id:        e3Data.GetMovieId(),
			Title:     e3Data.GetTitle(),
			AvgRating: avgRating,
		})
	}

	// Create tasks for each worker
	for nodeId, data := range thetaData {
		tasks[TOP_EXCHANGE][THETA_STAGE][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Theta{
				Theta: &protocol.Theta{
					Data: data,
				},
			},
		}
	}
}

func (m *Merger) kappa3Results(tasks Tasks, clientId string) {
	dataMap := m.partialResults[clientId].kappa3
	TOP_EXCHANGE := m.infraConfig.GetTopExchange()

	if _, ok := tasks[TOP_EXCHANGE]; !ok {
		tasks[TOP_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[TOP_EXCHANGE][LAMBDA_STAGE] = make(map[string]*protocol.Task)
	lambdaData := make(map[string][]*protocol.Lambda_Data)
	// Asign the data to the corresponding worker
	nodeId := m.itemHashFunc(m.infraConfig.GetTopCount(), LAMBDA_STAGE)
	// Divide the resulting actors by hashing each actor
	for _, k3Data := range dataMap {
		participations := k3Data.GetPartialParticipations()
		lambdaData[nodeId] = append(lambdaData[nodeId], &protocol.Lambda_Data{
			ActorId:        k3Data.GetActorId(),
			ActorName:      k3Data.GetActorName(),
			Participations: participations,
		})
	}
	// Create tasks for each worker
	for nodeId, data := range lambdaData {
		tasks[TOP_EXCHANGE][LAMBDA_STAGE][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Lambda{
				Lambda: &protocol.Lambda{
					Data: data,
				},
			},
		}
	}
}

func (m *Merger) nu3Results(tasks Tasks, clientId string) {
	dataMap := m.partialResults[clientId].nu3Data

	RESULT_EXCHANGE := m.infraConfig.GetResultExchange()

	if _, ok := tasks[RESULT_EXCHANGE]; !ok {
		tasks[RESULT_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[RESULT_EXCHANGE][RESULT_STAGE] = make(map[string]*protocol.Task)
	resultData := make(map[string][]*protocol.Result5_Data)

	// Asign the data to the corresponding worker
	nodeId := clientId

	for _, n3Data := range dataMap {
		ratio := n3Data.GetRatio() / float32(n3Data.GetCount())
		resultData[nodeId] = append(resultData[nodeId], &protocol.Result5_Data{
			Sentiment: n3Data.GetSentiment(),
			Ratio:     ratio,
		})
	}

	// Create tasks for each worker
	for nodeId, data := range resultData {
		tasks[RESULT_EXCHANGE][RESULT_STAGE][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Result5{
				Result5: &protocol.Result5{
					Data: data,
				},
			},
		}
	}
}

func (m *Merger) addResultsToNextStage(tasks Tasks, stage string, clientId string) error {
	switch stage {
	case DELTA_STAGE_3:
		m.delta3Results(tasks, clientId)
	case ETA_STAGE_3:
		m.eta3Results(tasks, clientId)
	case KAPPA_STAGE_3:
		m.kappa3Results(tasks, clientId)
	case NU_STAGE_3:
		m.nu3Results(tasks, clientId)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}

	m.partialResults[clientId].toDeleteCount++

	return nil
}

/*
 */
func (m *Merger) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	tasks = make(Tasks)
	log.Debugf("omegaEOFStage: %v", data)
	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == m.infraConfig.GetNodeId() {
		nextStage, nextExchange, nextStageCount, err := m.getNextStageData(data.GetStage())
		if err != nil {
			log.Errorf("Failed to get next stage data: %s", err)
			return nil
		}

		nextStageEOF := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						WorkerCreatorId: "",
						Stage:           nextStage,
					},
				},
			},
		}

		var nodeId string

		if nextStage == RESULT_STAGE {
			nodeId = clientId
		} else if nextExchange == m.infraConfig.GetTopExchange() {
			nodeId = m.itemHashFunc(m.infraConfig.GetTopCount(), nextStage)
		} else {
			nodeId = m.randomHashFunc(nextStageCount)
		}

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStage][nodeId] = nextStageEOF

	} else { // if the creator is not the same as the worker, send the stage results and EOF to the next node
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = m.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := m.getNextNodeId(m.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		mergeExchange := m.infraConfig.GetMergeExchange()
		stage := data.GetStage()

		tasks[mergeExchange] = make(map[string]map[string]*protocol.Task)
		tasks[mergeExchange][stage] = make(map[string]*protocol.Task)
		tasks[mergeExchange][stage][nextNode] = eofTask

		// send the results
		if err := m.addResultsToNextStage(tasks, data.GetStage(), clientId); err == nil {
			if m.partialResults[clientId].toDeleteCount >= MERGER_STAGES_COUNT {
				delete(m.partialResults, clientId)
			}

			if err := utils.DeletePartialResults(m.infraConfig.GetDirectory(), clientId, data.GetStage(), ANY_SOURCE); err != nil {
				log.Errorf("Failed to delete partial results: %s", err)
			}
			if err := m.deleteStage(clientId, data.GetStage()); err != nil {
				log.Errorf("Failed to delete stage: %s", err)
			}
		}

	}
	return tasks
}

func (m *Merger) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	m.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Delta_3:
		data := v.Delta_3.GetData()
		return m.delta3Stage(data, clientId), nil

	case *protocol.Task_Eta_3:
		data := v.Eta_3.GetData()
		return m.eta3Stage(data, clientId), nil

	case *protocol.Task_Kappa_3:
		data := v.Kappa_3.GetData()
		return m.kappa3Stage(data, clientId), nil

	case *protocol.Task_Nu_3:
		data := v.Nu_3.GetData()
		return m.nu3Stage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return m.omegaEOFStage(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (m *Merger) deleteStage(clientId string, stage string) error {

	log.Infof("Deleting stage %s for client %s", stage, clientId)

	if anStage, ok := m.partialResults[clientId]; ok {
		switch stage {
		case DELTA_STAGE_3:
			anStage.delta3 = nil
		case ETA_STAGE_3:
			anStage.eta3 = nil
		case KAPPA_STAGE_3:
			anStage.kappa3 = nil
		case NU_STAGE_3:
			anStage.nu3Data = nil
		default:
			log.Errorf("Invalid stage: %s", stage)
			return fmt.Errorf("invalid stage: %s", stage)
		}
	}
	return nil
}
