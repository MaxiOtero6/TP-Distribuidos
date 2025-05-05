package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
)

const REDUCER_STAGES_COUNT uint = 4
const REDUCER_FILE_TYPE string = ""

type ReducerPartialResults struct {
	toDeleteCount uint
	delta2        map[string]*protocol.Delta_2_Data
	eta2          map[string]*protocol.Eta_2_Data
	kappa2        map[string]*protocol.Kappa_2_Data
	nu2Data       map[string]*protocol.Nu_2_Data
}

// Reducer is a struct that implements the Action interface.
type Reducer struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*ReducerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     eof_handler.IEOFHandler
}

func (r *Reducer) makePartialResults(clientId string) {
	if _, ok := r.partialResults[clientId]; ok {
		return
	}

	r.partialResults[clientId] = &ReducerPartialResults{
		delta2:  make(map[string]*protocol.Delta_2_Data),
		eta2:    make(map[string]*protocol.Eta_2_Data),
		kappa2:  make(map[string]*protocol.Kappa_2_Data),
		nu2Data: make(map[string]*protocol.Nu_2_Data),
	}
}

// NewReduce creates a new Reduce instance.
// It initializes the worker count and returns a pointer to the Reduce struct.
func NewReducer(infraConfig *model.InfraConfig, eofHandler eof_handler.IEOFHandler) *Reducer {
	return &Reducer{
		infraConfig:    infraConfig,
		partialResults: make(map[string]*ReducerPartialResults),
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		eofHandler:     eofHandler,
	}
}

/*
delta2Stage partially sum up investment by country

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Then it divides the resulting countries by hashing each country and send it to the corresponding worker to finish the reduction.

# Return example

	{
		"topExchange": {
			"delta_3": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) delta2Stage(data []*protocol.Delta_2_Data, clientId string) (tasks common.Tasks) {
	dataMap := r.partialResults[clientId].delta2

	// Sum up the partial budgets by country
	for _, country := range data {
		prodCountry := country.GetCountry()

		if _, ok := dataMap[prodCountry]; !ok {
			dataMap[prodCountry] = &protocol.Delta_2_Data{
				Country:       prodCountry,
				PartialBudget: 0,
			}
		}

		dataMap[prodCountry].PartialBudget += country.GetPartialBudget()
	}

	err := utils.SaveDataToFile(r.infraConfig.GetDirectory(), clientId, common.DELTA_STAGE_2, common.ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.DELTA_STAGE_2, err)
	}

	return nil
}

/*
eta2Stage calculates average rating for each movie

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"topExchange": {
			"theta": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) eta2Stage(data []*protocol.Eta_2_Data, clientId string) (tasks common.Tasks) {
	dataMap := r.partialResults[clientId].eta2

	// Sum up the partial ratings and counts for each movie
	for _, e2Data := range data {
		movieId := e2Data.GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = &protocol.Eta_2_Data{
				MovieId: movieId,
				Title:   e2Data.GetTitle(),
				Rating:  0,
				Count:   0,
			}
		}

		dataMap[movieId].Rating += e2Data.GetRating()
		dataMap[movieId].Count += e2Data.GetCount()
	}

	err := utils.SaveDataToFile(r.infraConfig.GetDirectory(), clientId, common.ETA_STAGE_2, common.ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.ETA_STAGE_2, err)
	}

	return nil
}

/*
kappa2Stage reduce into one, partials actors participations in movies

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"topExchange": {
			"lambda": {
				"0": Task,
				"1": Task
			}
		},
	}
*/
func (r *Reducer) kappa2Stage(data []*protocol.Kappa_2_Data, clientId string) (tasks common.Tasks) {
	dataMap := r.partialResults[clientId].kappa2

	// Sum up the partial participations by actor
	for _, k2Data := range data {
		actorId := k2Data.GetActorId()

		if _, ok := dataMap[actorId]; !ok {
			dataMap[actorId] = &protocol.Kappa_2_Data{
				ActorId:               actorId,
				ActorName:             k2Data.GetActorName(),
				PartialParticipations: 0,
			}
		}

		dataMap[actorId].PartialParticipations += k2Data.GetPartialParticipations()
	}

	err := utils.SaveDataToFile(r.infraConfig.GetDirectory(), clientId, common.KAPPA_STAGE_2, common.ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.KAPPA_STAGE_2, err)
	}

	return nil
}

/*
nu2Stage reduce into one, partials revenue and budget from movies by sentiment.

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"resultExchange": {
			"result": {
				"" : Task
			}
		},
	}
*/
func (r *Reducer) nu2Stage(data []*protocol.Nu_2_Data, clientId string) (tasks common.Tasks) {
	dataMap := r.partialResults[clientId].nu2Data

	// Sum up the budget and revenue by sentiment
	for _, nu2Data := range data {
		sentiment := fmt.Sprintf("%t", nu2Data.GetSentiment())

		if _, ok := dataMap[sentiment]; !ok {
			dataMap[sentiment] = &protocol.Nu_2_Data{
				Sentiment: nu2Data.GetSentiment(),
				Ratio:     0,
				Count:     0,
			}
		}

		dataMap[sentiment].Ratio += nu2Data.GetRatio()
		dataMap[sentiment].Count += nu2Data.GetCount()
	}

	err := utils.SaveDataToFile(r.infraConfig.GetDirectory(), clientId, common.NU_STAGE_2, common.ANY_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.NU_STAGE_2, err)
	}

	return nil
}

func (r *Reducer) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	switch stage {
	case common.DELTA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.DELTA_STAGE_3,
				Exchange:    r.infraConfig.GetMergeExchange(),
				WorkerCount: r.infraConfig.GetMergeCount(),
				RoutingKey:  r.infraConfig.GetEofBroadcastRK(),
			},
		}, nil
	case common.ETA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_3,
				Exchange:    r.infraConfig.GetMergeExchange(),
				WorkerCount: r.infraConfig.GetMergeCount(),
				RoutingKey:  r.infraConfig.GetEofBroadcastRK(),
			},
		}, nil
	case common.KAPPA_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_3,
				Exchange:    r.infraConfig.GetMergeExchange(),
				WorkerCount: r.infraConfig.GetMergeCount(),
				RoutingKey:  r.infraConfig.GetEofBroadcastRK(),
			},
		}, nil
	case common.NU_STAGE_2:
		return []common.NextStageData{
			{
				Stage:       common.NU_STAGE_3,
				Exchange:    r.infraConfig.GetMergeExchange(),
				WorkerCount: r.infraConfig.GetMergeCount(),
				RoutingKey:  r.infraConfig.GetEofBroadcastRK(),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (r *Reducer) delta2Results(tasks common.Tasks, clientId string) {
	dataMap := r.partialResults[clientId].delta2

	MERGE_EXCHANGE := r.infraConfig.GetMergeExchange()
	MERGE_COUNT := r.infraConfig.GetMergeCount()

	if _, ok := tasks[MERGE_EXCHANGE]; !ok {
		tasks[MERGE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[MERGE_EXCHANGE][common.DELTA_STAGE_3] = make(map[string]*protocol.Task)
	delta3Data := make(map[string][]*protocol.Delta_3_Data)

	// Divide the resulting countries by hashing each country
	for _, d3Data := range dataMap {
		nodeId := r.itemHashFunc(MERGE_COUNT, d3Data.GetCountry())
		delta3Data[nodeId] = append(delta3Data[nodeId], &protocol.Delta_3_Data{
			Country:       d3Data.GetCountry(),
			PartialBudget: d3Data.GetPartialBudget(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range delta3Data {
		tasks[MERGE_EXCHANGE][common.DELTA_STAGE_3][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Delta_3{
				Delta_3: &protocol.Delta_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) eta2Results(tasks common.Tasks, clientId string) {
	dataMap := r.partialResults[clientId].eta2

	MERGE_EXCHANGE := r.infraConfig.GetMergeExchange()
	MERGE_COUNT := r.infraConfig.GetMergeCount()

	if _, ok := tasks[MERGE_EXCHANGE]; !ok {
		tasks[MERGE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[MERGE_EXCHANGE][common.ETA_STAGE_3] = make(map[string]*protocol.Task)
	eta3Data := make(map[string][]*protocol.Eta_3_Data)

	// Divide the resulting movies by hashing each movie
	for _, e2Data := range dataMap {
		nodeId := r.itemHashFunc(MERGE_COUNT, e2Data.GetMovieId())
		eta3Data[nodeId] = append(eta3Data[nodeId], &protocol.Eta_3_Data{
			MovieId: e2Data.GetMovieId(),
			Title:   e2Data.GetTitle(),
			Rating:  e2Data.GetRating(),
			Count:   e2Data.GetCount(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range eta3Data {
		tasks[MERGE_EXCHANGE][common.ETA_STAGE_3][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_3{
				Eta_3: &protocol.Eta_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) kappa2Results(tasks common.Tasks, clientId string) {
	dataMap := r.partialResults[clientId].kappa2

	MERGE_EXCHANGE := r.infraConfig.GetMergeExchange()
	MERGE_COUNT := r.infraConfig.GetMergeCount()

	if _, ok := tasks[MERGE_EXCHANGE]; !ok {
		tasks[MERGE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[MERGE_EXCHANGE][common.KAPPA_STAGE_3] = make(map[string]*protocol.Task)
	kappa3Data := make(map[string][]*protocol.Kappa_3_Data)

	// Divide the resulting actors by hashing each actor
	for _, k2Data := range dataMap {
		nodeId := r.itemHashFunc(MERGE_COUNT, k2Data.GetActorId())
		kappa3Data[nodeId] = append(kappa3Data[nodeId], &protocol.Kappa_3_Data{
			ActorId:               k2Data.GetActorId(),
			ActorName:             k2Data.GetActorName(),
			PartialParticipations: k2Data.GetPartialParticipations(),
		})
	}
	// Create tasks for each worker
	for nodeId, data := range kappa3Data {
		tasks[MERGE_EXCHANGE][common.KAPPA_STAGE_3][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_3{
				Kappa_3: &protocol.Kappa_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) nu2Results(tasks common.Tasks, clientId string) {
	dataMap := r.partialResults[clientId].nu2Data

	MERGE_EXCHANGE := r.infraConfig.GetMergeExchange()
	MERGE_COUNT := r.infraConfig.GetMergeCount()

	if _, ok := tasks[MERGE_EXCHANGE]; !ok {
		tasks[MERGE_EXCHANGE] = make(map[string]map[string]*protocol.Task)
	}

	tasks[MERGE_EXCHANGE][common.NU_STAGE_3] = make(map[string]*protocol.Task)

	nu3Data := make(map[string][]*protocol.Nu_3_Data)

	// Divide the resulting sentiments by hashing each sentiment
	for _, n2Data := range dataMap {
		sentiment := fmt.Sprintf("%t", n2Data.GetSentiment())
		nodeId := r.itemHashFunc(MERGE_COUNT, sentiment)
		nu3Data[nodeId] = append(nu3Data[nodeId], &protocol.Nu_3_Data{
			Sentiment: n2Data.GetSentiment(),
			Ratio:     n2Data.GetRatio(),
			Count:     n2Data.GetCount(),
		})
	}

	// Create tasks for each worker
	for nodeId, data := range nu3Data {
		tasks[MERGE_EXCHANGE][common.NU_STAGE_3][nodeId] = &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Nu_3{
				Nu_3: &protocol.Nu_3{
					Data: data,
				},
			},
		}
	}
}

func (r *Reducer) addResultsToNextStage(tasks common.Tasks, stage string, clientId string) error {
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

func (r *Reducer) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = r.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)

	if err := r.addResultsToNextStage(tasks, data.GetStage(), clientId); err == nil {
		if err := utils.DeletePartialResults(r.infraConfig.GetDirectory(), clientId, data.GetStage(), common.ANY_SOURCE); err != nil {
			log.Errorf("Failed to delete partial results: %s", err)
		}
		if err := r.deleteStage(clientId, data.GetStage()); err != nil {
			log.Errorf("Failed to delete stage: %s", err)
		}
	}

	return tasks
}

func (r *Reducer) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	tasks = r.eofHandler.HandleRing(data, clientId, r.getNextStageData, true)

	if r.infraConfig.GetNodeId() != data.GetCreatorId() {
		if err := r.addResultsToNextStage(tasks, data.GetStage(), clientId); err == nil {
			if err := utils.DeletePartialResults(r.infraConfig.GetDirectory(), clientId, data.GetStage(), common.ANY_SOURCE); err != nil {
				log.Errorf("Failed to delete partial results: %s", err)
			}
			if err := r.deleteStage(clientId, data.GetStage()); err != nil {
				log.Errorf("Failed to delete stage: %s", err)
			}
		} else {
			log.Errorf("Failed to add results to next stage: %s", err)
		}
	}

	return tasks
}

func (r *Reducer) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	r.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Delta_2:
		data := v.Delta_2.GetData()
		return r.delta2Stage(data, clientId), nil

	case *protocol.Task_Eta_2:
		data := v.Eta_2.GetData()
		return r.eta2Stage(data, clientId), nil

	case *protocol.Task_Kappa_2:
		data := v.Kappa_2.GetData()
		return r.kappa2Stage(data, clientId), nil

	case *protocol.Task_Nu_2:
		data := v.Nu_2.GetData()
		return r.nu2Stage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return r.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return r.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (r *Reducer) deleteStage(clientId string, stage string) error {

	log.Debugf("Deleting stage %s for client %s", stage, clientId)

	if anStage, ok := r.partialResults[clientId]; ok {
		switch stage {
		case common.DELTA_STAGE_2:
			anStage.delta2 = nil
		case common.ETA_STAGE_2:
			anStage.eta2 = nil
		case common.KAPPA_STAGE_2:
			anStage.kappa2 = nil
		case common.NU_STAGE_2:
			anStage.nu2Data = nil
		default:
			log.Errorf("Invalid stage: %s", stage)
			return fmt.Errorf("invalid stage: %s", stage)

		}
	}
	return nil
}
