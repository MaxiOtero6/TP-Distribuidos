package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"
)

func newJoinerTableData[T any](data T) *common.JoinerTableData[T] {

	return &common.JoinerTableData[T]{
		Data:          data,
		TaskFragments: make(map[model.TaskFragmentIdentifier]common.FragmentStatus),
		Ready:         false,
	}
}

func newJoinerStageData[S any, B any]() *common.JoinerStageData[S, B] {
	return &common.JoinerStageData[S, B]{
		SmallTable:          newJoinerTableData(make(common.SmallTableData[S])),
		BigTable:            newJoinerTableData(make(common.BigTableData[B])),
		SendedTaskCount:     0,
		SmallTableTaskCount: 0,
		RingRound:           0,
	}
}

func newJoinerPartialResults() *common.JoinerPartialResults {
	return &common.JoinerPartialResults{
		ZetaData: newJoinerStageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating](),
		IotaData: newJoinerStageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor](),
	}
}

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.JoinerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatefulEofHandler
}

func (j *Joiner) makePartialResults(clientId string) {
	if _, ok := j.partialResults[clientId]; ok {
		return
	}

	j.partialResults[clientId] = newJoinerPartialResults()
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(infraConfig *model.InfraConfig) *Joiner {
	eofHandler := eof.NewStatefulEofHandler(
		model.JoinerAction,
		infraConfig,
		joinerNextStageData,
		utils.GetWorkerIdFromHash,
	)

	joiner := &Joiner{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		partialResults: make(map[string]*common.JoinerPartialResults),
		eofHandler:     eofHandler,
	}

	go storage.StartCleanupRoutine(infraConfig.GetDirectory())

	return joiner
}

func (j *Joiner) joinZetaData(ratingsData common.BigTableData[*protocol.Zeta_Data_Rating], clientId string) map[int][]*protocol.Eta_1_Data {
	joinedDataByTask := make(map[int][]*protocol.Eta_1_Data)
	for taskNumber, ratingsByTask := range ratingsData {
		joinedData := make([]*protocol.Eta_1_Data, 0)

		for movieId, ratings := range ratingsByTask {
			movieData, ok := j.partialResults[clientId].ZetaData.SmallTable.Data[movieId]
			if !ok {
				continue
			}

			for _, rating := range ratings {
				joinedData = append(joinedData, &protocol.Eta_1_Data{
					MovieId: movieId,
					Title:   movieData.GetTitle(),
					Rating:  rating.GetRating(),
				})
			}
		}

		if len(joinedData) > 0 {
			joinedDataByTask[taskNumber] = joinedData
		}
	}

	return joinedDataByTask
}

func (j *Joiner) joinIotaData(actorsData common.BigTableData[*protocol.Iota_Data_Actor], clientId string) map[int][]*protocol.Kappa_1_Data {
	joinedDataByTask := make(map[int][]*protocol.Kappa_1_Data)

	for taskNumber, actorsByTask := range actorsData {
		joinedData := make([]*protocol.Kappa_1_Data, 0)

		for movieId, actors := range actorsByTask {
			_, ok := j.partialResults[clientId].IotaData.SmallTable.Data[movieId]
			if !ok {
				continue
			}

			for _, actor := range actors {
				joinedData = append(joinedData, &protocol.Kappa_1_Data{
					MovieId:   movieId,
					ActorId:   actor.GetActorId(),
					ActorName: actor.GetActorName(),
				})
			}
		}

		if len(joinedData) > 0 {
			joinedDataByTask[taskNumber] = joinedData
		}
	}

	return joinedDataByTask
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	zetaData := j.partialResults[clientId].ZetaData

	identifierFunc := func(input *protocol.Zeta_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Zeta_Data) *protocol.Zeta_Data_Movie {
		return &protocol.Zeta_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
			Title:   input.GetMovie().GetTitle(),
		}
	}

	smallTable := zetaData.SmallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.OmegaProcessed

	smallTableReady := omegaProcessed && len(smallTable.TaskFragments) == zetaData.SmallTableTaskCount
	smallTable.Ready = smallTableReady

	if smallTableReady {
		bigTable := zetaData.BigTable

		partialResults := j.joinZetaData(bigTable.Data, clientId)
		j.addEta1Results(tasks, partialResults, clientId)

		bigTable.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])

		if bigTable.Ready {
			zetaData.SmallTable.IsReadyToDelete = true
			zetaData.BigTable.IsReadyToDelete = true
		} else {
			zetaData.BigTable.IsReadyToDelete = true
		}

	}

	return tasks

	// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	// if err != nil {
	// 	log.Errorf("Failed to save %s data: %s", common.ZETA_STAGE, err)
	// }
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	iotaData := j.partialResults[clientId].IotaData

	identifierFunc := func(input *protocol.Iota_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Iota_Data) *protocol.Iota_Data_Movie {
		return &protocol.Iota_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
		}
	}

	smallTable := iotaData.SmallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.OmegaProcessed

	smallTableReady := omegaProcessed && len(smallTable.TaskFragments) == iotaData.SmallTableTaskCount
	smallTable.Ready = smallTableReady

	if smallTableReady {
		bigTable := iotaData.BigTable

		partialResults := j.joinIotaData(bigTable.Data, clientId)
		j.addKappa1Results(tasks, partialResults, clientId)

		bigTable.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

		if bigTable.Ready {
			iotaData.SmallTable.IsReadyToDelete = true
			iotaData.BigTable.IsReadyToDelete = true
		} else {
			//TODO delete big table
			iotaData.BigTable.IsReadyToDelete = true
		}
	}

	return tasks

	// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	// if err != nil {
	// 	log.Errorf("Failed to save %s data: %s", common.IOTA_STAGE, err)
	// }
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	zetaData := j.partialResults[clientId].ZetaData
	partialData := zetaData.BigTable

	identifierFunc := func(input *protocol.Zeta_Data) string {
		return input.GetRating().GetMovieId()
	}

	mappingFunc := func(input *protocol.Zeta_Data) *protocol.Zeta_Data_Rating {
		return &protocol.Zeta_Data_Rating{
			MovieId: input.GetRating().GetMovieId(),
			Rating:  input.GetRating().GetRating(),
		}
	}

	processBigTableJoinerStage(partialData, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	if zetaData.SmallTable.Ready {
		partialResults := j.joinZetaData(partialData.Data, clientId)
		j.addEta1Results(tasks, partialResults, clientId)

		partialData.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])

		if zetaData.BigTable.Ready {
			zetaData.SmallTable.IsReadyToDelete = true
			zetaData.BigTable.IsReadyToDelete = true
		} else {
			zetaData.BigTable.IsReadyToDelete = true
		}
	}

	// TODO - not ready -- save data

	return tasks
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	iotaData := j.partialResults[clientId].IotaData
	partialData := iotaData.BigTable

	identifierFunc := func(input *protocol.Iota_Data) string {
		return input.GetActor().GetMovieId()
	}

	mappingFunc := func(input *protocol.Iota_Data) *protocol.Iota_Data_Actor {
		return &protocol.Iota_Data_Actor{
			MovieId:   input.GetActor().GetMovieId(),
			ActorId:   input.GetActor().GetActorId(),
			ActorName: input.GetActor().GetActorName(),
		}
	}

	processBigTableJoinerStage(partialData, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	if iotaData.SmallTable.Ready {
		partialResults := j.joinIotaData(partialData.Data, clientId)
		j.addKappa1Results(tasks, partialResults, clientId)

		partialData.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

		if iotaData.BigTable.Ready {
			iotaData.SmallTable.IsReadyToDelete = true
			iotaData.BigTable.IsReadyToDelete = true
		} else {
			iotaData.BigTable.IsReadyToDelete = true
		}
	}

	// TODO - not ready -- save data

	return tasks
}

func (j *Joiner) zetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier, tableType string) common.Tasks {
	switch tableType {
	case model.SMALL_TABLE:
		return j.moviesZetaStage(data, clientId, taskIdentifier)
	case model.BIG_TABLE:
		return j.ratingsZetaStage(data, clientId, taskIdentifier)
	default:
		log.Errorf("Invalid table type: %s", tableType)
		return nil
	}
}

func (j *Joiner) iotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier, tableType string) common.Tasks {

	switch tableType {
	case model.SMALL_TABLE:
		return j.moviesIotaStage(data, clientId, taskIdentifier)
	case model.BIG_TABLE:
		return j.actorsIotaStage(data, clientId, taskIdentifier)
	default:
		log.Errorf("Invalid table type: %s", tableType)
		return nil
	}
}

func (j *Joiner) nextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	return joinerNextStageData(stage, clientId, j.infraConfig, j.itemHashFunc)
}

func joinerNextStageData(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error) {
	switch stage {
	case common.ZETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil
	case common.IOTA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  infraConfig.GetBroadcastID(),
			},
		}, nil
	case common.RING_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.RING_STAGE,
				Exchange:    infraConfig.GetEofExchange(),
				WorkerCount: infraConfig.GetJoinCount(),
				RoutingKey:  utils.GetNextNodeId(infraConfig.GetNodeId(), infraConfig.GetJoinCount()),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (j *Joiner) smallTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	// Estas 2 los dejo por si se necesita para el borrado
	var ready bool
	// var bigTableReady bool

	taskCount := int(data.GetTasksCount())

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData

		if zetaData.SmallTable.OmegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.SmallTableTaskCount = taskCount
		zetaData.SmallTable.OmegaProcessed = true

		ready = len(zetaData.SmallTable.TaskFragments) == taskCount
		zetaData.SmallTable.Ready = ready

		if ready {
			partialResults := j.joinZetaData(zetaData.BigTable.Data, clientId)
			j.addEta1Results(tasks, partialResults, clientId)

			zetaData.BigTable.Data = make(common.BigTableData[*protocol.Zeta_Data_Rating])

			if zetaData.BigTable.Ready {
				zetaData.SmallTable.IsReadyToDelete = true
				zetaData.BigTable.IsReadyToDelete = true

			} else {
				zetaData.BigTable.IsReadyToDelete = true
			}
		}

		// bigTableReady = zetaData.bigTable.ready

	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData

		if iotaData.SmallTable.OmegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.SmallTableTaskCount = taskCount
		iotaData.SmallTable.OmegaProcessed = true

		ready = len(iotaData.SmallTable.TaskFragments) == taskCount
		iotaData.SmallTable.Ready = ready

		if ready {
			partialResults := j.joinIotaData(iotaData.BigTable.Data, clientId)
			j.addKappa1Results(tasks, partialResults, clientId)

			iotaData.BigTable.Data = make(common.BigTableData[*protocol.Iota_Data_Actor])

			if iotaData.BigTable.Ready {
				iotaData.SmallTable.IsReadyToDelete = true
				iotaData.BigTable.IsReadyToDelete = true

			} else {
				iotaData.BigTable.IsReadyToDelete = true
			}
		}

		// bigTableReady = iotaData.bigTable.ready
	}

	// delete only the big table
	// if err := storage.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, dataStage, common.BIG_TABLE_SOURCE); err != nil {
	// 	log.Errorf("Failed to delete partial results: %s", err)
	// }
	// j.DeleteTableType(clientId, dataStage, common.BIG_TABLE_SOURCE)

	return tasks
}

func (j *Joiner) bigTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData

		if zetaData.BigTable.OmegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.BigTable.OmegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData

		if iotaData.BigTable.OmegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.BigTable.OmegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	}

	return tasks
}

func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	switch data.EofType {
	case model.SMALL_TABLE:
		return j.smallTableOmegaEOFStage(data, clientId)
	case model.BIG_TABLE:
		return j.bigTableOmegaEOFStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	var smallTableReady bool
	var bigTableReady bool

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].ZetaData
		ringRound := data.GetRoundNumber()

		if zetaData.RingRound >= ringRound {
			log.Infof("RingEOF for Zeta stage already processed for clientId: %s, round: %d", clientId, zetaData.RingRound)
			return tasks
		}

		zetaData.RingRound = ringRound

		resultsTaskCount := zetaData.SendedTaskCount
		taskFragments := []model.TaskFragmentIdentifier{}

		smallTableReady = zetaData.SmallTable.Ready
		if smallTableReady {
			taskFragments = utils.MapKeys(zetaData.BigTable.TaskFragments)
		}

		bigTableReady = j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, resultsTaskCount)
		zetaData.BigTable.Ready = bigTableReady

		if bigTableReady && smallTableReady {
			zetaData.SmallTable.IsReadyToDelete = true
			zetaData.BigTable.IsReadyToDelete = true
		}

	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].IotaData
		ringRound := data.GetRoundNumber()

		if iotaData.RingRound >= ringRound {
			log.Infof("RingEOF for Iota stage already processed for clientId: %s, round: %d", clientId, iotaData.RingRound)
			return tasks
		}

		iotaData.RingRound = ringRound

		resultsTaskCount := iotaData.SendedTaskCount
		taskFragments := []model.TaskFragmentIdentifier{}

		smallTableReady = iotaData.SmallTable.Ready
		if smallTableReady {
			taskFragments = utils.MapKeys(iotaData.BigTable.TaskFragments)
		}

		bigTableReady = j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, resultsTaskCount)
		iotaData.BigTable.Ready = bigTableReady

		if bigTableReady && smallTableReady {
			iotaData.SmallTable.IsReadyToDelete = true
			iotaData.BigTable.IsReadyToDelete = true
		}
	}

	return tasks
	// if ready {
	// 	if err := storage.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, data.GetStage(), common.ANY_SOURCE); err != nil {
	// 		log.Errorf("Failed to delete partial results: %s", err)
	// 	}

	// 	j.DeleteStage(clientId, data.GetStage())

	// 	if len(j.partialResults[clientId].zetaData.smallTable.data) == 0 &&
	// 		len(j.partialResults[clientId].iotaData.smallTable.data) == 0 &&
	// 		len(j.partialResults[clientId].zetaData.bigTable.data) == 0 &&
	// 		len(j.partialResults[clientId].iotaData.bigTable.data) == 0 {
	// 		j.deletePartialResult(clientId)
	// 	}
	// }
}

func (j *Joiner) Execute(task *protocol.Task) (common.Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()
	tableType := task.GetTableType()

	j.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data, clientId, taskIdentifier, tableType), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data, clientId, taskIdentifier, tableType), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return j.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return j.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (j *Joiner) addEta1Results(tasks common.Tasks, partialResultsByTask map[int][]*protocol.Eta_1_Data, clientId string) {
	dataStage := common.ZETA_STAGE
	zetaData := j.partialResults[clientId].ZetaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)

	taskDataCreator := func(stage string, data []*protocol.Eta_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	creatorId := j.infraConfig.GetNodeId()

	for taskNumber, partialResults := range partialResultsByTask {
		AddResultsToStateless(tasks, partialResults, nextStageData[0], clientId, creatorId, taskNumber, taskDataCreator)
		zetaData.SendedTaskCount++
	}

}

func (j *Joiner) addKappa1Results(tasks common.Tasks, partialResultsByTask map[int][]*protocol.Kappa_1_Data, clientId string) {
	dataStage := common.IOTA_STAGE
	iotaData := j.partialResults[clientId].IotaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)

	taskDataCreator := func(stage string, data []*protocol.Kappa_1_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task {
		return &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: data,
				},
			},
			TaskIdentifier: taskIdentifier,
		}
	}

	creatorId := j.infraConfig.GetNodeId()

	for taskNumber, partialResults := range partialResultsByTask {
		AddResultsToStateless(tasks, partialResults, nextStageData[0], clientId, creatorId, taskNumber, taskDataCreator)
		iotaData.SendedTaskCount++
	}
}

func processSmallTableJoinerStage[G any, S any](
	partialData *common.JoinerTableData[common.SmallTableData[S]],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) S,
) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, exists := partialData.TaskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.TaskFragments[taskID] = common.FragmentStatus{
		Logged: false,
	}

	for _, item := range data {
		id := identifierFunc(item)
		partialData.Data[id] = mappingFunc(item)
	}
}

func processBigTableJoinerStage[G any, B any](
	partialData *common.JoinerTableData[common.BigTableData[B]],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) B,
) {
	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, exists := partialData.TaskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.TaskFragments[taskID] = common.FragmentStatus{
		Logged: false,
	}

	taskNumber := int(taskID.TaskNumber)

	if _, exists := partialData.Data[taskNumber]; !exists {
		partialData.Data[taskNumber] = make(map[string][]B)
	}

	for _, item := range data {
		id := identifierFunc(item)

		if _, exists := partialData.Data[taskNumber][id]; !exists {
			partialData.Data[taskNumber][id] = make([]B, 0)
		}

		partialData.Data[taskNumber][id] = append(partialData.Data[taskNumber][id], mappingFunc(item))
	}
}

func joinerAddResults[T any](
	tasks common.Tasks,
	results []T,
	nextStageData common.NextStageData,
	clientId string,
	creatorId string,
	taskNumber uint32,
	taskDataCreator func(stage string, results []T, clientId string, taskIdentifier *protocol.TaskIdentifier) *protocol.Task,
) {
	exchange := nextStageData.Exchange
	routingKey := nextStageData.RoutingKey

	if _, ok := tasks[exchange]; !ok {
		tasks[exchange] = make(map[string][]*protocol.Task)
	}

	taskIdentifier := &protocol.TaskIdentifier{
		CreatorId:          creatorId,
		TaskNumber:         taskNumber,
		TaskFragmentNumber: 0,
		LastFragment:       true,
	}

	task := taskDataCreator(
		nextStageData.Stage,
		results,
		clientId,
		taskIdentifier,
	)

	if _, ok := tasks[exchange][routingKey]; !ok {
		tasks[exchange][routingKey] = []*protocol.Task{}
	}

	tasks[exchange][routingKey] = append(tasks[exchange][routingKey], task)
}

func (j *Joiner) LoadData(dirBase string) error {
	var err error
	j.partialResults, err = storage.LoadJoinerPartialResultsFromDisk(dirBase)
	if err != nil {
		return fmt.Errorf("failed to load joiner partial results from disk: %w", err)
	}
	return nil
}

// // Delete partialResult by clientId
// func (j *Joiner) deletePartialResult(clientId string) {
// 	delete(j.partialResults, clientId)
// 	log.Infof("Deleted partial result for clientId: %s", clientId)
// }

func (j *Joiner) SaveData(task *protocol.Task) error {
	stage := task.GetStage()
	tableType := task.GetTableType()
	clientId := task.GetClientId()
	taskIdentifier := task.GetTaskIdentifier()

	taskID := model.TaskFragmentIdentifier{
		CreatorId:          taskIdentifier.GetCreatorId(),
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	switch s := stage.(type) {
	case *protocol.Task_Iota:

		IotaPartialData := j.partialResults[clientId].IotaData
		switch tableType {
		case model.SMALL_TABLE:
			data := s.Iota.GetData()
			err := storage.SaveJoinerTableToFile(j.infraConfig.GetDirectory(), clientId, data, common.FolderType(tableType), IotaPartialData.SmallTable, taskID)
			if err != nil {
				return fmt.Errorf("failed to save Iota small table data: %w", err)
			}
			err = storage.SaveJoinerMetadataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.FolderType(tableType), IotaPartialData)
			if err != nil {
				return fmt.Errorf("failed to save Iota small table metadata: %w", err)
			}
			return nil
		case model.BIG_TABLE:
			data := s.Iota.GetData()
			err := storage.SaveJoinerTableToFile(j.infraConfig.GetDirectory(), clientId, data, common.FolderType(tableType), IotaPartialData.BigTable, taskID)
			if err != nil {
				return fmt.Errorf("failed to save Iota big table data: %w", err)
			}
			err = storage.SaveJoinerMetadataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.FolderType(tableType), IotaPartialData)
			if err != nil {
				return fmt.Errorf("failed to save Iota big table metadata: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("invalid table type: %s", tableType)
		}

	case *protocol.Task_Zeta:
		switch tableType {
		case model.SMALL_TABLE:
			data := s.Zeta.GetData()
			err := storage.SaveJoinerTableToFile(j.infraConfig.GetDirectory(), clientId, data, common.FolderType(tableType), j.partialResults[clientId].ZetaData.SmallTable, taskID)
			if err != nil {
				return fmt.Errorf("failed to save Zeta small table data: %w", err)
			}
			err = storage.SaveJoinerMetadataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.FolderType(tableType), j.partialResults[clientId].ZetaData)
			if err != nil {
				return fmt.Errorf("failed to save Zeta small table metadata: %w", err)
			}
			return nil
		case model.BIG_TABLE:
			data := s.Zeta.GetData()
			err := storage.SaveJoinerTableToFile(j.infraConfig.GetDirectory(), clientId, data, common.FolderType(tableType), j.partialResults[clientId].ZetaData.BigTable, taskID)
			if err != nil {
				return fmt.Errorf("failed to save Zeta big table data: %w", err)
			}
			err = storage.SaveJoinerMetadataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.FolderType(tableType), j.partialResults[clientId].ZetaData)
			if err != nil {
				return fmt.Errorf("failed to save Zeta big table metadata: %w", err)
			}
			return nil
		default:
			return fmt.Errorf("invalid table type: %s", tableType)
		}

	case *protocol.Task_OmegaEOF:
		processedStage := task.GetOmegaEOF().GetData().GetStage()
		return j.loadMetaData(processedStage, clientId, tableType)

	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		return j.loadMetaData(processedStage, clientId, tableType)

	default:
		return fmt.Errorf("invalid query stage: %v", s)
	}

}

func (j *Joiner) loadMetaData(stage string, clientId string, tableType string) error {
	switch stage {
	case common.ZETA_STAGE:
		partialData := j.partialResults[clientId].ZetaData
		return storage.SaveJoinerMetadataToFile(
			j.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.FolderType(tableType),
			partialData,
		)

	case common.IOTA_STAGE:

		partialData := j.partialResults[clientId].IotaData
		return storage.SaveJoinerMetadataToFile(
			j.infraConfig.GetDirectory(),
			clientId,
			stage,
			common.FolderType(tableType),
			partialData,
		)
	default:
		return fmt.Errorf("invalid stage: %s", stage)
	}
}

func (j *Joiner) DeleteData(task *protocol.Task) error {
	stage := task.GetStage()
	tableType := task.GetTableType()
	clientId := task.GetClientId()

	iota := j.partialResults[clientId].IotaData
	zeta := j.partialResults[clientId].ZetaData

	switch s := stage.(type) {
	case *protocol.Task_Iota:
		switch tableType {
		case model.SMALL_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), stage, tableType, clientId, iota.SmallTable.IsReadyToDelete)
		case model.BIG_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), stage, tableType, clientId, iota.BigTable.IsReadyToDelete)
		default:
			return fmt.Errorf("invalid table type: %s", tableType)

		}

	case *protocol.Task_Zeta:
		switch tableType {
		case model.SMALL_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), stage, tableType, clientId, zeta.SmallTable.IsReadyToDelete)
		case model.BIG_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), stage, tableType, clientId, zeta.BigTable.IsReadyToDelete)
		default:
			return fmt.Errorf("invalid table type: %s", tableType)

		}

	case *protocol.Task_OmegaEOF:
		processedStage := task.GetOmegaEOF().GetData().GetStage()
		switch tableType {
		case model.SMALL_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), processedStage, tableType, clientId, zeta.SmallTable.IsReadyToDelete)
		case model.BIG_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), processedStage, tableType, clientId, zeta.BigTable.IsReadyToDelete)
		default:
			return fmt.Errorf("invalid table type: %s", tableType)

		}

	case *protocol.Task_RingEOF:
		processedStage := task.GetRingEOF().GetStage()
		switch tableType {
		case model.SMALL_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), processedStage, tableType, clientId, zeta.SmallTable.IsReadyToDelete)
		case model.BIG_TABLE:
			return storage.TryDeletePartialData(j.infraConfig.GetDirectory(), processedStage, tableType, clientId, zeta.BigTable.IsReadyToDelete)
		default:
			return fmt.Errorf("invalid table type: %s", tableType)

		}

	default:
		return fmt.Errorf("invalid query stage: %v", s)
	}

}
