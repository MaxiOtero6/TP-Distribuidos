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

type JoinerTableData[T any] struct {
	data           map[string]T
	taskFragments  map[common.TaskFragmentIdentifier]struct{}
	ready          bool
	omegaProcessed bool
}

type JoinerStageData[S any, B any] struct {
	smallTable          JoinerTableData[*S]
	bigTable            JoinerTableData[[]*B]
	sendedFragmentCount int
	smallTableTaskCount int
	ringRound           uint32
}

type JoinerPartialResults struct {
	zetaData JoinerStageData[protocol.Zeta_Data_Movie, protocol.Zeta_Data_Rating]
	iotaData JoinerStageData[protocol.Iota_Data_Movie, protocol.Iota_Data_Actor]
}

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*JoinerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     *eof.StatefulEofHandler
}

func (j *Joiner) makePartialResults(clientId string) {
	if _, ok := j.partialResults[clientId]; ok {
		return
	}

	j.partialResults[clientId] = &JoinerPartialResults{
		zetaData: JoinerStageData[protocol.Zeta_Data_Movie, protocol.Zeta_Data_Rating]{
			smallTable: JoinerTableData[*protocol.Zeta_Data_Movie]{
				data: make(map[string]*protocol.Zeta_Data_Movie),
			},
			bigTable: JoinerTableData[[]*protocol.Zeta_Data_Rating]{
				data: make(map[string][]*protocol.Zeta_Data_Rating),
			},
		},
		iotaData: JoinerStageData[protocol.Iota_Data_Movie, protocol.Iota_Data_Actor]{
			smallTable: JoinerTableData[*protocol.Iota_Data_Movie]{
				data: make(map[string]*protocol.Iota_Data_Movie),
			},
			bigTable: JoinerTableData[[]*protocol.Iota_Data_Actor]{
				data: make(map[string][]*protocol.Iota_Data_Actor),
			},
		},
	}
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
		partialResults: make(map[string]*JoinerPartialResults),
		eofHandler:     eofHandler,
	}

	go storage.StartCleanupRoutine(infraConfig.GetDirectory())

	return joiner
}

func (j *Joiner) joinZetaData(tasks common.Tasks, ratingsData map[string][]*protocol.Zeta_Data_Rating, clientId string) []*protocol.Eta_1_Data {
	joinedData := make([]*protocol.Eta_1_Data, 0)

	for movieId, ratings := range ratingsData {
		movieData, ok := j.partialResults[clientId].zetaData.smallTable.data[movieId]
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

	return joinedData
}

func (j *Joiner) joinIotaData(tasks common.Tasks, actorsData map[string][]*protocol.Iota_Data_Actor, clientId string) []*protocol.Kappa_1_Data {
	joinedData := make([]*protocol.Kappa_1_Data, 0)

	for movieId, actors := range actorsData {
		_, ok := j.partialResults[clientId].iotaData.smallTable.data[movieId]
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

	return joinedData
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	zetaData := j.partialResults[clientId].zetaData

	identifierFunc := func(input *protocol.Zeta_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Zeta_Data) *protocol.Zeta_Data_Movie {
		return &protocol.Zeta_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
			Title:   input.GetMovie().GetTitle(),
		}
	}

	smallTable := zetaData.smallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.omegaProcessed
	smallTableReady := omegaProcessed && len(smallTable.taskFragments) == zetaData.smallTableTaskCount
	smallTable.ready = smallTableReady

	if smallTableReady {
		bigTable := zetaData.bigTable
		bigTableData := bigTable.data

		partialResults := j.joinZetaData(tasks, bigTableData, clientId)
		j.addEta1Results(tasks, partialResults, clientId)

		bigTable.data = make(map[string][]*protocol.Zeta_Data_Rating)
	}

	return tasks

	// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	// if err != nil {
	// 	log.Errorf("Failed to save %s data: %s", common.ZETA_STAGE, err)
	// }
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)
	iotaData := j.partialResults[clientId].iotaData

	identifierFunc := func(input *protocol.Iota_Data) string {
		return input.GetMovie().GetMovieId()
	}

	mappingFunc := func(input *protocol.Iota_Data) *protocol.Iota_Data_Movie {
		return &protocol.Iota_Data_Movie{
			MovieId: input.GetMovie().GetMovieId(),
		}
	}

	smallTable := iotaData.smallTable

	processSmallTableJoinerStage(smallTable, data, clientId, taskIdentifier, identifierFunc, mappingFunc)

	omegaProcessed := smallTable.omegaProcessed
	smallTableReady := omegaProcessed && len(smallTable.taskFragments) == iotaData.smallTableTaskCount
	smallTable.ready = smallTableReady

	if smallTableReady {
		bigTable := iotaData.bigTable
		bigTableData := bigTable.data

		partialResults := j.joinIotaData(tasks, bigTableData, clientId)
		j.addKappa1Results(tasks, partialResults, clientId)

		bigTable.data = make(map[string][]*protocol.Iota_Data_Actor)
	}

	return tasks

	// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	// if err != nil {
	// 	log.Errorf("Failed to save %s data: %s", common.IOTA_STAGE, err)
	// }
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	zetaData := j.partialResults[clientId].zetaData
	partialData := zetaData.bigTable

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

	if zetaData.smallTable.ready {
		partialResults := j.joinZetaData(tasks, partialData.data, clientId)
		partialData.data = make(map[string][]*protocol.Zeta_Data_Rating)

		j.addEta1Results(tasks, partialResults, clientId)
	} else {
		// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.BIG_TABLE_SOURCE, dataMap)
		// 	if err != nil {
		// 		log.Errorf("Failed to save %s data: %s", common.ZETA_STAGE, err)
		// 	}
	}

	return tasks
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	tasks := make(common.Tasks)

	iotaData := j.partialResults[clientId].iotaData
	partialData := iotaData.bigTable

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

	if iotaData.smallTable.ready {
		partialResults := j.joinIotaData(tasks, partialData.data, clientId)
		partialData.data = make(map[string][]*protocol.Iota_Data_Actor)

		j.addKappa1Results(tasks, partialResults, clientId)
	} else {
		// err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.BIG_TABLE_SOURCE, dataMap)
		// if err != nil {
		// log.Errorf("Failed to save %s data: %s", common.IOTA_STAGE, err)
		// }
	}

	return tasks
}

func (j *Joiner) zetaStage(data []*protocol.Zeta_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	if data == nil {
		return nil
	}

	switch data[0].GetData().(type) {
	case *protocol.Zeta_Data_Movie_:
		return j.moviesZetaStage(data, clientId, taskIdentifier)
	case *protocol.Zeta_Data_Rating_:
		return j.ratingsZetaStage(data, clientId, taskIdentifier)

	default:
		return nil
	}
}

func (j *Joiner) iotaStage(data []*protocol.Iota_Data, clientId string, taskIdentifier *protocol.TaskIdentifier) common.Tasks {
	if len(data) == 0 {
		return nil
	}

	switch data[0].GetData().(type) {
	case *protocol.Iota_Data_Movie_:
		return j.moviesIotaStage(data, clientId, taskIdentifier)
	case *protocol.Iota_Data_Actor_:
		return j.actorsIotaStage(data, clientId, taskIdentifier)
	default:
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
				RoutingKey:  itemHashFunc(infraConfig.GetMapCount(), clientId+common.ETA_STAGE_1),
			},
		}, nil
	case common.IOTA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_1,
				Exchange:    infraConfig.GetMapExchange(),
				WorkerCount: infraConfig.GetMapCount(),
				RoutingKey:  itemHashFunc(infraConfig.GetMapCount(), clientId+common.KAPPA_STAGE_1),
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
	var smallTableReady bool
	// var bigTableReady bool

	taskCount := int(data.GetTasksCount())

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].zetaData

		if zetaData.smallTable.omegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.smallTableTaskCount = taskCount
		zetaData.smallTable.omegaProcessed = true

		smallTableReady = len(zetaData.smallTable.taskFragments) == taskCount
		zetaData.smallTable.ready = smallTableReady

		if smallTableReady {
			partialResults := j.joinZetaData(tasks, zetaData.bigTable.data, clientId)
			j.addEta1Results(tasks, partialResults, clientId)
		}

		// bigTableReady = zetaData.bigTable.ready

	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].iotaData

		if iotaData.smallTable.omegaProcessed {
			log.Infof("OmegaEOF for small table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.smallTableTaskCount = taskCount
		iotaData.smallTable.omegaProcessed = true

		smallTableReady = len(iotaData.smallTable.taskFragments) == taskCount
		iotaData.smallTable.ready = smallTableReady

		if smallTableReady {
			partialResults := j.joinIotaData(tasks, iotaData.bigTable.data, clientId)
			j.addKappa1Results(tasks, partialResults, clientId)
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
		zetaData := j.partialResults[clientId].zetaData

		if zetaData.bigTable.omegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		zetaData.bigTable.omegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].iotaData

		if iotaData.bigTable.omegaProcessed {
			log.Infof("OmegaEOF for big table already processed for clientId: %s", clientId)
			return tasks
		}

		iotaData.bigTable.omegaProcessed = true
		j.eofHandler.HandleOmegaEOF(tasks, data, clientId)
	}

	return tasks
}

func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	switch data.EofType {
	case common.SMALL_TABLE:
		return j.smallTableOmegaEOFStage(data, clientId)
	case common.BIG_TABLE:
		return j.bigTableOmegaEOFStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) ringEOFStage(data *protocol.RingEOF, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	switch data.GetStage() {
	case common.ZETA_STAGE:
		zetaData := j.partialResults[clientId].zetaData
		ringRound := data.GetRoundNumber()

		if zetaData.ringRound >= ringRound {
			log.Infof("RingEOF for Zeta stage already processed for clientId: %s, round: %d", clientId, zetaData.ringRound)
			return tasks
		}

		zetaData.ringRound = ringRound

		participatesInResults := zetaData.sendedFragmentCount > 0
		taskFragments := []common.TaskFragmentIdentifier{}

		if zetaData.smallTable.ready {
			taskFragments = utils.MapKeys(zetaData.bigTable.taskFragments)
		}

		ready := j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, participatesInResults)
		zetaData.bigTable.ready = ready
	case common.IOTA_STAGE:
		iotaData := j.partialResults[clientId].iotaData
		ringRound := data.GetRoundNumber()

		if iotaData.ringRound >= ringRound {
			log.Infof("RingEOF for Iota stage already processed for clientId: %s, round: %d", clientId, iotaData.ringRound)
			return tasks
		}

		iotaData.ringRound = ringRound

		participatesInResults := iotaData.sendedFragmentCount > 0
		taskFragments := []common.TaskFragmentIdentifier{}

		if iotaData.smallTable.ready {
			taskFragments = utils.MapKeys(iotaData.bigTable.taskFragments)
		}

		ready := j.eofHandler.HandleRingEOF(tasks, data, clientId, taskFragments, participatesInResults)
		iotaData.bigTable.ready = ready
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

	j.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data, clientId, taskIdentifier), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return j.omegaEOFStage(data, clientId), nil

	case *protocol.Task_RingEOF:
		return j.ringEOFStage(v.RingEOF, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

func (j *Joiner) addEta1Results(tasks common.Tasks, partialResults []*protocol.Eta_1_Data, clientId string) {
	dataStage := common.ETA_STAGE_1
	zetaData := j.partialResults[clientId].zetaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)
	taskNumber, _ := strconv.Atoi(j.infraConfig.GetNodeId())

	identifierFunc := func(input *protocol.Eta_1_Data) string {
		return input.GetMovieId()
	}

	itemHashFunc := func(workersCount int, item string) string {
		return j.infraConfig.GetBroadcastID()
	}

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

	fragmentCount := zetaData.sendedFragmentCount
	bigTableReady := zetaData.bigTable.ready

	newFragments := AddResults(tasks, partialResults, nextStageData[0], clientId, taskNumber, fragmentCount, bigTableReady, itemHashFunc, identifierFunc, taskDataCreator)
	zetaData.sendedFragmentCount += newFragments
}

func (j *Joiner) addKappa1Results(tasks common.Tasks, partialResults []*protocol.Kappa_1_Data, clientId string) {
	dataStage := common.KAPPA_STAGE_1
	iotaData := j.partialResults[clientId].iotaData

	nextStageData, _ := j.nextStageData(dataStage, clientId)
	taskNumber, _ := strconv.Atoi(j.infraConfig.GetNodeId())

	identifierFunc := func(input *protocol.Kappa_1_Data) string {
		return input.GetMovieId()
	}

	itemHashFunc := func(workersCount int, item string) string {
		return j.infraConfig.GetBroadcastID()
	}

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

	fragmentCount := iotaData.sendedFragmentCount
	bigTableReady := iotaData.bigTable.ready

	newFragments := AddResults(tasks, partialResults, nextStageData[0], clientId, taskNumber, fragmentCount, bigTableReady, itemHashFunc, identifierFunc, taskDataCreator)
	iotaData.sendedFragmentCount += newFragments
}

func processSmallTableJoinerStage[G any, S any](
	partialData JoinerTableData[S],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) S,
) {
	taskID := common.TaskFragmentIdentifier{
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, exists := partialData.taskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.taskFragments[taskID] = struct{}{}

	for _, item := range data {
		id := identifierFunc(item)
		partialData.data[id] = mappingFunc(item)
	}
}

func processBigTableJoinerStage[G any, B any](
	partialData JoinerTableData[[]B],
	data []G,
	clientId string,
	taskIdentifier *protocol.TaskIdentifier,
	identifierFunc func(input G) string,
	mappingFunc func(input G) B,
) {
	taskID := common.TaskFragmentIdentifier{
		TaskNumber:         taskIdentifier.GetTaskNumber(),
		TaskFragmentNumber: taskIdentifier.GetTaskFragmentNumber(),
		LastFragment:       taskIdentifier.GetLastFragment(),
	}

	if _, exists := partialData.taskFragments[taskID]; exists {
		log.Infof("Task fragment %v already processed for clientId: %s", taskID, clientId)
		return
	}

	partialData.taskFragments[taskID] = struct{}{}

	for _, item := range data {
		id := identifierFunc(item)
		if _, exists := partialData.data[id]; !exists {
			partialData.data[id] = make([]B, 0)
		}
		partialData.data[id] = append(partialData.data[id], mappingFunc(item))
	}
}

// // Delete partialResult by clientId
// func (j *Joiner) deletePartialResult(clientId string) {
// 	delete(j.partialResults, clientId)
// 	log.Infof("Deleted partial result for clientId: %s", clientId)
// }

// This function clears the data for the specified stage (zeta or iota) for the given key.
// It does not delete the entire partial result for the key, only the data for the specified stage.
// func (j *Joiner) DeleteStage(clientId string, stage string) {

// 	log.Debugf("Deleting stage: %s for clientId: %s", stage, clientId)

// 	if clientData, ok := j.partialResults[clientId]; ok {
// 		switch stage {
// 		case common.ZETA_STAGE:
// 			clientData.zetaData = JoinerStageData[protocol.Zeta_Data_Movie, protocol.Zeta_Data_Rating]{}
// 		case common.IOTA_STAGE:
// 			clientData.iotaData = JoinerStageData[protocol.Iota_Data_Movie, protocol.Iota_Data_Actor]{}
// 		default:
// 			log.Errorf("Invalid stage: %s", stage)
// 		}
// 	}

// }

// This function clears the data for the specified table type (small or big) for the given key and stage.
// It does not delete the entire partial result for the key, only the data for the specified table type.
// func (j *Joiner) DeleteTableType(clientId, stage, tableType string) {

// 	log.Debugf("Deleting table type: %s for stage: %s and for clientId: %s", tableType, stage, clientId)

// 	if clientData, ok := j.partialResults[clientId]; ok {
// 		switch stage {
// 		case common.ZETA_STAGE:
// 			if tableType == common.SMALL_TABLE {
// 				clientData.zetaData.smallTable = JoinerPartialData[*protocol.Zeta_Data_Movie]{}
// 			} else if tableType == common.BIG_TABLE {
// 				clientData.zetaData.bigTable = JoinerPartialData[[]*protocol.Zeta_Data_Rating]{}
// 			}

// 		case common.IOTA_STAGE:
// 			if tableType == common.SMALL_TABLE {
// 				clientData.iotaData.smallTable = JoinerPartialData[*protocol.Iota_Data_Movie]{}
// 			} else if tableType == common.BIG_TABLE {
// 				clientData.iotaData.bigTable = JoinerPartialData[[]*protocol.Iota_Data_Actor]{}
// 			}

// 		default:
// 			log.Errorf("Invalid stage: %s", stage)
// 		}
// 	}
// }
