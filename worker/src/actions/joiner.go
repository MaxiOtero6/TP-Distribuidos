package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
)

type SmallTablePartialData[T any] struct {
	data  map[string]T
	ready bool
}

type BigTablePartialData[T any] struct {
	data  map[string][]T
	ready bool
}

type StageData[S any, B any] struct {
	smallTable SmallTablePartialData[S]
	bigTable   BigTablePartialData[B]
	generalEOF bool
}

type JoinerPartialResults struct {
	zetaData StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]
	iotaData StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]
}

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*JoinerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
}

func (j *Joiner) makePartialResults(clientId string) {
	if _, ok := j.partialResults[clientId]; ok {
		return
	}

	j.partialResults[clientId] = &JoinerPartialResults{
		zetaData: StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{
			smallTable: SmallTablePartialData[*protocol.Zeta_Data_Movie]{
				data:  make(map[string]*protocol.Zeta_Data_Movie),
				ready: false,
			},
			bigTable: BigTablePartialData[*protocol.Zeta_Data_Rating]{
				data:  make(map[string][]*protocol.Zeta_Data_Rating),
				ready: false,
			},
			generalEOF: false,
		},
		iotaData: StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{
			smallTable: SmallTablePartialData[*protocol.Iota_Data_Movie]{
				data:  make(map[string]*protocol.Iota_Data_Movie),
				ready: false,
			},
			bigTable: BigTablePartialData[*protocol.Iota_Data_Actor]{
				data:  make(map[string][]*protocol.Iota_Data_Actor),
				ready: false,
			},
			generalEOF: false,
		},
	}
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(infraConfig *model.InfraConfig) *Joiner {
	return &Joiner{
		infraConfig:    infraConfig,
		itemHashFunc:   utils.GetWorkerIdFromHash,
		randomHashFunc: utils.RandomHash,
		partialResults: make(map[string]*JoinerPartialResults),
	}
}

func (j *Joiner) joinZetaData(tasks Tasks, ratingsData map[string][]*protocol.Zeta_Data_Rating, clientId string) {
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

	// assign the results to random nodes
	eta1Data := make(map[string][]*protocol.Eta_1_Data)

	for _, data := range joinedData {
		nodeId := utils.RandomHash(j.infraConfig.GetMapCount())

		if _, ok := eta1Data[nodeId]; !ok {
			eta1Data[nodeId] = make([]*protocol.Eta_1_Data, 0)
		}
		eta1Data[nodeId] = append(eta1Data[nodeId], data)
	}

	// create the tasks
	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := ETA_STAGE_1

	tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
	tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)

	for nodeId, data := range eta1Data {
		task := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: data,
				},
			},
		}

		tasks[nextExchange][nextStage][nodeId] = task
	}
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data, clientId string) (tasks Tasks) {
	dataMap := j.partialResults[clientId].zetaData.smallTable.data

	for _, zdata := range data {
		if zdata.GetData().(*protocol.Zeta_Data_Movie_) == nil {
			continue
		}

		movieId := zdata.GetMovie().GetMovieId()
		movieTitle := zdata.GetMovie().GetTitle()

		dataMap[movieId] = &protocol.Zeta_Data_Movie{
			MovieId: movieId,
			Title:   movieTitle,
		}
	}

	err := utils.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, ZETA_STAGE, SMALL_TABLE_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", ZETA_STAGE, err)
	}

	return nil
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data, clientId string) (tasks Tasks) {
	var dataMap map[string][]*protocol.Zeta_Data_Rating
	readyToJoin := j.partialResults[clientId].zetaData.smallTable.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Zeta_Data_Rating)
	} else {
		dataMap = j.partialResults[clientId].zetaData.bigTable.data
	}

	for _, zdata := range data {
		if zdata.GetData().(*protocol.Zeta_Data_Rating_) == nil {
			continue
		}

		movieId := zdata.GetRating().GetMovieId()
		rating := zdata.GetRating().GetRating()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = make([]*protocol.Zeta_Data_Rating, 0)
		}

		dataMap[movieId] = append(dataMap[movieId], &protocol.Zeta_Data_Rating{
			MovieId: movieId,
			Rating:  rating,
		})
	}

	tasks = make(Tasks)

	if readyToJoin {
		j.joinZetaData(tasks, dataMap, clientId)
		return tasks
	} else {
		dataMap = j.partialResults[clientId].zetaData.bigTable.data
		err := utils.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, ZETA_STAGE, BIG_TABLE_SOURCE, dataMap)
		if err != nil {
			log.Errorf("Failed to save %s data: %s", ZETA_STAGE, err)
		}
		return nil
	}
}

/*
zetaStage joins movies and ratings by movieId:

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"eta_1": {
				"": Task
			}
		},
	}
*/
func (j *Joiner) zetaStage(data []*protocol.Zeta_Data, clientId string) (tasks Tasks) {
	if data == nil {
		return nil
	}

	switch data[0].GetData().(type) {
	case *protocol.Zeta_Data_Movie_:
		return j.moviesZetaStage(data, clientId)
	case *protocol.Zeta_Data_Rating_:
		return j.ratingsZetaStage(data, clientId)

	default:
		return nil
	}
}

func (j *Joiner) joinIotaData(tasks Tasks, actorsData map[string][]*protocol.Iota_Data_Actor, clientId string) {
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

	kappa1Data := make(map[string][]*protocol.Kappa_1_Data)

	for _, data := range joinedData {
		nodeId := utils.RandomHash(j.infraConfig.GetMapCount())

		if _, ok := kappa1Data[nodeId]; !ok {
			kappa1Data[nodeId] = make([]*protocol.Kappa_1_Data, 0)
		}
		kappa1Data[nodeId] = append(kappa1Data[nodeId], data)
	}

	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := KAPPA_STAGE_1

	tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
	tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)

	for nodeId, data := range kappa1Data {
		task := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: data,
				},
			},
		}

		tasks[nextExchange][nextStage][nodeId] = task
	}
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data, clientId string) (tasks Tasks) {
	dataMap := j.partialResults[clientId].iotaData.smallTable.data
	for _, idata := range data {
		if idata.GetData().(*protocol.Iota_Data_Movie_) == nil {
			continue
		}

		movieId := idata.GetMovie().GetMovieId()

		dataMap[movieId] = &protocol.Iota_Data_Movie{
			MovieId: movieId,
		}
	}

	err := utils.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, IOTA_STAGE, SMALL_TABLE_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", IOTA_STAGE, err)
	}

	return nil
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data, clientId string) (tasks Tasks) {
	var dataMap map[string][]*protocol.Iota_Data_Actor
	readyToJoin := j.partialResults[clientId].iotaData.smallTable.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Iota_Data_Actor)
	} else {
		dataMap = j.partialResults[clientId].iotaData.bigTable.data
	}

	for _, idata := range data {
		if idata.GetData().(*protocol.Iota_Data_Actor_) == nil {
			continue
		}

		movieId := idata.GetActor().GetMovieId()

		if _, ok := dataMap[movieId]; !ok {
			dataMap[movieId] = make([]*protocol.Iota_Data_Actor, 0)
		}

		dataMap[movieId] = append(dataMap[movieId], &protocol.Iota_Data_Actor{
			MovieId:   movieId,
			ActorId:   idata.GetActor().GetActorId(),
			ActorName: idata.GetActor().GetActorName(),
		})
	}

	tasks = make(Tasks)

	if readyToJoin {
		j.joinIotaData(tasks, dataMap, clientId)
		return tasks
	} else {
		err := utils.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, IOTA_STAGE, BIG_TABLE_SOURCE, dataMap)
		if err != nil {
			log.Errorf("Failed to save %s data: %s", IOTA_STAGE, err)
		}

		return nil
	}

}

/*
iotaStage joins movies and actors by movieId:

This function is nil-safe, meaning it will not panic if the input is nil.
It will simply return a map with empty data.

Return example

	{
		"mapExchange": {
			"kappa_1": {
				"": Task
			}
		},
	}
*/
func (j *Joiner) iotaStage(data []*protocol.Iota_Data, clientId string) (tasks Tasks) {
	if data == nil {
		return nil
	}

	tasks = make(Tasks)

	switch data[0].GetData().(type) {
	case *protocol.Iota_Data_Movie_:
		return j.moviesIotaStage(data, clientId)
	case *protocol.Iota_Data_Actor_:
		return j.actorsIotaStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) getNextStageData(stage string) (string, string, int, error) {
	switch stage {
	case ZETA_STAGE:
		return ETA_STAGE_1, j.infraConfig.GetMapExchange(), j.infraConfig.GetMapCount(), nil
	case IOTA_STAGE:
		return KAPPA_STAGE_1, j.infraConfig.GetMapExchange(), j.infraConfig.GetMapCount(), nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return "", "", 0, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (j *Joiner) getNextNodeId() (string, error) {
	nodeId, err := strconv.Atoi(j.infraConfig.GetNodeId())

	if err != nil {
		return "", fmt.Errorf("failed to convert currentNodeId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (nodeId+1)%j.infraConfig.GetJoinCount())
	return nextNodeId, nil
}

func (j *Joiner) createEofTask(tasks Tasks, eof *protocol.OmegaEOF_Data, ringEof bool, clientId string) {
	if eof.GetWorkerCreatorId() == "" {
		eof.WorkerCreatorId = j.infraConfig.GetNodeId()
	}

	var exchange string
	var stage string
	var nodeId string

	if ringEof {
		nextNodeId, err := j.getNextNodeId()

		if err != nil {
			return
		}

		exchange = j.infraConfig.GetJoinExchange()
		stage = eof.Stage
		nodeId = nextNodeId
	} else {
		nextStage, nextExchange, nextCount, err := j.getNextStageData(eof.Stage)

		if err != nil {
			return
		}

		exchange = nextExchange
		stage = nextStage
		nodeId = utils.RandomHash(nextCount)

		eof.Stage = nextStage
		eof.WorkerCreatorId = ""
	}

	eofTask := &protocol.Task{
		ClientId: clientId,
		Stage: &protocol.Task_OmegaEOF{
			OmegaEOF: &protocol.OmegaEOF{
				Data: eof,
			},
		},
	}

	if _, ok := tasks[exchange]; !ok {
		tasks[exchange] = make(map[string]map[string]*protocol.Task)
	}

	if _, ok := tasks[exchange][stage]; !ok {
		tasks[exchange][stage] = make(map[string]*protocol.Task)
	}

	tasks[exchange][stage][nodeId] = eofTask
}

func (j *Joiner) smallTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		switch data.Stage {
		case ZETA_STAGE:
			if j.partialResults[clientId].zetaData.bigTable.ready && !j.partialResults[clientId].zetaData.generalEOF {
				j.partialResults[clientId].zetaData.generalEOF = true
			} else {
				return nil
			}
		case IOTA_STAGE:
			if j.partialResults[clientId].iotaData.bigTable.ready && !j.partialResults[clientId].iotaData.generalEOF {
				j.partialResults[clientId].iotaData.generalEOF = true
			} else {
				return nil
			}
		default:
			return nil
		}

		generalEof := data
		generalEof.WorkerCreatorId = ""
		generalEof.EofType = GENERAL

		j.createEofTask(tasks, generalEof, true, clientId)
	} else {
		j.createEofTask(tasks, data, true, clientId)

		var bigTableReady bool
		var dataStage string

		switch data.Stage {
		case ZETA_STAGE:
			j.partialResults[clientId].zetaData.smallTable.ready = true
			j.joinZetaData(tasks, j.partialResults[clientId].zetaData.bigTable.data, clientId)

			bigTableReady = j.partialResults[clientId].zetaData.bigTable.ready
			dataStage = data.Stage

		case IOTA_STAGE:
			j.partialResults[clientId].iotaData.smallTable.ready = true
			j.joinIotaData(tasks, j.partialResults[clientId].iotaData.bigTable.data, clientId)

			bigTableReady = j.partialResults[clientId].iotaData.bigTable.ready
			dataStage = data.Stage
		default:
			return nil
		}

		log.Debugf("Big table ready: %v", bigTableReady)
		log.Debugf("Data stage: %s", dataStage)

		if bigTableReady {
			// delete both tables
			if err := utils.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, dataStage, ANY_SOURCE); err != nil {
				log.Errorf("Failed to delete partial results: %s", err)
			}
			j.DeleteStage(clientId, dataStage)

		} else {
			// delete only the big table
			if err := utils.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, dataStage, BIG_TABLE_SOURCE); err != nil {
				log.Errorf("Failed to delete partial results: %s", err)
			}
			j.DeleteTableType(clientId, dataStage, BIG_TABLE_SOURCE)
		}
	}
	return tasks
}

func (j *Joiner) bigTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		switch data.Stage {
		case ZETA_STAGE:
			if j.partialResults[clientId].zetaData.smallTable.ready && !j.partialResults[clientId].zetaData.generalEOF {
				j.partialResults[clientId].zetaData.generalEOF = true
			} else {
				return nil
			}
		case IOTA_STAGE:
			if j.partialResults[clientId].iotaData.smallTable.ready && !j.partialResults[clientId].iotaData.generalEOF {
				j.partialResults[clientId].iotaData.generalEOF = true
			} else {
				return nil
			}
		default:
			return nil
		}

		generalEof := data
		generalEof.WorkerCreatorId = ""
		generalEof.EofType = GENERAL

		j.createEofTask(tasks, generalEof, true, clientId)
	} else {
		j.createEofTask(tasks, data, true, clientId)

		var smallTableReady bool
		var dataStage string

		switch data.Stage {
		case ZETA_STAGE:
			j.partialResults[clientId].zetaData.bigTable.ready = true
			smallTableReady = j.partialResults[clientId].zetaData.smallTable.ready
			dataStage = data.Stage
		case IOTA_STAGE:
			j.partialResults[clientId].iotaData.bigTable.ready = true
			smallTableReady = j.partialResults[clientId].iotaData.smallTable.ready
			dataStage = data.Stage
		default:
			return nil
		}

		if smallTableReady {
			// delete small table
			if err := utils.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, dataStage, ANY_SOURCE); err != nil {
				log.Errorf("Failed to delete partial results: %s", err)
			}

			j.DeleteTableType(clientId, dataStage, SMALL_TABLE_SOURCE)

		}
	}

	return tasks
}

func (j *Joiner) generalOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		data.EofType = ""
		j.createEofTask(tasks, data, false, clientId)

	} else {
		switch data.Stage {
		case ZETA_STAGE:
			if !j.partialResults[clientId].zetaData.generalEOF {
				j.createEofTask(tasks, data, true, clientId)
			} else if j.infraConfig.GetNodeId() < data.GetWorkerCreatorId() {
				j.createEofTask(tasks, data, true, clientId)
				j.partialResults[clientId].zetaData.generalEOF = false
			} else {
				return nil
			}
		case IOTA_STAGE:
			if !j.partialResults[clientId].iotaData.generalEOF {
				j.createEofTask(tasks, data, true, clientId)
			} else if j.infraConfig.GetNodeId() < data.GetWorkerCreatorId() {
				j.createEofTask(tasks, data, true, clientId)
				j.partialResults[clientId].iotaData.generalEOF = false
			} else {
				return nil
			}
		default:
			return nil
		}

		if j.partialResults[clientId].zetaData.generalEOF && j.partialResults[clientId].iotaData.generalEOF {
			j.deletePartialResult(clientId)
		}
	}

	return tasks
}

func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks Tasks) {
	switch data.EofType {
	case SMALL_TABLE:
		return j.smallTableOmegaEOFStage(data, clientId)
	case BIG_TABLE:
		return j.bigTableOmegaEOFStage(data, clientId)
	case GENERAL:
		return j.generalOmegaEOFStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()
	clientId := task.GetClientId()

	j.makePartialResults(clientId)

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data, clientId), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data, clientId), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return j.omegaEOFStage(data, clientId), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}

// Delete partialResult by clientId
func (j *Joiner) deletePartialResult(clientId string) {
	delete(j.partialResults, clientId)
	log.Infof("Deleted partial result for clientId: %s", clientId)
}

// This function clears the data for the specified stage (zeta or iota) for the given key.
// It does not delete the entire partial result for the key, only the data for the specified stage.
func (j *Joiner) DeleteStage(clientId string, stage string) {

	log.Infof("Deleting both tables for clientId: %s and stage: %s", clientId, stage)

	if clientData, ok := j.partialResults[clientId]; ok {
		switch stage {
		case ZETA_STAGE:
			clientData.zetaData = StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{}
		case IOTA_STAGE:
			clientData.iotaData = StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{}
		default:
			log.Errorf("Invalid stage: %s", stage)
		}
	}

}

// This function clears the data for the specified table type (small or big) for the given key and stage.
// It does not delete the entire partial result for the key, only the data for the specified table type.
func (j *Joiner) DeleteTableType(clientId, stage, tableType string) {

	log.Infof("Deleting table type: %s for stage: %s and for clientId: %s", tableType, stage, clientId)

	if clientData, ok := j.partialResults[clientId]; ok {
		switch stage {
		case ZETA_STAGE:
			if tableType == SMALL_TABLE {
				clientData.zetaData.smallTable = SmallTablePartialData[*protocol.Zeta_Data_Movie]{}
			} else if tableType == BIG_TABLE {
				clientData.zetaData.bigTable = BigTablePartialData[*protocol.Zeta_Data_Rating]{}
			}

		case IOTA_STAGE:
			if tableType == SMALL_TABLE {
				clientData.iotaData.smallTable = SmallTablePartialData[*protocol.Iota_Data_Movie]{}
			} else if tableType == BIG_TABLE {
				clientData.iotaData.bigTable = BigTablePartialData[*protocol.Iota_Data_Actor]{}
			}

		default:
			log.Errorf("Invalid stage: %s", stage)
		}
	}
}
