package actions

import (
	"fmt"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/common/utils"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/utils/storage"

	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/eof_handler"
)

// Joiner is a struct that implements the Action interface.
type Joiner struct {
	infraConfig    *model.InfraConfig
	partialResults map[string]*common.JoinerPartialResults
	itemHashFunc   func(workersCount int, item string) string
	randomHashFunc func(workersCount int) string
	eofHandler     eof_handler.IEOFHandler
}

func (j *Joiner) makePartialResults(clientId string) {
	if _, ok := j.partialResults[clientId]; ok {
		return
	}

	j.partialResults[clientId] = &common.JoinerPartialResults{
		ZetaData: common.StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{
			SmallTable: common.PartialData[*protocol.Zeta_Data_Movie]{
				Data:  make(map[string]*protocol.Zeta_Data_Movie),
				Ready: false,
			},
			BigTable: common.PartialData[[]*protocol.Zeta_Data_Rating]{
				Data:  make(map[string][]*protocol.Zeta_Data_Rating),
				Ready: false,
			},
			Ready: false,
		},
		IotaData: common.StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{
			SmallTable: common.PartialData[*protocol.Iota_Data_Movie]{
				Data:  make(map[string]*protocol.Iota_Data_Movie),
				Ready: false,
			},
			BigTable: common.PartialData[[]*protocol.Iota_Data_Actor]{
				Data:  make(map[string][]*protocol.Iota_Data_Actor),
				Ready: false,
			},
			Ready: false,
		},
	}
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(infraConfig *model.InfraConfig, eofHandler eof_handler.IEOFHandler) *Joiner {
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

func (j *Joiner) joinZetaData(tasks common.Tasks, ratingsData map[string][]*protocol.Zeta_Data_Rating, clientId string) {
	joinedData := make([]*protocol.Eta_1_Data, 0)

	for movieId, ratings := range ratingsData {
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

	// assign the results to random nodes
	eta1Data := make(map[string][]*protocol.Eta_1_Data)

	for _, data := range joinedData {
		// nodeId := utils.RandomHash(j.infraConfig.GetMapCount())

		if _, ok := eta1Data[""]; !ok {
			eta1Data[""] = make([]*protocol.Eta_1_Data, 0)
		}
		eta1Data[""] = append(eta1Data[""], data)
	}

	// create the tasks
	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := common.ETA_STAGE_1

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

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data, clientId string) (tasks common.Tasks) {
	dataMap := j.partialResults[clientId].ZetaData.SmallTable.Data

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

	err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.ZETA_STAGE, err)
	}

	return nil
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data, clientId string) (tasks common.Tasks) {
	var dataMap map[string][]*protocol.Zeta_Data_Rating
	readyToJoin := j.partialResults[clientId].ZetaData.SmallTable.Ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Zeta_Data_Rating)
	} else {
		dataMap = j.partialResults[clientId].ZetaData.BigTable.Data
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

	tasks = make(common.Tasks)

	if readyToJoin {
		j.joinZetaData(tasks, dataMap, clientId)
		return tasks
	} else {
		dataMap = j.partialResults[clientId].ZetaData.BigTable.Data
		err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.ZETA_STAGE, common.BIG_TABLE_SOURCE, dataMap)
		if err != nil {
			log.Errorf("Failed to save %s data: %s", common.ZETA_STAGE, err)
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
func (j *Joiner) zetaStage(data []*protocol.Zeta_Data, clientId string) (tasks common.Tasks) {
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

func (j *Joiner) joinIotaData(tasks common.Tasks, actorsData map[string][]*protocol.Iota_Data_Actor, clientId string) {
	joinedData := make([]*protocol.Kappa_1_Data, 0)

	for movieId, actors := range actorsData {
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

	kappa1Data := make(map[string][]*protocol.Kappa_1_Data)

	for _, data := range joinedData {
		// nodeId := utils.RandomHash(j.infraConfig.GetMapCount())

		if _, ok := kappa1Data[""]; !ok {
			kappa1Data[""] = make([]*protocol.Kappa_1_Data, 0)
		}
		kappa1Data[""] = append(kappa1Data[""], data)
	}

	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := common.KAPPA_STAGE_1

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

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data, clientId string) (tasks common.Tasks) {
	dataMap := j.partialResults[clientId].IotaData.SmallTable.Data
	for _, idata := range data {
		if idata.GetData().(*protocol.Iota_Data_Movie_) == nil {
			continue
		}

		movieId := idata.GetMovie().GetMovieId()

		dataMap[movieId] = &protocol.Iota_Data_Movie{
			MovieId: movieId,
		}
	}

	err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.SMALL_TABLE_SOURCE, dataMap)
	if err != nil {
		log.Errorf("Failed to save %s data: %s", common.IOTA_STAGE, err)
	}

	return nil
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data, clientId string) (tasks common.Tasks) {
	var dataMap map[string][]*protocol.Iota_Data_Actor
	readyToJoin := j.partialResults[clientId].IotaData.SmallTable.Ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Iota_Data_Actor)
	} else {
		dataMap = j.partialResults[clientId].IotaData.BigTable.Data
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

	tasks = make(common.Tasks)

	if readyToJoin {
		j.joinIotaData(tasks, dataMap, clientId)
		return tasks
	} else {
		err := storage.SaveDataToFile(j.infraConfig.GetDirectory(), clientId, common.IOTA_STAGE, common.BIG_TABLE_SOURCE, dataMap)
		if err != nil {
			log.Errorf("Failed to save %s data: %s", common.IOTA_STAGE, err)
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
func (j *Joiner) iotaStage(data []*protocol.Iota_Data, clientId string) (tasks common.Tasks) {
	if data == nil {
		return nil
	}

	tasks = make(common.Tasks)

	switch data[0].GetData().(type) {
	case *protocol.Iota_Data_Movie_:
		return j.moviesIotaStage(data, clientId)
	case *protocol.Iota_Data_Actor_:
		return j.actorsIotaStage(data, clientId)
	default:
		return nil
	}
}

func (j *Joiner) getNextStageData(stage string, clientId string) ([]common.NextStageData, error) {
	switch stage {
	case common.ZETA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.ETA_STAGE_1,
				Exchange:    j.infraConfig.GetMapExchange(),
				WorkerCount: j.infraConfig.GetMapCount(),
				RoutingKey:  j.infraConfig.GetBroadcastID(),
			},
		}, nil
	case common.IOTA_STAGE:
		return []common.NextStageData{
			{
				Stage:       common.KAPPA_STAGE_1,
				Exchange:    j.infraConfig.GetMapExchange(),
				WorkerCount: j.infraConfig.GetMapCount(),
				RoutingKey:  j.infraConfig.GetBroadcastID(),
			},
		}, nil
	default:
		log.Errorf("Invalid stage: %s", stage)
		return []common.NextStageData{}, fmt.Errorf("invalid stage: %s", stage)
	}
}

func (j *Joiner) smallTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)
	var bigTableReady bool
	var dataStage string

	switch data.Stage {
	case common.ZETA_STAGE:
		if j.partialResults[clientId].ZetaData.BigTable.Ready {
			j.partialResults[clientId].ZetaData.Ready = true
			tasks = j.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)
		}

		j.partialResults[clientId].ZetaData.SmallTable.Ready = true
		j.joinZetaData(tasks, j.partialResults[clientId].ZetaData.BigTable.Data, clientId)

		bigTableReady = j.partialResults[clientId].ZetaData.BigTable.Ready
		dataStage = data.Stage

	case common.IOTA_STAGE:
		if j.partialResults[clientId].IotaData.BigTable.Ready {
			j.partialResults[clientId].IotaData.Ready = true
			tasks = j.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)
		}

		j.partialResults[clientId].IotaData.SmallTable.Ready = true
		j.joinIotaData(tasks, j.partialResults[clientId].IotaData.BigTable.Data, clientId)

		bigTableReady = j.partialResults[clientId].IotaData.BigTable.Ready
		dataStage = data.Stage
	default:
		return nil
	}

	log.Debugf("Big table ready: %v", bigTableReady)
	log.Debugf("Data stage: %s", dataStage)

	// delete only the big table
	if err := storage.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, dataStage, common.BIG_TABLE_SOURCE); err != nil {
		log.Errorf("Failed to delete partial results: %s", err)
	}
	j.DeleteTableType(clientId, dataStage, common.BIG_TABLE_SOURCE)

	return tasks
}

func (j *Joiner) bigTableOmegaEOFStage(data *protocol.OmegaEOF_Data, clientId string) (tasks common.Tasks) {
	tasks = make(common.Tasks)

	switch data.Stage {
	case common.ZETA_STAGE:
		if j.partialResults[clientId].ZetaData.SmallTable.Ready {
			j.partialResults[clientId].ZetaData.Ready = true
			tasks = j.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)
		}

		j.partialResults[clientId].ZetaData.BigTable.Ready = true
	case common.IOTA_STAGE:
		if j.partialResults[clientId].IotaData.SmallTable.Ready {
			j.partialResults[clientId].IotaData.Ready = true
			tasks = j.eofHandler.InitRing(data.GetStage(), data.GetEofType(), clientId)
		}

		j.partialResults[clientId].IotaData.BigTable.Ready = true
	default:
		return nil
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
	var ready bool

	switch data.Stage {
	case common.ZETA_STAGE:
		ready = j.partialResults[clientId].ZetaData.Ready
	case common.IOTA_STAGE:
		ready = j.partialResults[clientId].IotaData.Ready
	}

	if ready {
		if err := storage.DeletePartialResults(j.infraConfig.GetDirectory(), clientId, data.GetStage(), common.ANY_SOURCE); err != nil {
			log.Errorf("Failed to delete partial results: %s", err)
		}

		j.DeleteStage(clientId, data.GetStage())

		if len(j.partialResults[clientId].ZetaData.SmallTable.Data) == 0 &&
			len(j.partialResults[clientId].IotaData.SmallTable.Data) == 0 &&
			len(j.partialResults[clientId].ZetaData.BigTable.Data) == 0 &&
			len(j.partialResults[clientId].IotaData.BigTable.Data) == 0 {
			j.deletePartialResult(clientId)
		}
	}

	return j.eofHandler.HandleRing(data, clientId, j.getNextStageData, ready)
}

func (j *Joiner) Execute(task *protocol.Task) (common.Tasks, error) {
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

	case *protocol.Task_RingEOF:
		return j.ringEOFStage(v.RingEOF, clientId), nil

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

	log.Debugf("Deleting stage: %s for clientId: %s", stage, clientId)

	if clientData, ok := j.partialResults[clientId]; ok {
		switch stage {
		case common.ZETA_STAGE:
			clientData.ZetaData = common.StageData[*protocol.Zeta_Data_Movie, *protocol.Zeta_Data_Rating]{}
		case common.IOTA_STAGE:
			clientData.IotaData = common.StageData[*protocol.Iota_Data_Movie, *protocol.Iota_Data_Actor]{}
		default:
			log.Errorf("Invalid stage: %s", stage)
		}
	}

}

// This function clears the data for the specified table type (small or big) for the given key and stage.
// It does not delete the entire partial result for the key, only the data for the specified table type.
func (j *Joiner) DeleteTableType(clientId, stage, tableType string) {

	log.Debugf("Deleting table type: %s for stage: %s and for clientId: %s", tableType, stage, clientId)

	if clientData, ok := j.partialResults[clientId]; ok {
		switch stage {
		case common.ZETA_STAGE:
			if tableType == common.SMALL_TABLE {
				clientData.ZetaData.SmallTable = common.PartialData[*protocol.Zeta_Data_Movie]{}
			} else if tableType == common.BIG_TABLE {
				clientData.ZetaData.BigTable = common.PartialData[[]*protocol.Zeta_Data_Rating]{}
			}

		case common.IOTA_STAGE:
			if tableType == common.SMALL_TABLE {
				clientData.IotaData.SmallTable = common.PartialData[*protocol.Iota_Data_Movie]{}
			} else if tableType == common.BIG_TABLE {
				clientData.IotaData.BigTable = common.PartialData[[]*protocol.Iota_Data_Actor]{}
			}

		default:
			log.Errorf("Invalid stage: %s", stage)
		}
	}
}
