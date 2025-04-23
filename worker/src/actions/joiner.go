package actions

import (
	"fmt"
	"strconv"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/server-comm/protocol"
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
	partialResults *JoinerPartialResults
}

// NewJoiner creates a new Joiner instance.
func NewJoiner(infraConfig *model.InfraConfig) *Joiner {
	return &Joiner{
		infraConfig: infraConfig,
	}
}

func (j *Joiner) joinZetaData(tasks Tasks, ratingsData map[string][]*protocol.Zeta_Data_Rating) {
	joinedData := make([]*protocol.Eta_1_Data, 0)

	for movieId, ratings := range ratingsData {
		movieData, ok := j.partialResults.zetaData.smallTable.data[movieId]
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
	tasks = make(Tasks)
	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := ETA_STAGE_1

	tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
	tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)

	for nodeId, data := range eta1Data {
		task := &protocol.Task{
			Stage: &protocol.Task_Eta_1{
				Eta_1: &protocol.Eta_1{
					Data: data,
				},
			},
		}

		tasks[nextExchange][nextStage][nodeId] = task
	}
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data) (tasks Tasks) {
	dataMap := j.partialResults.zetaData.smallTable.data

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

	return nil
}

func (j *Joiner) ratingsZetaStage(data []*protocol.Zeta_Data) (tasks Tasks) {
	var dataMap map[string][]*protocol.Zeta_Data_Rating
	readyToJoin := j.partialResults.zetaData.smallTable.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Zeta_Data_Rating)
	} else {
		dataMap = j.partialResults.zetaData.bigTable.data
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
		j.joinZetaData(tasks, dataMap)
		return tasks
	} else {
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
func (j *Joiner) zetaStage(data []*protocol.Zeta_Data) (tasks Tasks) {
	if data == nil {
		return nil
	}

	switch data[0].GetData().(type) {
	case *protocol.Zeta_Data_Movie_:
		return j.moviesZetaStage(data)
	case *protocol.Zeta_Data_Rating_:
		return j.ratingsZetaStage(data)
	default:
		return nil
	}
}

func (j *Joiner) joinIotaData(tasks Tasks, actorsData map[string][]*protocol.Iota_Data_Actor) {
	joinedData := make([]*protocol.Kappa_1_Data, 0)

	for movieId, actors := range actorsData {
		_, ok := j.partialResults.iotaData.smallTable.data[movieId]
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

	tasks = make(Tasks)
	nextExchange := j.infraConfig.GetMapExchange()
	nextStage := KAPPA_STAGE_1

	tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
	tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)

	for nodeId, data := range kappa1Data {
		task := &protocol.Task{
			Stage: &protocol.Task_Kappa_1{
				Kappa_1: &protocol.Kappa_1{
					Data: data,
				},
			},
		}

		tasks[nextExchange][nextStage][nodeId] = task
	}
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data) (tasks Tasks) {
	dataMap := j.partialResults.iotaData.smallTable.data
	for _, idata := range data {
		if idata.GetData().(*protocol.Iota_Data_Movie_) == nil {
			continue
		}

		movieId := idata.GetMovie().GetMovieId()

		dataMap[movieId] = &protocol.Iota_Data_Movie{
			MovieId: movieId,
		}
	}
	return nil
}

func (j *Joiner) actorsIotaStage(data []*protocol.Iota_Data) (tasks Tasks) {
	var dataMap map[string][]*protocol.Iota_Data_Actor
	readyToJoin := j.partialResults.iotaData.smallTable.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Iota_Data_Actor)
	} else {
		dataMap = j.partialResults.iotaData.bigTable.data
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
		j.joinIotaData(tasks, dataMap)
		return tasks
	} else {
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
func (j *Joiner) iotaStage(data []*protocol.Iota_Data) (tasks Tasks) {
	if data == nil {
		return nil
	}

	switch data[0].GetData().(type) {
	case *protocol.Iota_Data_Movie_:
		return j.moviesIotaStage(data)
	case *protocol.Iota_Data_Actor_:
		return j.actorsIotaStage(data)
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

func (j *Joiner) createEofTask(tasks Tasks, eof *protocol.OmegaEOF_Data, ringEof bool) {
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
	}

	eofTask := &protocol.Task{
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

func (j *Joiner) smallTableOmegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		switch data.Stage {
		case ZETA_STAGE:
			if j.partialResults.zetaData.bigTable.ready && !j.partialResults.zetaData.generalEOF {
				j.partialResults.zetaData.generalEOF = true
			} else {
				return nil
			}
		case IOTA_STAGE:
			if j.partialResults.iotaData.bigTable.ready && !j.partialResults.iotaData.generalEOF {
				j.partialResults.iotaData.generalEOF = true
			} else {
				return nil
			}
		default:
			return nil
		}

		generalEof := data
		generalEof.WorkerCreatorId = ""
		generalEof.EofType = GENERAL

		j.createEofTask(tasks, generalEof, true)
	} else {
		j.createEofTask(tasks, data, true)

		switch data.Stage {
		case ZETA_STAGE:
			j.partialResults.zetaData.smallTable.ready = true
			j.joinZetaData(tasks, j.partialResults.zetaData.bigTable.data)
		case IOTA_STAGE:
			j.partialResults.iotaData.smallTable.ready = true
			j.joinIotaData(tasks, j.partialResults.iotaData.bigTable.data)
		default:
			return nil
		}
	}

	return tasks
}

func (j *Joiner) bigTableOmegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		switch data.Stage {
		case ZETA_STAGE:
			if j.partialResults.zetaData.smallTable.ready && !j.partialResults.zetaData.generalEOF {
				j.partialResults.zetaData.generalEOF = true
			} else {
				return nil
			}
		case IOTA_STAGE:
			if j.partialResults.iotaData.smallTable.ready && !j.partialResults.iotaData.generalEOF {
				j.partialResults.iotaData.generalEOF = true
			} else {
				return nil
			}
		default:
			return nil
		}

		generalEof := data
		generalEof.WorkerCreatorId = ""
		generalEof.EofType = GENERAL

		j.createEofTask(tasks, generalEof, true)
	} else {
		j.createEofTask(tasks, data, true)

		switch data.Stage {
		case ZETA_STAGE:
			j.partialResults.zetaData.bigTable.ready = true
		case IOTA_STAGE:
			j.partialResults.iotaData.bigTable.ready = true
		default:
			return nil
		}
	}

	return tasks
}

func (j *Joiner) generalOmegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		data.EofType = ""
		j.createEofTask(tasks, data, true)
	} else {
		switch data.Stage {
		case ZETA_STAGE:
			if !j.partialResults.zetaData.generalEOF {
				j.createEofTask(tasks, data, true)
			} else if j.infraConfig.GetNodeId() < data.GetWorkerCreatorId() {
				j.createEofTask(tasks, data, true)
				j.partialResults.zetaData.generalEOF = false
			} else {
				return nil
			}
		case IOTA_STAGE:
			if !j.partialResults.iotaData.generalEOF {
				j.createEofTask(tasks, data, true)
			} else if j.infraConfig.GetNodeId() < data.GetWorkerCreatorId() {
				j.createEofTask(tasks, data, true)
				j.partialResults.iotaData.generalEOF = false
			} else {
				return nil
			}
		default:
			return nil
		}

		j.createEofTask(tasks, data, true)
	}

	return tasks
}

/*
 */
func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	switch data.EofType {
	case SMALL_TABLE:
		return j.smallTableOmegaEOFStage(data)
	case BIG_TABLE:
		return j.bigTableOmegaEOFStage(data)
	case GENERAL:
		return j.generalOmegaEOFStage(data)
	default:
		return nil
	}
}

func (j *Joiner) Execute(task *protocol.Task) (Tasks, error) {
	stage := task.GetStage()

	switch v := stage.(type) {
	case *protocol.Task_Zeta:
		data := v.Zeta.GetData()
		return j.zetaStage(data), nil

	case *protocol.Task_Iota:
		data := v.Iota.GetData()
		return j.iotaStage(data), nil

	case *protocol.Task_OmegaEOF:
		data := v.OmegaEOF.GetData()
		return j.omegaEOFStage(data), nil

	default:
		return nil, fmt.Errorf("invalid query stage: %v", v)
	}
}
