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

type JoinerPartialResults struct {
	zetaMovieData  SmallTablePartialData[*protocol.Zeta_Data_Movie]
	zetaRatingData BigTablePartialData[*protocol.Zeta_Data_Rating]

	iotaMovieData SmallTablePartialData[*protocol.Iota_Data_Movie]
	iotaActorData BigTablePartialData[*protocol.Iota_Data_Actor]
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

func (j *Joiner) joinZetaData(ratingsData map[string][]*protocol.Zeta_Data_Rating) (tasks Tasks) {
	joinedData := make([]*protocol.Eta_1_Data, 0)

	for movieId, ratings := range ratingsData {
		movieData, ok := j.partialResults.zetaMovieData.data[movieId]
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

	return tasks
}

func (j *Joiner) moviesZetaStage(data []*protocol.Zeta_Data) (tasks Tasks) {
	dataMap := j.partialResults.zetaMovieData.data

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
	readyToJoin := j.partialResults.zetaMovieData.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Zeta_Data_Rating)
	} else {
		dataMap = j.partialResults.zetaRatingData.data
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

	if readyToJoin {
		return j.joinZetaData(dataMap)
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

func (j *Joiner) joinIotaData(actorsData map[string][]*protocol.Iota_Data_Actor) (tasks Tasks) {
	joinedData := make([]*protocol.Kappa_1_Data, 0)

	for movieId, actors := range actorsData {
		_, ok := j.partialResults.iotaMovieData.data[movieId]
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

	return tasks
}

func (j *Joiner) moviesIotaStage(data []*protocol.Iota_Data) (tasks Tasks) {
	dataMap := j.partialResults.iotaMovieData.data
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
	readyToJoin := j.partialResults.iotaMovieData.ready

	if readyToJoin {
		dataMap = make(map[string][]*protocol.Iota_Data_Actor)
	} else {
		dataMap = j.partialResults.iotaActorData.data
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

	if readyToJoin {
		return j.joinIotaData(dataMap)
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

func (j *Joiner) getNextNodeId(nodeId string) (string, error) {
	currentNodeId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert currentNodeId to int: %s", err)
	}

	nextNodeId := fmt.Sprintf("%d", (currentNodeId+1)%j.infraConfig.GetJoinCount())
	return nextNodeId, nil
}

/*
 */
func (j *Joiner) omegaEOFStage(data *protocol.OmegaEOF_Data) (tasks Tasks) {
	tasks = make(Tasks)

	// if the creator is the same as the worker, send the EOF to the next stage
	if data.GetWorkerCreatorId() == j.infraConfig.GetNodeId() {
		nextStage, nextExchange, nextStageCount, err := j.getNextStageData(data.GetStage())
		if err != nil {
			log.Errorf("Failed to get next stage data: %s", err)
			return nil
		}

		nextStageEOF := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						ClientId:        data.GetClientId(),
						WorkerCreatorId: "",
						Stage:           nextStage,
					},
				},
			},
		}

		randomNode := utils.RandomHash(nextStageCount)

		tasks[nextExchange] = make(map[string]map[string]*protocol.Task)
		tasks[nextExchange][nextStage] = make(map[string]*protocol.Task)
		tasks[nextExchange][nextStage][randomNode] = nextStageEOF

	} else { // if the creator is not the same as the worker, send the stage results and EOF to the next node
		nextRingEOF := data

		if data.GetWorkerCreatorId() == "" {
			nextRingEOF.WorkerCreatorId = j.infraConfig.GetNodeId()
		}

		eofTask := &protocol.Task{
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: nextRingEOF,
				},
			},
		}

		nextNode, err := j.getNextNodeId(j.infraConfig.GetNodeId())

		if err != nil {
			log.Errorf("Failed to get next node id: %s", err)
			return nil
		}

		reduceExchange := j.infraConfig.GetReduceExchange()
		stage := data.GetStage()

		tasks[reduceExchange] = make(map[string]map[string]*protocol.Task)
		tasks[reduceExchange][stage] = make(map[string]*protocol.Task)
		tasks[reduceExchange][stage][nextNode] = eofTask

		// send the results
		// j.addResultsToNextStage(tasks, stage)
	}
	return tasks
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
