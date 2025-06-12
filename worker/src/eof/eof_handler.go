package eof

// import (
// 	"slices"

// 	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
// 	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
// )

// type EOFHandler struct {
// 	workerID         string
// 	workerType       string
// 	workerCount      int
// 	eofExchange      string
// 	nextWorkerID     string
// 	ignoreDuplicates bool
// 	discartedReadies map[string]map[string][]string
// }

// func NewEOFHandler(
// 	workerID string, workerType string, workerCount int,
// 	eofExchange string, nextWorkerID string,
// ) *EOFHandler {
// 	return &EOFHandler{
// 		workerID:         workerID,
// 		workerType:       workerType,
// 		workerCount:      workerCount,
// 		eofExchange:      eofExchange,
// 		nextWorkerID:     nextWorkerID,
// 		discartedReadies: make(map[string]map[string][]string),
// 	}
// }

// func (h *EOFHandler) IgnoreDuplicates() {
// 	h.ignoreDuplicates = true
// }

// func (h *EOFHandler) InitRing(stage string, eofType string, clientId string) (tasks common.Tasks) {
// 	log.Debugf(
// 		"action: ringEOF | stage: %v | clientId: %v | Initializing RingEOF for stage %s and EOF type %s",
// 		stage, clientId, stage, eofType,
// 	)

// 	tasks = make(common.Tasks)
// 	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
// 	tasks[h.eofExchange][common.RING_STAGE] = make(map[string]*protocol.Task)

// 	tasks[h.eofExchange][common.RING_STAGE][h.workerType+"_"+h.nextWorkerID] = &protocol.Task{
// 		ClientId: clientId,
// 		Stage: &protocol.Task_RingEOF{
// 			RingEOF: &protocol.RingEOF{
// 				Alive:     []string{h.workerID},
// 				Ready:     []string{h.workerID},
// 				Stage:     stage,
// 				EofType:   eofType,
// 				CreatorId: h.workerID,
// 			},
// 		},
// 	}

// 	return tasks
// }

// func (h *EOFHandler) HandleRing(
// 	data *protocol.RingEOF, clientId string,
// 	nextStageFunc func(stage string, clientId string) ([]common.NextStageData, error),
// 	eofStatus bool,
// ) (tasks common.Tasks) {
// 	// Filter to only circulate one RingEOF message
// 	if h.ignoreDuplicates && !(h.workerID <= data.GetCreatorId()) {
// 		log.Debugf("action: ringEOF | stage: %v | clientId: %v | Ignoring RingEOF message from %s", data.GetStage(), clientId, data.GetCreatorId())

// 		if _, exists := h.discartedReadies[clientId]; !exists {
// 			h.discartedReadies[clientId] = make(map[string][]string)
// 		}

// 		h.discartedReadies[clientId][data.GetStage()] = append(h.discartedReadies[clientId][data.GetStage()], data.GetReady()...)
// 		return nil
// 	}

// 	if len(h.discartedReadies[clientId]) > 0 {
// 		log.Debugf("action: ringEOF | stage: %v | clientId: %v | Appending discarded readies: %v", data.GetStage(), clientId, h.discartedReadies[clientId][data.GetStage()])

// 		for _, id := range h.discartedReadies[clientId][data.GetStage()] {
// 			if !slices.Contains(data.GetReady(), id) {
// 				data.Ready = append(data.Ready, id)
// 			}
// 		}

// 		delete(h.discartedReadies[clientId], data.GetStage())

// 		if len(h.discartedReadies[clientId]) == 0 {
// 			delete(h.discartedReadies, clientId)
// 		}

// 		log.Debugf("action: ringEOF | stage: %v | clientId: %v | Readies after appending: %v", data.GetStage(), clientId, data.GetReady())
// 	}

// 	if !slices.Contains(data.Alive, h.workerID) {
// 		log.Debugf("action: ringEOF | stage: %v | clientId: %v | Im not alive, adding myself to the list", data.GetStage(), clientId)
// 		return h.handleSelfNotAlive(data, clientId, eofStatus)
// 	}

// 	if !(len(data.Ready) == h.workerCount) {
// 		log.Debugf("action: ringEOF | stage: %v | clientId: %v | A lap finished, but not all workers are ready %v. Resetting alive list", data.GetStage(), clientId, data.GetReady())
// 		return h.handleAllNotReady(data, clientId)
// 	}

// 	log.Debugf("action: ringEOF | stage: %v | clientId: %v | All workers are ready, sending to next stage", data.GetStage(), clientId)
// 	return h.handleSendToNextStage(data, clientId, nextStageFunc)
// }

// func (h *EOFHandler) handleSelfNotAlive(data *protocol.RingEOF, clientId string, eofStatus bool) common.Tasks {
// 	tasks := make(common.Tasks)

// 	data.Alive = append(data.Alive, h.workerID)

// 	if eofStatus {
// 		if !slices.Contains(data.GetReady(), h.workerID) {
// 			data.Ready = append(data.Ready, h.workerID)
// 		}
// 	}

// 	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
// 	tasks[h.eofExchange][common.RING_STAGE] = make(map[string]*protocol.Task)
// 	tasks[h.eofExchange][common.RING_STAGE][h.workerType+"_"+h.nextWorkerID] = &protocol.Task{
// 		ClientId: clientId,
// 		Stage: &protocol.Task_RingEOF{
// 			RingEOF: data,
// 		},
// 	}

// 	return tasks
// }

// func (h *EOFHandler) handleAllNotReady(data *protocol.RingEOF, clientId string) common.Tasks {
// 	tasks := make(common.Tasks)

// 	data.Alive = []string{h.workerID}

// 	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
// 	tasks[h.eofExchange][common.RING_STAGE] = make(map[string]*protocol.Task)
// 	tasks[h.eofExchange][common.RING_STAGE][h.workerType+"_"+h.nextWorkerID] = &protocol.Task{
// 		ClientId: clientId,
// 		Stage: &protocol.Task_RingEOF{
// 			RingEOF: data,
// 		},
// 	}

// 	return tasks
// }

// func (h *EOFHandler) handleSendToNextStage(
// 	data *protocol.RingEOF, clientId string,
// 	nextStageFunc func(stage string, clientId string) ([]common.NextStageData, error),
// ) common.Tasks {
// 	tasks := make(common.Tasks)

// 	nextDataStages, err := nextStageFunc(data.GetStage(), clientId)
// 	if err != nil {
// 		log.Errorf("action: ringEOF | stage: %v | clientId: %v | Failed to get next stage data: %s", data.GetStage(), clientId, err)
// 		return nil
// 	}

// 	for _, nextDataStage := range nextDataStages {

// 		nextStageEOF := &protocol.Task{
// 			ClientId: clientId,
// 			Stage: &protocol.Task_OmegaEOF{
// 				OmegaEOF: &protocol.OmegaEOF{
// 					Data: &protocol.OmegaEOF_Data{
// 						Stage:   nextDataStage.Stage,
// 						EofType: data.GetEofType(),
// 					},
// 				},
// 			},
// 		}

// 		routingKey := nextDataStage.RoutingKey

// 		if _, exists := tasks[nextDataStage.Exchange]; !exists {
// 			tasks[nextDataStage.Exchange] = make(map[string]map[string]*protocol.Task)
// 		}

// 		if _, exists := tasks[nextDataStage.Exchange][nextDataStage.Stage]; !exists {
// 			tasks[nextDataStage.Exchange][nextDataStage.Stage] = make(map[string]*protocol.Task)
// 		}

// 		tasks[nextDataStage.Exchange][nextDataStage.Stage][routingKey] = nextStageEOF
// 	}

// 	return tasks
// }
