package eof_handler

import (
	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
)

type EOFHandler struct {
	workerID     string
	workerCount  int
	eofExchange  string
	nextWorkerID string
}

func NewEOFHandler(
	workerID string, workerCount int,
	eofExchange string, nextWorkerID string,
) *EOFHandler {
	return &EOFHandler{
		workerID:     workerID,
		workerCount:  workerCount,
		eofExchange:  eofExchange,
		nextWorkerID: nextWorkerID,
	}
}

func (h *EOFHandler) InitRing(stage string, eofType string) (tasks Tasks) {
	tasks = make(Tasks)
	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
	tasks[h.eofExchange][RING_STAGE] = make(map[string]*protocol.Task)

	tasks[h.eofExchange][RING_STAGE][h.nextWorkerID] = &protocol.Task{
		ClientId: h.workerID,
		Stage: &protocol.Task_RingEOF{
			RingEOF: &protocol.RingEOF{
				Alive:   []string{h.workerID},
				Ready:   []string{h.workerID},
				Stage:   stage,
				EofType: eofType,
			},
		},
	}

	return tasks
}

func (h *EOFHandler) HandleRing(
	data *protocol.RingEOF, clientId string,
	nextStageFunc func(stage string, clientId string) ([]NextStageData, error),
	eofStatus bool,
) (tasks Tasks) {
	// Filter to only circulate one RingEOF message
	if h.workerID > data.GetCreatorId() {
		log.Debugf("Ignoring RingEOF message from %s", data.GetCreatorId())
		return nil
	}

	if !slices.Contains(data.Alive, h.workerID) {
		log.Debugf("Im not alive, adding myself to the list")
		return h.handleSelfNotAlive(data, clientId, eofStatus)
	}

	if !(len(data.Ready) == h.workerCount) {
		log.Debugf("A lap finished, but not all workers are ready. Resetting alive list")
		return h.handleAllNotReady(data, clientId)
	}

	log.Debugf("All workers are ready, sending to next stage")
	return h.handleSendToNextStage(data, clientId, nextStageFunc)
}

func (h *EOFHandler) handleSelfNotAlive(data *protocol.RingEOF, clientId string, eofStatus bool) Tasks {
	tasks := make(Tasks)

	data.Alive = append(data.Alive, h.workerID)

	if eofStatus {
		data.Ready = append(data.Ready, h.workerID)
	}

	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
	tasks[h.eofExchange][RING_STAGE] = make(map[string]*protocol.Task)
	tasks[h.eofExchange][RING_STAGE][h.nextWorkerID] = &protocol.Task{
		ClientId: clientId,
		Stage: &protocol.Task_RingEOF{
			RingEOF: data,
		},
	}

	return tasks
}

func (h *EOFHandler) handleAllNotReady(data *protocol.RingEOF, clientId string) Tasks {
	tasks := make(Tasks)

	data.Alive = []string{h.workerID}

	tasks[h.eofExchange] = make(map[string]map[string]*protocol.Task)
	tasks[h.eofExchange][RING_STAGE] = make(map[string]*protocol.Task)
	tasks[h.eofExchange][RING_STAGE][h.nextWorkerID] = &protocol.Task{
		ClientId: clientId,
		Stage: &protocol.Task_RingEOF{
			RingEOF: data,
		},
	}

	return tasks
}

func (h *EOFHandler) handleSendToNextStage(
	data *protocol.RingEOF, clientId string,
	nextStageFunc func(stage string, clientId string) ([]NextStageData, error),
) Tasks {
	tasks := make(Tasks)

	nextDataStages, err := nextStageFunc(data.GetStage(), clientId)
	if err != nil {
		log.Errorf("Failed to get next stage data: %s", err)
		return nil
	}

	for _, nextDataStage := range nextDataStages {

		nextStageEOF := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						Stage:   nextDataStage.Stage,
						EofType: data.GetEofType(),
					},
				},
			},
		}

		routingKey := nextDataStage.RoutingKey

		if _, exists := tasks[nextDataStage.Exchange]; !exists {
			tasks[nextDataStage.Exchange] = make(map[string]map[string]*protocol.Task)
		}

		if _, exists := tasks[nextDataStage.Exchange][nextDataStage.Stage]; !exists {
			tasks[nextDataStage.Exchange][nextDataStage.Stage] = make(map[string]*protocol.Task)
		}

		tasks[nextDataStage.Exchange][nextDataStage.Stage][routingKey] = nextStageEOF
	}

	return tasks
}
