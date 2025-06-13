package eof

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
)

type StatefulEofHandler struct {
	workerId      string
	NextStageFunc func(stage string, clientId string) ([]common.NextStageData, error)
}

func NewStatefulEofHandler(
	workerId string,
	nextStageFunc func(stage string, clientId string) ([]common.NextStageData, error),
) *StatefulEofHandler {
	return &StatefulEofHandler{
		workerId:      workerId,
		NextStageFunc: nextStageFunc,
	}
}

func (h *StatefulEofHandler) nextWorkerRing(previousRingEof *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	nextStagesData, err := h.NextStageFunc(common.RING_STAGE, clientId)
	if err != nil {
		return tasks
	}

	for _, nextStageData := range nextStagesData {
		nextStageRing := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_RingEOF{
				RingEOF: &protocol.RingEOF{
					Stage:         previousRingEof.GetStage(),
					EofType:       previousRingEof.GetEofType(),
					Alive:         previousRingEof.GetAlive(),
					Ready:         previousRingEof.GetReady(),
					TaskCount:     previousRingEof.GetTaskCount(),
					TaskFragments: previousRingEof.GetTaskFragments(),
				},
			},
		}

		if _, exists := tasks[nextStageData.Exchange]; !exists {
			tasks[nextStageData.Exchange] = make(map[string]map[string]*protocol.Task)
		}
		if _, exists := tasks[nextStageData.Exchange][nextStageData.Stage]; !exists {
			tasks[nextStageData.Exchange][nextStageData.Stage] = make(map[string]*protocol.Task)
		}
		tasks[nextStageData.Exchange][nextStageData.Stage][nextStageData.RoutingKey] = nextStageRing
	}

	return tasks
}

func (h *StatefulEofHandler) initRingEof(eofData *protocol.OmegaEOF_Data) *protocol.RingEOF {
	return &protocol.RingEOF{
		Stage:         eofData.GetStage(),
		EofType:       eofData.GetEofType(),
		CreatorId:     h.workerId,
		Alive:         []string{},
		Ready:         []string{},
		TaskCount:     eofData.GetTasksCount(),
		TaskFragments: []*protocol.TaskFragments{},
	}
}

func (h *StatefulEofHandler) HandleOmegaEOF(eofData *protocol.OmegaEOF_Data, clientId string) common.Tasks {
	ringEof := h.initRingEof(eofData)
	return h.nextWorkerRing(ringEof, clientId)
}

func (h *StatefulEofHandler) HandleRingEOF(eofData *protocol.RingEOF, clientId string) common.Tasks {
	tasks := make(common.Tasks)

	stage := eofData.Stage
	nextStagesData, err := h.NextStageFunc(stage, clientId)
	if err != nil {
		return tasks
	}

	for _, nextStageData := range nextStagesData {
		nextStageEOF := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_RingEOF{
				RingEOF: &protocol.RingEOF{
					Stage:   stage,
					EofType: eofData.EofType,
				},
			},
		}

		if _, exists := tasks[nextStageData.Exchange]; !exists {
			tasks[nextStageData.Exchange] = make(map[string]map[string]*protocol.Task)
		}

		if _, exists := tasks[nextStageData.Exchange][nextStageData.Stage]; !exists {
			tasks[nextStageData.Exchange][nextStageData.Stage] = make(map[string]*protocol.Task)
		}

		tasks[nextStageData.Exchange][nextStageData.Stage][nextStageData.RoutingKey] = nextStageEOF
	}

	return tasks
}
