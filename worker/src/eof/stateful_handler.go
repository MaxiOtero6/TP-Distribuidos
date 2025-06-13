package eof

import (
	"sort"

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
					Stage:           previousRingEof.GetStage(),
					EofType:         previousRingEof.GetEofType(),
					TaskCount:       previousRingEof.GetTaskCount(),
					StageFragmentes: previousRingEof.GetStageFragmentes(),
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
		Stage:           eofData.GetStage(),
		EofType:         eofData.GetEofType(),
		CreatorId:       h.workerId,
		TaskCount:       eofData.GetTasksCount(),
		StageFragmentes: []*protocol.StageFragment{},
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

func mergeStageFragments(stageFragments []*protocol.StageFragment, taskFragments []*protocol.TaskIdentifier) []*protocol.StageFragment {
	for _, taskFragment := range taskFragments {
		stageFragments = append(stageFragments, &protocol.StageFragment{
			Start: &protocol.FragmentIdentifier{
				TaskNumber:         taskFragment.TaskNumber,
				TaskFragmentNumber: taskFragment.TaskFragmentNumber,
			},
			LastFragment: taskFragment.LastFragment,
		})
	}

	// Sort stage fragments by TaskNumber and TaskFragmentNumber
	sort.Slice(stageFragments, func(i, j int) bool {
		if stageFragments[i].Start.GetTaskNumber() != stageFragments[j].Start.GetTaskNumber() {
			return stageFragments[i].Start.GetTaskNumber() < stageFragments[j].Start.GetTaskNumber()
		}
		return stageFragments[i].Start.GetTaskFragmentNumber() < stageFragments[j].Start.GetTaskFragmentNumber()
	})

	// Merge consecutive fragments iteratively
	mergedFragments := []*protocol.StageFragment{}
	i := 0
	for i < len(stageFragments) {
		// Add a new fragment to the merged list
		current := stageFragments[i]
		i++
		for i < len(stageFragments) {
			// Check if the next fragment can be merged with the current one
			next := stageFragments[i]

			if current.End.GetTaskNumber() == next.Start.GetTaskNumber() {
				// If the next fragment starts with the same TaskNumber
				if current.End.GetTaskFragmentNumber()+1 != next.Start.GetTaskFragmentNumber() {
					// If the next fragment does not continue the current one
					break
				}
			} else if current.LastFragment || current.End.GetTaskNumber()+1 != next.Start.GetTaskNumber() {
				// If the current fragment is not the last fragment or the next task number is not consecutive
				break
			}

			current.End = next.End
			current.LastFragment = next.LastFragment
			i++
		}
		mergedFragments = append(mergedFragments, current)
	}

	return mergedFragments
}

func isStageReady(stageFragments []*protocol.StageFragment, taskCount uint32) bool {
	if len(stageFragments) == 0 {
		return taskCount == 0
	}

	if len(stageFragments) != 1 {
		return false
	}

	if stageFragments[0].Start.GetTaskNumber() != 0 || stageFragments[0].Start.GetTaskFragmentNumber() != 0 {
		return false
	}

	if stageFragments[0].End.GetTaskNumber() != taskCount-1 || !stageFragments[0].LastFragment {
		return false
	}

	return true
}
