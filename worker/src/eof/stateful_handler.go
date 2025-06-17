package eof

import (
	"sort"

	"slices"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
)

type StatefulEofHandler struct {
	workerType    model.ActionType
	nodeId        string
	infraConfig   *model.InfraConfig
	nextStageFunc func(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error)
	itemHashFunc  func(workersCount int, item string) string
}

func NewStatefulEofHandler(
	workerType model.ActionType,
	infraConfig *model.InfraConfig,
	nextStageFunc func(stage string, clientId string, infraConfig *model.InfraConfig, itemHashFunc func(workersCount int, item string) string) ([]common.NextStageData, error),
	itemHashFunc func(workersCount int, item string) string,
) *StatefulEofHandler {
	nodeId := infraConfig.GetNodeId()

	return &StatefulEofHandler{
		workerType,
		nodeId,
		infraConfig,
		nextStageFunc,
		itemHashFunc,
	}
}

func (h *StatefulEofHandler) nextWorkerRing(tasks common.Tasks, previousRingEOF *protocol.RingEOF, clientId string, parking bool) {
	nextStagesData, err := h.nextStageFunc(common.RING_STAGE, clientId, h.infraConfig, h.itemHashFunc)
	if err != nil {
		return
	}

	for _, nextStageData := range nextStagesData {
		nextStageRing := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_RingEOF{
				RingEOF: previousRingEOF,
			},
		}

		exchange := nextStageData.Exchange
		routingKey := nextStageData.RoutingKey
		if parking {
			exchange = h.infraConfig.GetParkingEOFExchange()
			routingKey = string(h.workerType) + "_" + nextStageData.RoutingKey
		}

		if _, exists := tasks[exchange]; !exists {
			tasks[exchange] = make(map[string]map[string]*protocol.Task)
		}
		if _, exists := tasks[exchange][nextStageData.Stage]; !exists {
			tasks[exchange][nextStageData.Stage] = make(map[string]*protocol.Task)
		}
		tasks[exchange][nextStageData.Stage][routingKey] = nextStageRing
	}
}

func (h *StatefulEofHandler) initRingEof(omegaEOFData *protocol.OmegaEOF_Data) *protocol.RingEOF {
	return &protocol.RingEOF{
		Stage:                         omegaEOFData.GetStage(),
		EofType:                       omegaEOFData.GetEofType(),
		CreatorId:                     h.nodeId,
		ReadyId:                       "",
		TasksCount:                    omegaEOFData.GetTasksCount(),
		RoundNumber:                   1, // Start with round 1 so it does not conflict with the default round 0
		StageFragmentes:               []*protocol.StageFragment{},
		NextStageWorkerParticipantIds: []string{},
	}
}

func (h *StatefulEofHandler) nextStagesOmegaEOF(tasks common.Tasks, rinfEOF *protocol.RingEOF, clientId string) {
	nextStagesData, err := h.nextStageFunc(rinfEOF.GetStage(), clientId, h.infraConfig, h.itemHashFunc)
	if err != nil {
		return
	}
	for _, nextStageData := range nextStagesData {
		nextStageOmegaEof := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						Stage:      nextStageData.Stage,
						EofType:    rinfEOF.GetEofType(),
						TasksCount: uint32(len(rinfEOF.GetNextStageWorkerParticipantIds())),
					},
				},
			},
		}
		if _, exists := tasks[nextStageData.Exchange]; !exists {
			tasks[nextStageData.Exchange] = make(map[string]map[string]*protocol.Task)
		}
		if _, exists := tasks[nextStageData.Exchange][nextStageData.Stage]; !exists {
			tasks[nextStageData.Exchange][nextStageData.Stage] = make(map[string]*protocol.Task)
		}
		tasks[nextStageData.Exchange][nextStageData.Stage][nextStageData.RoutingKey] = nextStageOmegaEof
	}
}

func (h *StatefulEofHandler) HandleOmegaEOF(tasks common.Tasks, omegaEOFData *protocol.OmegaEOF_Data, clientId string) {
	ringEof := h.initRingEof(omegaEOFData)

	// if participatesInResults {
	// 	h.addToWorkerParticipantIds(ringEof)
	// }

	// h.mergeStageFragments(ringEof, workerFragments)

	h.nextWorkerRing(tasks, ringEof, clientId, false)
}

func (h *StatefulEofHandler) HandleRingEOF(tasks common.Tasks, ringEOF *protocol.RingEOF, clientId string, workerFragments []common.TaskFragmentIdentifier, participatesInResults bool) bool {
	if ringEOF.GetCreatorId() == h.nodeId {
		// If the RingEOF is created by this worker, we advaance the round
		ringEOF.RoundNumber++
	}

	if participatesInResults {
		h.addToWorkerParticipantIds(ringEOF)
	}

	if ringEOF.GetReadyId() == "" {

		// If the EOF is not ready yet, we merge the fragments and check if the stage is ready
		h.mergeStageFragments(ringEOF, workerFragments)

		if isStageReady(ringEOF) {
			// If the stage is ready, we set the ReadyId to this worker and return the next stage EOF
			ringEOF.ReadyId = h.nodeId
			h.nextWorkerRing(tasks, ringEOF, clientId, false)
			return true
		} else {

			if ringEOF.GetCreatorId() == h.nodeId {
				// If the EOF is not ready and it does a full cycle, we wait by sending to a delay exchange and with the dead letter exchange send it back to the workers
				h.nextWorkerRing(tasks, ringEOF, clientId, true)
				return false

			} else {
				// If the EOF does not do a full cycle, we continue the RingEOF cycle until it is ready
				h.nextWorkerRing(tasks, ringEOF, clientId, false)
				return false
			}
		}

	} else {
		// IF the EOF is Ready, we can return the tasks immediatel or the next stage EOF
		if ringEOF.GetReadyId() == h.nodeId {
			// If the EOF is ready and it is from this worker, we can send the next stage EOF cause the RingEOF do a full cycle
			h.nextStagesOmegaEOF(tasks, ringEOF, clientId)
			return false
		} else {
			// If the EOF is ready but it is not from this worker, we continue the RingEOF cycle
			h.nextWorkerRing(tasks, ringEOF, clientId, false)
			return true
		}
	}
}

func (h *StatefulEofHandler) mergeStageFragments(ringEOF *protocol.RingEOF, taskFragments []common.TaskFragmentIdentifier) {
	if len(taskFragments) == 0 {
		return
	}

	stageFragments := ringEOF.GetStageFragmentes()

	for _, taskFragment := range taskFragments {
		stageFragments = append(stageFragments, &protocol.StageFragment{
			Start: &protocol.FragmentIdentifier{
				TaskNumber:         taskFragment.TaskNumber,
				TaskFragmentNumber: taskFragment.TaskFragmentNumber,
			},
			End: &protocol.FragmentIdentifier{
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
		currentFragment := stageFragments[i]
		i++
		for i < len(stageFragments) {
			nextFragment := stageFragments[i]

			if currentFragment.End.GetTaskNumber() == nextFragment.Start.GetTaskNumber() && currentFragment.End.GetTaskFragmentNumber()+1 == nextFragment.Start.GetTaskFragmentNumber() {
				currentFragment.End = nextFragment.End
				currentFragment.LastFragment = currentFragment.LastFragment || nextFragment.LastFragment
				i++
			} else if currentFragment.LastFragment && currentFragment.End.GetTaskNumber()+1 == nextFragment.Start.GetTaskNumber() && nextFragment.Start.GetTaskFragmentNumber() == 0 {
				// If the current fragment is the last and the next fragment starts with 0, we can merge them
				currentFragment.End = nextFragment.End
				currentFragment.LastFragment = nextFragment.LastFragment
				i++
			} else {
				break // No more consecutive fragments to merge
			}
		}
		mergedFragments = append(mergedFragments, currentFragment)
	}

	ringEOF.StageFragmentes = mergedFragments
}

func (h *StatefulEofHandler) addToWorkerParticipantIds(
	ringEof *protocol.RingEOF,
) {
	if slices.Contains(ringEof.GetNextStageWorkerParticipantIds(), h.nodeId) {
		return
	}

	ringEof.NextStageWorkerParticipantIds = append(ringEof.NextStageWorkerParticipantIds, h.nodeId)
}

func isStageReady(ringEOF *protocol.RingEOF) bool {
	stageFragments := ringEOF.GetStageFragmentes()
	taskCount := ringEOF.GetTasksCount()

	totalTasks := 0
	for _, frag := range stageFragments {
		// Each fragment must start with fragment 0 and be marked as last
		if frag.Start.GetTaskFragmentNumber() != 0 || !frag.LastFragment {
			return false
		}

		startTask := frag.Start.GetTaskNumber()
		endTask := frag.End.GetTaskNumber()

		totalTasks += int(endTask - startTask + 1)
	}

	return totalTasks == int(taskCount)
}
