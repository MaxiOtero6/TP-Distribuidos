package eof

import (
	"sort"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

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
			tasks[exchange] = make(map[string][]*protocol.Task)
		}

		if _, exists := tasks[exchange][routingKey]; !exists {
			tasks[exchange][routingKey] = []*protocol.Task{}
		}

		tasks[exchange][routingKey] = append(tasks[exchange][routingKey], nextStageRing)

	}
}

func (h *StatefulEofHandler) initRingEof(omegaEOFData *protocol.OmegaEOF_Data) *protocol.RingEOF {
	return &protocol.RingEOF{
		Stage:                      omegaEOFData.GetStage(),
		EofType:                    omegaEOFData.GetEofType(),
		CreatorId:                  h.nodeId,
		ReadyId:                    "",
		TasksCount:                 omegaEOFData.GetTasksCount(),
		RoundNumber:                1, // Start with round 1 so it does not conflict with the default round 0
		StageFragmentes:            []*protocol.StageFragment{},
		NextStageTaskCountByWorker: []*protocol.TaskCountByWorker{},
	}
}

func (h *StatefulEofHandler) nextStagesOmegaEOF(tasks common.Tasks, ringEOF *protocol.RingEOF, clientId string) {
	nextStagesData, err := h.nextStageFunc(ringEOF.GetStage(), clientId, h.infraConfig, h.itemHashFunc)
	if err != nil {
		return
	}

	tasksCount := 0

	for _, workerTaskCount := range ringEOF.GetNextStageTaskCountByWorker() {
		tasksCount += int(workerTaskCount.GetTaskCount())
	}

	for _, nextStageData := range nextStagesData {
		nextStageOmegaEof := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						Stage:      nextStageData.Stage,
						EofType:    ringEOF.GetEofType(),
						TasksCount: uint32(tasksCount),
					},
				},
			},
		}

		exchange := nextStageData.Exchange
		routingKey := nextStageData.RoutingKey

		if _, exists := tasks[exchange]; !exists {
			tasks[exchange] = make(map[string][]*protocol.Task)
		}

		if _, exists := tasks[exchange][routingKey]; !exists {
			tasks[exchange][routingKey] = []*protocol.Task{}
		}

		tasks[exchange][routingKey] = append(tasks[exchange][routingKey], nextStageOmegaEof)
	}
}

func (h *StatefulEofHandler) HandleOmegaEOF(tasks common.Tasks, omegaEOFData *protocol.OmegaEOF_Data, clientId string) {
	ringEof := h.initRingEof(omegaEOFData)

	h.nextWorkerRing(tasks, ringEof, clientId, false)
}

func (h *StatefulEofHandler) HandleRingEOF(tasks common.Tasks, ringEOF *protocol.RingEOF, clientId string, workerFragments []model.TaskFragmentIdentifier, resultsTaskCount int) bool {
	if ringEOF.GetCreatorId() == h.nodeId {
		// If the RingEOF is created by this worker, we advaance the round
		ringEOF.RoundNumber++
	}

	// TODO: @MaxiOtero6 como harías esto ?
	// if ringEOF.RoundNumber > 2000 {
	// 	return false // If the round number is greater than 20, we stop the RingEOF cycle to avoid infinite loops
	// }

	h.updateNextStageTaskCount(ringEOF, resultsTaskCount)

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
				h.nextWorkerRing(tasks, ringEOF, clientId, false)
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

func (h *StatefulEofHandler) mergeStageFragments(ringEOF *protocol.RingEOF, taskFragments []model.TaskFragmentIdentifier) {
	// if len(taskFragments) > 0 {
	// 	for _, taskFragment := range taskFragments {
	// 		log.Debugf("Adding Task Fragment for stage %s: %s", ringEOF.GetStage(), taskFragment)
	// 	}

	// 	log.Debugf("Merging Task Fragments for stage %s, initial fragments len %d and ranges %d", ringEOF.GetStage(), len(ringEOF.GetStageFragmentes()), ringEOF.GetStageFragmentes())
	// } else {
	// 	log.Debugf("No Task Fragments to merge for stage %s, initial fragments len %d and ranges %d", ringEOF.GetStage(), len(ringEOF.GetStageFragmentes()), ringEOF.GetStageFragmentes())
	// }

	stageFragments := ringEOF.GetStageFragmentes()

	for _, taskFragment := range taskFragments {
		stageFragments = append(stageFragments, &protocol.StageFragment{
			CreatorId: taskFragment.CreatorId,
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
		if stageFragments[i].GetCreatorId() != stageFragments[j].GetCreatorId() {
			return stageFragments[i].GetCreatorId() < stageFragments[j].GetCreatorId()
		}
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

			if currentFragment.CreatorId != nextFragment.CreatorId {
				break // Different creators, cannot merge
			}

			merge := false
			// overlap := false

			if currentFragment.End.GetTaskNumber() > nextFragment.Start.GetTaskNumber() {
				// overlap = true
				// If the current fragment's end task number is greater than or equal to the next fragment's start task number,
				// we can merge them if they are consecutive or overlapping
				if currentFragment.End.GetTaskNumber() > nextFragment.End.GetTaskNumber() {
					// If the current fragment's end task number is greater than the next fragment's end task number,
					// we ignore it cause it is already included in the current fragment
					merge = false
				} else if currentFragment.End.GetTaskNumber() == nextFragment.Start.GetTaskNumber() {
					if currentFragment.End.GetTaskFragmentNumber() >= nextFragment.End.GetTaskFragmentNumber() {
						// If the current fragment's end task fragment number is greater than or equal to the next fragment's end task fragment number,
						// we ignore it cause it is already included in the current fragment
						merge = false
					} else {
						// If the current fragment's end task fragment number is less than the next fragment's end task fragment number,
						// we can merge them
						merge = true
					}
				} else {
					merge = true
				}
			} else if currentFragment.End.GetTaskNumber() == nextFragment.Start.GetTaskNumber() {
				if currentFragment.End.GetTaskFragmentNumber() >= nextFragment.Start.GetTaskFragmentNumber() {
					// overlap = true
					if currentFragment.End.GetTaskFragmentNumber() >= nextFragment.End.GetTaskFragmentNumber() {
						// If the current fragment's end task fragment number is greater than the next fragment's start task fragment number,
						// we ignore it cause it is already included in the current fragment
						merge = false
					} else {
						// If the current fragment's end task fragment number is equal to the next fragment's start task fragment number,
						// we can merge them
						merge = true
					}
				} else if currentFragment.End.GetTaskFragmentNumber()+1 == nextFragment.Start.GetTaskFragmentNumber() {
					// If the current fragment's end task fragment number is one less than the next fragment's start task fragment number,
					// we can merge them
					merge = true
				} else {
					// If the current fragment's end task fragment number is less than the next fragment's start task fragment number,
					// we cannot merge them
					break
				}
			} else if currentFragment.End.GetTaskNumber()+1 == nextFragment.Start.GetTaskNumber() {
				// If the next fragment is consecutive to the current fragment,
				if currentFragment.LastFragment && nextFragment.Start.GetTaskFragmentNumber() == 0 {
					// If the current fragment's end task number is one less than the next fragment's start task number,
					// and the next fragment starts with task fragment number 0, we can merge them
					merge = true
				} else {
					// If the current fragment's end task number is one less than the next fragment's start task number,
					// but the next fragment does not start with task fragment number 0, we cannot merge them
					break
				}
			}

			// if overlap {
			// 	log.Debugf("There is an overlap between fragments: %s and %s", currentFragment, nextFragment)
			// }

			if merge {
				currentFragment.End = nextFragment.End
				currentFragment.LastFragment = nextFragment.LastFragment
			}

			i++ // Move to the next fragment
		}
		mergedFragments = append(mergedFragments, currentFragment)
	}

	ringEOF.StageFragmentes = mergedFragments

	if len(taskFragments) > 0 {
		// log.Debugf("Merged fragments for stage %s: %d fragments after merging: %v", ringEOF.GetStage(), len(ringEOF.GetStageFragmentes()), ringEOF.GetStageFragmentes())
	} else {
		// log.Debugf("No task fragments to merge for stage %s, final fragments count: %d and ranges %v", ringEOF.GetStage(), len(ringEOF.GetStageFragmentes()), ringEOF.GetStageFragmentes())
	}

}

func (h *StatefulEofHandler) MergeStageFragmentsPublic(ringEOF *protocol.RingEOF, taskFragments []model.TaskFragmentIdentifier) {
	h.mergeStageFragments(ringEOF, taskFragments)
}

func (h *StatefulEofHandler) updateNextStageTaskCount(ringEof *protocol.RingEOF, taskCount int) {
	if taskCount <= 0 {
		return // No need to update if task count is zero or negative
	}

	for _, workerTaskCount := range ringEof.GetNextStageTaskCountByWorker() {
		if workerTaskCount.GetWorkerId() == h.nodeId {
			workerTaskCount.TaskCount = uint32(taskCount)
			return
		}
	}

	ringEof.NextStageTaskCountByWorker = append(ringEof.NextStageTaskCountByWorker, &protocol.TaskCountByWorker{
		WorkerId:  h.nodeId,
		TaskCount: uint32(taskCount),
	})
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

	if totalTasks < int(taskCount) {
		log.Debugf("Stage %s is not ready: expected %d tasks, but got %d", ringEOF.GetStage(), taskCount, totalTasks)
	} else if totalTasks > int(taskCount) {
		log.Debugf("Stage %s has more tasks than expected: expected %d tasks, but got %d", ringEOF.GetStage(), taskCount, totalTasks)
		log.Debugf("Task fragments: %v", ringEOF.GetStageFragmentes())
	} else {
		log.Debugf("Stage %s is ready with %d tasks", ringEOF.GetStage(), totalTasks)
	}

	return totalTasks == int(taskCount)
}
