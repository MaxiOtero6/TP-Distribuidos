package eof

import (
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"

	"github.com/MaxiOtero6/TP-Distribuidos/worker/src/common"
)

func StatelessOmegaEof(eofData *protocol.OmegaEOF_Data, clientId string, nextStageFunc func(stage string, clientId string) ([]common.NextStageData, error)) common.Tasks {
	tasks := make(common.Tasks)

	stage := eofData.Stage
	nextStagesData, err := nextStageFunc(stage, clientId)

	if err != nil {
		return tasks
	}

	for _, nextStageData := range nextStagesData {

		nextStageEOF := &protocol.Task{
			ClientId: clientId,
			Stage: &protocol.Task_OmegaEOF{
				OmegaEOF: &protocol.OmegaEOF{
					Data: &protocol.OmegaEOF_Data{
						Stage:      nextStageData.Stage,
						EofType:    eofData.EofType,
						TasksCount: eofData.TasksCount,
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

		tasks[nextStageData.Exchange][nextStageData.Stage][nextStageData.RoutingKey] = nextStageEOF
	}

	return tasks
}
