package common

import "github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"

type Tasks map[string]map[string]map[string]*protocol.Task

type NextStageData struct {
	Stage       string
	Exchange    string
	WorkerCount int
	RoutingKey  string
}

// lo tengo que duplicar porque no puedo usar el struct de protocol.TaskIdentifier como clave, no es comparable
type TaskFragmentIdentifier struct {
	TaskNumber         uint32
	TaskFragmentNumber uint32
	LastFragment       bool
}
