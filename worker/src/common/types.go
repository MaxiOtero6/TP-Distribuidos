package common

import "github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"

type Tasks map[string]map[string]map[string]*protocol.Task

type NextStageData struct {
	Stage       string
	Exchange    string
	WorkerCount int
	RoutingKey  string
}
