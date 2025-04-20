package actions

import "github.com/MaxiOtero6/TP-Distribuidos/common/model"

// Main for actions testing

var testInfraConfig = &model.InfraConfig{
	Workers: &model.WorkerClusterConfig{
		JoinCount: 1,
	},
	Rabbit: &model.RabbitConfig{
		FilterExchange: "filterExchange",
		JoinExchange:   "joinExchange",
		ResultExchange: "resultExchange",
		MapExchange:    "mapExchange",
		BroadcastID:    "",
	},
}
