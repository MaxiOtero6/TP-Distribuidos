package actions

import "github.com/MaxiOtero6/TP-Distribuidos/common/model"

// Main for actions testing

var testInfraConfig = model.NewInfraConfig(
	&model.WorkerClusterConfig{
		JoinCount: 1,
	},
	&model.RabbitConfig{
		FilterExchange: "filterExchange",
		JoinExchange:   "joinExchange",
		ResultExchange: "resultExchange",
		MapExchange:    "mapExchange",
		BroadcastID:    "",
	},
)
