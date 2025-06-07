package health_checker

import (
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/MaxiOtero6/TP-Distribuidos/common/model"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type HealthStatus = map[string]uint32

type HealthChecker struct {
	rabbitMQ            *mom.RabbitMQ
	ID                  string
	running             bool
	healthCheckInterval time.Duration
	infraConfig         *model.InfraConfig
	leaderQueueName     string
	status              HealthStatus
	maxStatus           uint32
}

func NewHealthChecker(id string, healthCheckInterval int, infraConfig *model.InfraConfig, leaderQueueName string, maxStatus uint32) *HealthChecker {
	return &HealthChecker{
		rabbitMQ:            mom.NewRabbitMQ(),
		ID:                  id,
		running:             true,
		healthCheckInterval: time.Duration(healthCheckInterval) * time.Second,
		infraConfig:         infraConfig,
		leaderQueueName:     leaderQueueName,
		status:              make(HealthStatus),
		maxStatus:           maxStatus,
	}
}

func (hc *HealthChecker) InitConfig(exchanges, queues, binds []map[string]string) {
	hc.rabbitMQ.InitConfig(exchanges, queues, binds, hc.ID)
}

func (hc *HealthChecker) Run() error {

	return nil
}

func (hc *HealthChecker) Stop() {
	hc.rabbitMQ.Close()
}
