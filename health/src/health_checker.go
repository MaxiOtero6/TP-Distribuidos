package health_checker

import "github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"

type HealthChecker struct {
	rabbitMQ *mom.RabbitMQ
	ID       string
}

func NewHealthChecker(id string) *HealthChecker {
	return &HealthChecker{
		rabbitMQ: mom.NewRabbitMQ(),
		ID:       id,
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
