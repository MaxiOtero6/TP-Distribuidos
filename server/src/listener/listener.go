package listener

import (
	client_server_communication "github.com/MaxiOtero6/TP-Distribuidos/common/communication/client-server"
	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/mom"
	"github.com/op/go-logging"
)

var log = logging.MustGetLogger("log")

type Listener struct {
	ch       *client_server_communication.ConnectionHandler
	ID       string
	rabbitMQ *mom.RabbitMQ
}

func NewListener(id string, address string) *Listener {
	ch, err := client_server_communication.NewConnectionHandler(id, address)
	if err != nil {
		log.Panicf("Failed to parse RabbitMQ configuration: %s", err)
	}

	return &Listener{
		ch:       ch,
		ID:       id,
		rabbitMQ: mom.NewRabbitMQ(),
	}
}

func (l *Listener) InitConfig(exchanges []map[string]string, queues []map[string]string, binds []map[string]string) {
	l.rabbitMQ.InitConfig(exchanges, queues, binds, l.ID)
}

func (l *Listener) Run() error {
	return l.ch.Run()
}

func (l *Listener) Stop() {
	log.Infof("Stopping listener with ID: %s", l.ID)
	l.ch.Stop()
	log.Infof("Listener with ID: %s stopped", l.ID)
}
