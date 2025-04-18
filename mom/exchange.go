package mom

import (
	"context"
	"fmt"
	"time"

	amqp "github.com/rabbitmq/amqp091-go"
)

type ctx struct {
	ctx    context.Context
	cancel context.CancelFunc
}

type exchange struct {
	channel     *amqp.Channel
	context     ctx
	name        string
	kind        string
	durable     bool
	autoDeleted bool
	internal    bool
	noWait      bool
	args        []string
}

func newExchange(ch *amqp.Channel, name string, kind string) *exchange {
	ex := &exchange{
		channel:     ch,
		context:     ctx{},
		name:        name,
		kind:        kind,
		durable:     true,
		autoDeleted: false,
		internal:    false,
		noWait:      false,
		args:        nil,
	}

	ex.context.ctx, ex.context.cancel = context.WithTimeout(context.Background(), 5*time.Second)

	err := ch.ExchangeDeclare(
		ex.name,
		ex.kind,
		ex.durable,
		ex.autoDeleted,
		ex.internal,
		ex.noWait,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to declare the exchange: '%v'", name))

	return ex
}

func (e *exchange) publish(routingKey string, body []byte) {
	err := e.channel.PublishWithContext(
		e.context.ctx,
		e.name,
		routingKey,
		false,
		false,
		amqp.Publishing{
			DeliveryMode: amqp.Persistent,
			ContentType:  "text/plain",
			Body:         body,
			Timestamp:    time.Now(),
		},
	)

	failOnError(err, fmt.Sprintf("Failed to publish a message to exchange '%v'", e.name))
}

func (e *exchange) close() {
	e.context.cancel()
}
