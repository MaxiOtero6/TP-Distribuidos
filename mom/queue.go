package mom

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

type queue struct {
	channel     *amqp.Channel
	name        string
	amqpName    string
	durable     bool
	autoDeleted bool
	exclusive   bool
	noWait      bool
	args        []string
}

func newQueue(ch *amqp.Channel, name string) *queue {
	q := &queue{
		channel:     ch,
		name:        name,
		durable:     true,
		autoDeleted: false,
		exclusive:   false,
		noWait:      false,
		args:        nil,
	}

	amqpQ, err := ch.QueueDeclare(
		q.name,
		q.durable,
		q.autoDeleted,
		q.exclusive,
		q.noWait,
		nil,
	)

	q.amqpName = amqpQ.Name

	failOnError(err, fmt.Sprintf("Failed to declare the queue: '%v'", name))

	return q
}

func (q *queue) bind(exchangeName string, routingKey string) {
	err := q.channel.QueueBind(
		q.amqpName,
		routingKey,
		exchangeName,
		q.noWait,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to bind the queue: '%v'", q.amqpName))
}

func (q *queue) consume() <-chan amqp.Delivery {
	msgs, err := q.channel.Consume(
		q.amqpName,
		q.amqpName,
		true,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to register a consumer: '%v'", q.amqpName))

	return msgs
}

func (q *queue) close() {
	if err := q.channel.Cancel(q.amqpName, false); err != nil {
		log.Errorf("Failed to cancel the consumer '%v': '%v'", q.name, err)
	} else {
		log.Infof("Consumer '%v' cancelled gracefully ", q.name)
	}
}
