package mom

import (
	"fmt"

	amqp "github.com/rabbitmq/amqp091-go"
)

// queue represents a RabbitMQ queue.
// It contains the channel to communicate with RabbitMQ, the queue name,
// and various properties of the queue.
// The queue name is the name used to identify the queue on the RabbitMQ server.
// The properties include whether the queue is durable, auto-deleted,
// exclusive, and whether to wait for the queue to be created.
// The args field can be used to pass additional arguments to the queue declaration.
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

// newQueue creates a new RabbitMQ queue with the specified name.
// It initializes the queue properties and declares the queue on the RabbitMQ server.
// If the queue declaration fails, an error is logged and the program panics.
// The function returns a pointer to the newly created queue.
// The queue is durable, auto-deleted, and not exclusive by default.
// The noWait property is set to false, and no additional arguments are passed.
// The queue name is the name used to identify the queue on the RabbitMQ server.
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

// bind binds the queue to an exchange with the specified routing key.
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

// unbind unbinds the queue from an exchange with the specified routing key.
func (q *queue) unbind(exchangeName string, routingKey string) {
	err := q.channel.QueueUnbind(
		q.amqpName,
		routingKey,
		exchangeName,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to unbind the queue: '%v'", q.amqpName))
}

// consume registers a consumer for the queue.
// It returns a channel that receives messages from the queue.
// autoAck is set to false, meaning that the consumer must acknowledge the messages manually.
func (q *queue) consume() <-chan amqp.Delivery {
	msgs, err := q.channel.Consume(
		q.amqpName,
		q.amqpName,
		false,
		false,
		false,
		false,
		nil,
	)

	failOnError(err, fmt.Sprintf("Failed to register a consumer: '%v'", q.amqpName))

	return msgs
}

// close cancels the consumer for the queue.
// It does not close the channel, as it is assumed that the channel
// will be closed by the caller when it is no longer needed.
// If the cancellation fails, an error is logged.
// If the cancellation is successful, a message is logged indicating
// that the consumer was cancelled gracefully.
func (q *queue) close() {
	if err := q.channel.Cancel(q.amqpName, false); err != nil {
		log.Errorf("Failed to cancel the consumer '%v': '%v'", q.name, err)
	} else {
		log.Infof("Consumer '%v' cancelled gracefully ", q.name)
	}
}
