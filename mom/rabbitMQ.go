package mom

import (
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const URL = "amqp://guest:guest@rabbitmq:5672/"

// failOnError checks if an error occurred and logs it.
// If an error occurred, it logs the error message and panics.
func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

// RabbitMQ represents a RabbitMQ connection and channel.
// It contains a map of queues and exchanges.
// The queues and exchanges are identified by their names.
// The queues and exchanges are created and managed by the RabbitMQ struct.
type RabbitMQ struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queues    map[string]*queue
	exchanges map[string]*exchange
}

// NewRabbitMQ creates a new RabbitMQ connection and channel.
func NewRabbitMQ() *RabbitMQ {
	conn, err := amqp.Dial(URL)
	failOnError(err, "Failed to connect to RabbitMQ")

	ch, err := conn.Channel()
	failOnError(err, "Failed to open a channel")

	return &RabbitMQ{
		conn:      conn,
		ch:        ch,
		queues:    make(map[string]*queue),
		exchanges: make(map[string]*exchange),
	}
}

// NewQueue creates a new RabbitMQ queue with the specified name.
// If a queue with the same name already exists, an error is logged and the function returns.
func (r *RabbitMQ) NewQueue(name string) {
	if _, ok := r.queues[name]; ok {
		log.Errorf("Queue '%s' already exists", name)
		return
	}

	r.queues[name] = newQueue(r.ch, name)
}

// NewExchange creates a new RabbitMQ exchange with the specified name and type.
// The exchange type can be "direct", "fanout", "topic", etc.
// If an exchange with the same name already exists with a different type,
// an error is logged and the function returns.
func (r *RabbitMQ) NewExchange(name string, kind string) {
	if ex, ok := r.exchanges[name]; ok {
		if ex.kind != kind {
			log.Errorf("Exchange '%s' already exists with a different kind", name)
		}

		return
	}

	r.exchanges[name] = newExchange(r.ch, name, kind)
}

// Publish publishes a message to the specified exchange with the given routing key.
// The message body is passed as a byte slice.
// If the exchange does not exist, an error is logged and the function returns.
// The routing key is used to route the message to the appropriate queue(s).
func (r *RabbitMQ) Publish(exchangeName string, routingKey string, body []byte) {
	ex, ok := r.exchanges[exchangeName]

	if !ok {
		log.Errorf("Exchange '%s' does not exist", exchangeName)
		return
	}

	ex.publish(routingKey, body)
}

// Consume consumes messages from the specified queue.
// It returns a channel that receives messages from the queue.
// If the queue does not exist, an error is logged and the function returns nil.
func (r *RabbitMQ) Consume(queueName string) <-chan amqp.Delivery {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return nil
	}

	return q.consume()
}

// BindQueue binds the specified queue to the specified exchange with the given routing key.
// If the queue or exchange does not exist, an error is logged and the function returns.
// The routing key is used to route the messages from the exchange to the queue.
func (r *RabbitMQ) BindQueue(queueName string, exchangeName string, routingKey string) {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return
	}

	q.bind(exchangeName, routingKey)
}

// Close closes the RabbitMQ connection and channel.
// It also closes all the queues and exchanges.
// If an error occurs while closing the connection or channel, it is logged.
func (r *RabbitMQ) Close() {
	if err := r.ch.Close(); err != nil {
		log.Errorf("Failed to close the RabbitMQ channel: '%v'", err)
	}

	if err := r.conn.Close(); err != nil {
		log.Errorf("Failed to close the RabbitMQ connection: '%v'", err)
	}

	for _, queue := range r.queues {
		queue.close()
	}

	for _, exchange := range r.exchanges {
		exchange.close()
	}
}
