package mom

import (
	"github.com/op/go-logging"
	amqp "github.com/rabbitmq/amqp091-go"
)

var log = logging.MustGetLogger("log")

const URL = "amqp://guest:guest@rabbitmq:5672/"

func failOnError(err error, msg string) {
	if err != nil {
		log.Panicf("%s: %s", msg, err)
	}
}

type RabbitMQ struct {
	conn      *amqp.Connection
	ch        *amqp.Channel
	queues    map[string]*queue
	exchanges map[string]*exchange
}

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

func (r *RabbitMQ) NewQueue(name string) {
	if _, ok := r.queues[name]; ok {
		log.Errorf("Queue '%s' already exists", name)
		return
	}

	r.queues[name] = newQueue(r.ch, name)
}

func (r *RabbitMQ) NewExchange(name string, kind string) {
	if ex, ok := r.exchanges[name]; ok {
		if ex.kind != kind {
			log.Errorf("Exchange '%s' already exists with a different kind", name)
		}
	}

	r.exchanges[name] = newExchange(r.ch, name, kind)
}

func (r *RabbitMQ) Publish(exchangeName string, routingKey string, body []byte) {
	ex, ok := r.exchanges[exchangeName]

	if !ok {
		log.Errorf("Exchange '%s' does not exist", exchangeName)
		return
	}

	ex.publish(routingKey, body)
}

func (r *RabbitMQ) Consume(queueName string) <-chan amqp.Delivery {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return nil
	}

	return q.consume()
}

func (r *RabbitMQ) BindQueue(queueName string, exchangeName string, routingKey string) {
	q, ok := r.queues[queueName]

	if !ok {
		log.Errorf("Queue '%s' does not exist", queueName)
		return
	}

	q.bind(exchangeName, routingKey)
}

func (r *RabbitMQ) Close() {
	r.conn.Close()
	r.ch.Close()

	for _, exchange := range r.exchanges {
		exchange.close()
	}
}
