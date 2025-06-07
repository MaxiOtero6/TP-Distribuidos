package health_checker

import (
	"os/exec"
	"time"

	"github.com/MaxiOtero6/TP-Distribuidos/common/communication/protocol"
	"google.golang.org/protobuf/proto"
)

func (hc *HealthChecker) runLeader() {
	for hc.running {
		hc.sendPing()

		time.Sleep(hc.healthCheckInterval)

		hc.readResponses()
		hc.sendStatus()
		hc.wakeUpContainers()
	}
}

func (hc *HealthChecker) wakeUpContainers() {
	for containerName, status := range hc.status {
		if status >= hc.maxStatus {
			cmd := exec.Command("docker-compose", "up", "-d", containerName)
			if err := cmd.Run(); err != nil {
				log.Errorf("Failed to wake up container %s: %v", containerName, err)
			} else {
				log.Infof("Container %s is now awake", containerName)
			}
		}
	}
}

func (hc *HealthChecker) sendStatus() {
	msg := &protocol.HealthInternalMessage{
		Message: &protocol.HealthInternalMessage_Status{
			Status: &protocol.Status{
				Status: hc.status,
			},
		},
	}

	bytesMsg, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Error marshalling status message: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetHealthExchange(),
		hc.infraConfig.GetBroadcastID(),
		bytesMsg,
	)
}

func (hc *HealthChecker) sendPing() {
	msg := &protocol.HealthMessage{
		Message: &protocol.HealthMessage_PingRequest{
			PingRequest: &protocol.PingRequest{},
		},
	}

	bytesMsg, err := proto.Marshal(msg)
	if err != nil {
		log.Errorf("Error marshalling ping request: %v", err)
		return
	}

	hc.rabbitMQ.Publish(
		hc.infraConfig.GetControlExchange(),
		hc.infraConfig.GetControlBroadcastRK(),
		bytesMsg,
	)
}

func (hc *HealthChecker) readResponses() {
	subscriptions := make([]string, 0)
	responses := make([]string, 0)

	for {
		msg, ok := hc.rabbitMQ.GetHeadDelivery(hc.leaderQueueName)

		if !ok {
			break
		}

		data := &protocol.HealthMessage{}

		err := proto.Unmarshal(msg.Body, data)
		if err != nil {
			log.Errorf("Error unmarshalling task: %v", err)
			continue
		}

		switch data.GetMessage().(type) {
		case *protocol.HealthMessage_PingResponse:
			pingResponse := data.GetPingResponse()
			responses = append(responses, pingResponse.GetContainerName())
		case *protocol.HealthMessage_Subscribe:
			subscribe := data.GetSubscribe()
			subscriptions = append(subscriptions, subscribe.GetContainerName())
		default:
			log.Warningf("Unknown or unexpected health message type: %T", data.GetMessage())
			msg.Reject(false)
			continue
		}

		msg.Ack(false)
	}

	for _, containerName := range subscriptions {
		if _, exists := hc.status[containerName]; !exists {
			hc.status[containerName] = 0 // Initialize status if not present
		}
	}

	for _, containerName := range responses {
		if _, exists := hc.status[containerName]; !exists {
			hc.status[containerName] = 0 // Initialize status if not present
		}
		hc.status[containerName]++ // Increment the status for the container
	}
}
