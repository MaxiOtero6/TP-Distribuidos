package utils

import (
	"fmt"
	"hash/fnv"
	"math/rand"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

func fnvHash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

func GetWorkerIdFromHash(workersCount int, item string) string {
	hashedItem := fnvHash(item)

	return fmt.Sprint(hashedItem % uint32(workersCount))
}

var randomSource = rand.New(rand.NewSource(time.Now().UnixNano()))

func RandomHash(workersCount int) string {
	// Use the package-level random source
	return fmt.Sprint(randomSource.Intn(workersCount))
}

func GetNextNodeId(nodeId string, workerCount int) (string, error) {
	currentNodeId, err := strconv.Atoi(nodeId)
	if err != nil {
		return "", fmt.Errorf("failed to convert currentNodeId to int: %s", err)
	}

	nextNodeIdInt := (currentNodeId + 1) % workerCount

	nextNodeId := fmt.Sprintf("%d", nextNodeIdInt)
	return nextNodeId, nil
}

func ViperGetSliceMapStringString(data map[string]any) ([]map[string]string, error) {
	var ret []map[string]string

	for key, value := range data {
		exchangeMap, ok := value.(map[string]any)

		if !ok {
			return nil, fmt.Errorf("Failed to assert type value: %v, expected map[string]any", value)
		}

		// Convert map[string]interface{} to map[string]string
		parsedExchange := make(map[string]string)
		for k, v := range exchangeMap {
			strValue, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("Failed to assert type for key %s in data %s", k, key)
			}
			parsedExchange[k] = strValue
		}

		ret = append(ret, parsedExchange)
	}

	return ret, nil
}

func GetRabbitConfig(nodeType string, v *viper.Viper) (exchanges []map[string]string, queues []map[string]string, binds []map[string]string, err error) {
	exchanges, err = ViperGetSliceMapStringString(
		v.GetStringMap("rabbitmq." + nodeType + ".exchanges"),
	)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse exchanges: %s", err)
	}

	queues, err = ViperGetSliceMapStringString(
		v.GetStringMap("rabbitmq." + nodeType + ".queues"),
	)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to parse queues: %s", err)
	}

	binds, err = ViperGetSliceMapStringString(
		v.GetStringMap("rabbitmq." + nodeType + ".binds"),
	)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("Failed to parse binds: %s", err)
	}

	return exchanges, queues, binds, nil
}
