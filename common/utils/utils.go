package utils

import (
	"fmt"
	"math/rand"
	"strconv"
	"time"

	"github.com/spf13/viper"
)

func GetWorkerIdFromHash(workersCount int, itemIdStr string) (hash string, err error) {
	itemId, err := strconv.Atoi(itemIdStr)

	if err != nil {
		return
	}

	hash = fmt.Sprint(itemId % workersCount)
	return
}

func RandomHash(workersCount int) string {
	// Create a new random source with a specific seed
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	return fmt.Sprint(r.Intn(workersCount))
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
