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

func GetNextNodeId(nodeId string, workerCount int) string {
	currentNodeId, err := strconv.Atoi(nodeId)
	if err != nil {
		return ""
	}

	nextNodeIdInt := (currentNodeId + 1) % workerCount

	nextNodeId := fmt.Sprintf("%d", nextNodeIdInt)
	return nextNodeId
}

func ViperGetSliceMapStringString(data map[string]any) ([]map[string]string, error) {
	var ret []map[string]string

	for key, value := range data {
		exchangeMap, ok := value.(map[string]any)

		if !ok {
			return nil, fmt.Errorf("failed to assert type value: %v, expected map[string]any", value)
		}

		// Convert map[string]interface{} to map[string]string
		parsedExchange := make(map[string]string)
		for k, v := range exchangeMap {
			strValue, ok := v.(string)
			if !ok {
				return nil, fmt.Errorf("failed to assert type for key %s in data %s", k, key)
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
		return nil, nil, nil, fmt.Errorf("failed to parse queues: %s", err)
	}

	binds, err = ViperGetSliceMapStringString(
		v.GetStringMap("rabbitmq." + nodeType + ".binds"),
	)

	if err != nil {
		return nil, nil, nil, fmt.Errorf("failed to parse binds: %s", err)
	}

	return exchanges, queues, binds, nil
}

func MapToSlice[T any, K comparable](dataMap map[K]T) []T {
	result := make([]T, 0, len(dataMap))
	for _, value := range dataMap {
		result = append(result, value)
	}
	return result
}

func MapData[F any, T any](data []F, mapperFunc func(F) T) []T {
	mappedData := make([]T, len(data))
	for i, item := range data {
		mappedData[i] = mapperFunc(item)
	}
	return mappedData
}

func FilterData[T any](data []*T, filterFunc func(input *T) bool) []*T {
	filteredData := make([]*T, 0)

	for _, item := range data {
		if item == nil {
			continue
		}
		if filterFunc(item) {
			filteredData = append(filteredData, item)
		}
	}

	return filteredData
}

func GroupData[T any](data []*T, keyFunc func(item *T) string, accFunc func(acc *T, item *T)) []*T {
	dataMap := make(map[string]*T)

	for _, item := range data {
		key := keyFunc(item)
		if _, exists := dataMap[key]; !exists {
			dataMap[key] = item
		} else {
			accFunc(dataMap[key], item)
		}
	}

	return MapToSlice(dataMap)
}
