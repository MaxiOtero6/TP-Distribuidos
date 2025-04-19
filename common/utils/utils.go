package utils

import (
	"fmt"
)

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
