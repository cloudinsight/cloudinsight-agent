package util

import (
	"errors"
	"fmt"
	"hash/fnv"
	"math"
	"reflect"
	"strings"
)

// Cast rounds num to integer.
func Cast(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

// Round implements the round function.
func Round(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(Cast(num*output)) / output
}

// Sum sums all numbers in nums.
func Sum(nums []float64) float64 {
	var total float64
	for _, num := range nums {
		total += num
	}
	return total
}

// Contains reports whether an item is within the slice.
func Contains(slice []int32, item int32) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}

// Hash generates hash number of a string.
func Hash(s string) uint32 {
	h := fnv.New32a()
	_, _ = h.Write([]byte(s))
	return h.Sum32()
}

// FillStruct converts map to struct.
// See http://stackoverflow.com/questions/26744873/converting-map-to-struct.
func FillStruct(m map[string]interface{}, s interface{}) error {
	structValue := reflect.ValueOf(s).Elem()

	for i := 0; i < structValue.NumField(); i++ {
		structFieldValue := structValue.Field(i)
		structFieldType := structValue.Type().Field(i)
		tag := strings.ToLower(structFieldType.Name)
		if structFieldType.Tag != "" {
			tag = structFieldType.Tag.Get("yaml")
		}

		if value, ok := m[tag]; ok {
			if !structFieldValue.CanSet() {
				return fmt.Errorf("Cannot set %s field value", tag)
			}

			val := reflect.ValueOf(value)
			if tag == "tags" {
				var tags []string
				for _, t := range val.Interface().([]interface{}) {
					switch v := t.(type) {
					case string:
						tags = append(tags, v)
					default:
						return fmt.Errorf("Tags type %s not supported", v)
					}
				}
				structFieldValue.Set(reflect.ValueOf(tags))
				return nil
			}

			if structFieldValue.Type() != val.Type() {
				return errors.New("Provided value type didn't match obj field type")
			}

			structFieldValue.Set(val)
		}
	}

	return nil
}
