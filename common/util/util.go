package util

import (
	"hash/fnv"
	"math"

	"github.com/cloudinsight/cloudinsight-agent/common/log"
	yaml "gopkg.in/yaml.v2"
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
func FillStruct(m map[string]interface{}, s interface{}) error {
	d, err := yaml.Marshal(&m)
	if err != nil {
		log.Errorf("Failed to marshal instance %v", m)
		return err
	}
	err = yaml.Unmarshal([]byte(string(d)), s)
	if err != nil {
		log.Errorf("Failed to unmarshal instance %s", string(d))
		return err
	}
	return nil
}

// StringInSlice checks if item is in slice/array.
func StringInSlice(str string, list []string) bool {
	for _, v := range list {
		if v == str {
			return true
		}
	}
	return false
}
