package util

import "math"

// Cast XXX
func Cast(num float64) int {
	return int(num + math.Copysign(0.5, num))
}

// Round XXX
func Round(num float64, precision int) float64 {
	output := math.Pow(10, float64(precision))
	return float64(Cast(num*output)) / output
}

// Sum XXX
func Sum(nums []float64) float64 {
	var total float64
	for _, num := range nums {
		total += num
	}
	return total
}

// Contains XXX
func Contains(slice []int32, item int32) bool {
	for _, val := range slice {
		if val == item {
			return true
		}
	}
	return false
}
