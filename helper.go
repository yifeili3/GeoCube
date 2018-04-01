package main

func argmax(values []float64) int {
	max_v := values[0]
	max_ind := 0
	for i, v := range values {
		if v > max_v {
			max_v = v
			max_ind = i
		}
	}
	return max_ind
}
