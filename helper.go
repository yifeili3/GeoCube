package main

/*
Pick the first occurence when duplicates exist
*/
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

/*
Pick the first occurence when duplicates exist
*/
func argmin(values []float64) int {
	min_v := values[0]
	min_ind := 0
	for i, v := range values {
		if v < min_v {
			min_v = v
			min_ind = i
		}
	}
	return min_ind
}
