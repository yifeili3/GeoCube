package main

/*
Pick the first occurence when duplicates exist
*/
func argmax(values []float64) int {
	maxV := values[0]
	maxInd := 0
	for i, v := range values {
		if v > maxV {
			maxV = v
			maxInd = i
		}
	}
	return maxInd
}

/*
Pick the first occurence when duplicates exist
*/
func argmin(values []float64) int {
	minV := values[0]
	minInd := 0
	for i, v := range values {
		if v < minV {
			minV = v
			minInd = i
		}
	}
	return minInd
}
