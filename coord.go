package geocube

import "fmt"

//func load

func geocube() {
	path := "lalala"
	dPoints, _ := ImportData(path)

	pDims := []uint{1, 0}
	pCaps := []uint{20, 20}

	initMins := []float64{-73.925 - 0.3, 40.75 - 0.3}
	initMaxs := []float64{-73.925 + 0.3, 40.75 + 0.3}
	splitThresRatio := 0.4

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)
	dTree.UpdateTree(dPoints)

	q1 := InitQuery(1, []uint{1, 0}, []float64{-73.925, 40.75}, []int{0, 0}, 5, "lala")
	fmt.Println(q1)
}
