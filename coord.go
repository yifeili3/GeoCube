package main

import (
	"fmt"
	"log"
	"time"
)

// Test ..
func Test(path string) {
	//dPoints = dPoints[:20]
	pDims := []uint{1, 0}
	pCaps := []uint{10, 10}

	initMins := []float64{40.75 - 0.3, -73.925 - 0.3}
	initMaxs := []float64{40.75 + 0.3, -73.925 + 0.3}
	splitThresRatio := 0.4

	fmt.Println("Start Initializing Tree...")

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)

	var qs []*Query
	for i := 1; i <= 1; i++ {
		fmt.Printf("loading file %d\n", i)
		//path = path + "2015-09-0" + strconv.Itoa(i) + ".csv"
		path = "out.csv"

		dPoints, err := ImportData(path)
		if err != nil {
			log.Println(err)
		}
		err = dTree.UpdateTree(dPoints)
		if err != nil {
			panic(err)
		}
		for _, d := range dPoints {
			qs = append(qs, d.GenerateFakeQuery())
		}

	}
	batches := dTree.ToDataBatch()

	fmt.Println(len(batches))
	//fmt.Println(batches[0])

	fmt.Println("Start Initializing DB...")
	db, err := InitDB()
	if err != nil {
		panic(err)
	}
	for _, batch := range batches {
		db.Feed(batch)
	}

	fmt.Println("Start Executing Query...")

	//q1 := InitQuery(1, []uint{1, 0}, []float64{40.693225860595703, -73.972030639648438}, []int{0, 0}, 5, "lala")
	//fmt.Println(q1)

	worker := Worker{dTree}
	start := time.Now()

	for _, batch := range batches {
		cubeind := batch.CubeId
		db.ReadAll(cubeind)
	}

	for _, q := range qs {
		_, err := worker.EqualityQuery(db, q)
		if err != nil {
			panic(err)
		}
		/*
			for _, dPoint := range dataPoints {
				fmt.Println(dPoint)
			}*/

	}
	elapsed := time.Since(start)
	log.Printf("Time: &s\n", elapsed)
}

func (dPoint *DataPoint) GenerateFakeQuery() *Query {
	q1 := InitQuery(1, []uint{1, 0}, []float64{dPoint.getFloatValByDim(uint(1)), dPoint.getFloatValByDim(uint(0))}, []int{0, 0}, 5, "lala")
	return q1
}
