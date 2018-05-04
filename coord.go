package main

import (
	"fmt"
	"log"
	"runtime"
	"time"
)

func printMemUsage() {
	var m runtime.MemStats
	runtime.ReadMemStats(&m)
	// For info on each, see: https://golang.org/pkg/runtime/#MemStats
	fmt.Printf("Alloc = %v MiB", bToMb(m.Alloc))
	fmt.Printf("\tTotalAlloc = %v MiB", bToMb(m.TotalAlloc))
	fmt.Printf("\tSys = %v MiB", bToMb(m.Sys))
	fmt.Printf("\tNumGC = %v\n", m.NumGC)
}

func bToMb(b uint64) uint64 {
	return b / 1024 / 1024
}

// Test ..
func Test(path string) {
	//dPoints = dPoints[:20]
	pDims := []uint{1, 0}
	pCaps := []uint{100, 100}

	initMins := []float64{40.75 - 0.3, -73.925 - 0.3}
	initMaxs := []float64{40.75 + 0.3, -73.925 + 0.3}
	splitThresRatio := 0.4

	fmt.Println("Start Initializing Tree...")

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)

	var qs []*Query
	for i := 1; i <= 1; i++ {
		fmt.Printf("loading file %d\n", i)
		//path = path + "2015-09-0" + strconv.Itoa(i) + ".csv"
		path = "medium_test.csv"

		dPoints, err := ImportData(path)
		//fmt.Println(len(dPoints_data))
		//length := 54921
		//dPoints := make([]DataPoint, length)

		//copy(dPoints, dPoints_data[:length])

		fmt.Println(len(dPoints))
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
	fmt.Printf("Total number of nodes, include non-leaf, %d\n", len(dTree.Nodes))

	batches := dTree.ToDataBatch()
	printMemUsage()

	fmt.Println(len(batches))
	//fmt.Println(batches[0])

	fmt.Println("Start Initializing DB...")
	db, err := InitDB()
	if err != nil {
		panic(err)
	}
	for _, batch := range batches {
		db.Feed(&batch)
	}
	printMemUsage()

	fmt.Println("Start Executing Query...")

	//q1 := InitQuery(1, []uint{1, 0}, []float64{40.693225860595703, -73.972030639648438}, []int{0, 0}, 5, "lala")
	//fmt.Println(q1)
	storageName := "dTree.json"
	dTree.ToString(storageName)
	dTree2 := LoadDTree(storageName, nil)

	worker := Worker{dTree: dTree2}
	start := time.Now()

	for _, batch := range batches {
		cubeind := batch.CubeId
		db.ReadAll(cubeind)
	}
	elapsed := time.Since(start)
	log.Printf("Time to load all data: &s\n", elapsed)

	totalConflictNum := int(0)
	for i, q := range qs {
		dataPoints, conflictNum, err := worker.EqualityQuery(db, q)
		totalConflictNum += conflictNum
		if err != nil {
			panic(err)
		}
		if len(dataPoints) == 0 {
			fmt.Println("Fail to find on query index %d\n", i)
		}

	}
	elapsed = time.Since(start)
	log.Printf("Time Overall: &s\n", elapsed)
	log.Printf("Total Conflict Number: %d, among %d queries\n", totalConflictNum, len(qs))
}

//GenerateFakeQuery ...
func (dPoint *DataPoint) GenerateFakeQuery() *Query {
	q1 := InitQuery(1, []uint{1, 0}, []float64{dPoint.getFloatValByDim(uint(1)), dPoint.getFloatValByDim(uint(0))}, []int{0, 0}, 5, "lala")
	return q1
}
