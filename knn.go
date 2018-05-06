// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
	"strconv"
)

const (
	extensionRatio float64 = 1. + 1e-6
)

func truncFloat(k float64) float64 {
	digitAcc := fmt.Sprintf("%.10f", k)
	f, _ := strconv.ParseFloat(digitAcc, 10)
	return f
}

func trunc2String(k float64) string {
	digitAcc := fmt.Sprintf("%.9f", k)
	return digitAcc
}

// A PriorityQueue implements heap.Interface and holds DataPoints.
/*
type PQDataPoints struct {
	q         Query
	distances []float64
	points    []*DataPoint
}

func (pq *PQDataPoints) Len() int { return len(pq.distances) }

func (pq *PQDataPoints) Less(i, j int) bool {
	// We want Pop to give us the lowest distance so we use less than here.
	return pq.distances[i] < pq.distances[j]
}

func (pq *PQDataPoints) Swap(i, j int) {
	pq.points[i], pq.points[j] = pq.points[j], pq.points[i]
	pq.distances[i], pq.distances[j] = pq.distances[j], pq.distances[i]
}

func (pq *PQDataPoints) Push(x interface{}) {
	item := x.(*DataPoint)
	pq.points = append(pq.points, item)
	pq.distances = append(pq.distances, pq.q.DistanceToCenter(item))
}

func (pq *PQDataPoints) Pop() interface{} {
	n := len(pq.points)
	item := pq.points[n-1]
	pq.points = pq.points[0 : n-1]
	pq.distances = pq.distances[0 : n-1]
	return item
}
*/

type KNNPoint struct {
	distance float64
	vals     []float64
	dPoint   *DataPoint
}

type PQKNNPoints struct {
	points []*KNNPoint
}

func (pq *PQKNNPoints) Len() int { return len(pq.points) }

func (pq *PQKNNPoints) Less(i, j int) bool {
	// We want Pop to give us the lowest distance so we use less than here.
	return pq.points[i].distance < pq.points[j].distance
}

func (pq *PQKNNPoints) Swap(i, j int) {
	pq.points[i], pq.points[j] = pq.points[j], pq.points[i]
}

func (pq *PQKNNPoints) Push(x interface{}) {
	item := x.(*KNNPoint)
	pq.points = append(pq.points, item)
}

func (pq *PQKNNPoints) Pop() interface{} {
	n := len(pq.points)
	item := pq.points[n-1]
	pq.points = pq.points[0 : n-1]
	return item
}

/*
Compute a point go beyond the boundary point by a small step
*/
func PointExtension(centerP []float64, boundaryP []float64) []float64 {
	extendedP := make([]float64, len(centerP))
	for i, vCenter := range centerP {
		extendedP[i] = vCenter + (boundaryP[i]-vCenter)*extensionRatio
	}
	return extendedP
}

func PointDistance(centerP []float64, boundaryP []float64) float64 {
	distance := float64(0)
	for i, vCenter := range centerP {
		diff := vCenter - boundaryP[i]
		distance += math.Pow(diff, 2)
	}
	return math.Sqrt(distance)
}

func CheckCachedCube(dTree *DTree, cachedCube []int, extendedData []float64) int {
	for _, cubeInd := range cachedCube {
		if err := dTree.Nodes[cubeInd].checkRangeByVal(nil, extendedData); err == nil {
			// nil err means the data is within range
			return cubeInd
		}
	}
	return -1
}

func (worker *Worker) KNNQuery(query *Query) ([]DataPoint, error) {
	centerData, err := query.ToDimFloatVal(worker.dTree)
	//fmt.Println(centerData)
	if err != nil {
		return nil, err
	}
	cubeInds, err := worker.dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, err
	}

	cachedCube := make([]int, 1) // "cached" metaCube, aka dTreeNode
	cachedCube[0] = cubeInds[0]

	// Init data point heap
	dataPointsPQ := new(PQKNNPoints)
	dataPointsPQ.points = make([]*KNNPoint, 0)
	heap.Init(dataPointsPQ)

	// Init boundary point heap
	boundaryPointsPQ := new(PQKNNPoints)
	boundaryPointsPQ.points = make([]*KNNPoint, 0)
	heap.Init(boundaryPointsPQ)

	// Init return data Points array
	outputDataPoints := make([]DataPoint, 0)

	// before beginning of loop, treat centerData as extension point
	extendedData := centerData
	currentBoundDistance := float64(0)
	currentDataDistance := float64(0)
	metaCubeMap := make(map[[2]int]bool)
	boundaryMap := make(map[string]bool)
	for len(outputDataPoints) < query.K {
		/*
			fmt.Println("New Iteration /////////////")
			forPrint := [2]float64{}
			forPrint[0] = extendedData[0] - centerData[0]
			forPrint[1] = extendedData[1] - centerData[1]
			fmt.Println(forPrint)*/
		extensionValid := true
		cubeInd := CheckCachedCube(worker.dTree, cachedCube, extendedData)
		currMetaInd := int(-1)
		if cubeInd == -1 {
			cubeInds, err = worker.dTree.EquatlitySearch(nil, extendedData)
			if err != nil {
				if cubeInds != nil && cubeInds[0] == -1 {
					// Possible Non-error condition: extended Point is out of the DTree range
					// Need to skip current extended Point
					//fmt.Println("Extension Point out of range")
					extensionValid = false
				} else {
					return nil, err
				}
			} else {
				cubeInd = cubeInds[0]
				cachedCube = append(cachedCube, cubeInd)
			}
		}
		if cubeInd != -1 {
			//fmt.Println(cubeInd)
			currMetaInd, err = worker.dTree.Nodes[cubeInd].MapIndByVal(nil, extendedData)
			if err != nil {
				return nil, err
			}
			metaCell := [2]int{cubeInd, currMetaInd}
			if _, exists := metaCubeMap[metaCell]; exists {
				extensionValid = false
			} else {
				metaCubeMap[metaCell] = true
			}
		}

		if extensionValid {
			metaIndList := make([]int, 1)
			metaIndList[0] = currMetaInd

			//fmt.Printf("Read data from MetaCube %d, CubeCell %d\n", cubeInd, currMetaInd)
			//fmt.Println(metaCubeMap)
			//Perform readBatch to force using cache, since ReadSingle doesn't cache metaCube
			dataPoints := worker.db.ReadBatch(cubeInd, metaIndList)
			for _, dp := range dataPoints {
				knnDp := new(KNNPoint)
				knnDp.dPoint = &dp
				knnDp.distance = query.DistanceToCenter(knnDp.dPoint)
				heap.Push(dataPointsPQ, knnDp)
			}
			// Push both corner and boundary points to Boundary heap
			cornerPoints, err := worker.dTree.Nodes[cubeInd].Corners(currMetaInd)
			if err != nil {
				return nil, err
			} else {
				for _, cP := range cornerPoints {
					// Prevent duplicate boundary point insertion (which will cause infinite loop)
					// Hard code for 2D
					boundKey := fmt.Sprintf("%s-%s", trunc2String(cP[0]), trunc2String(cP[1]))
					if _, exists := boundaryMap[boundKey]; !exists {
						knnBp := new(KNNPoint)
						knnBp.vals = cP
						knnBp.distance = PointDistance(centerData, cP)

						if knnBp.distance < currentBoundDistance {
							fmt.Println("A closer corner was proposed later")
						}
						//fmt.Println(cP)
						//fmt.Println(boundaryMap)
						/*
							forPrint = [2]float64{}
							forPrint[0] = cP[0] - centerData[0]
							forPrint[1] = cP[1] - centerData[1]
							fmt.Printf("%.15f,%.15f\n", forPrint[0], forPrint[1])
						*/
						//if knnBp.distance > currentDataDistance {
						//}
						heap.Push(boundaryPointsPQ, knnBp)
						boundaryMap[boundKey] = true
					}
				}
			}
			boundaryPoints, err := worker.dTree.Nodes[cubeInd].BoundaryConstrain(centerData, currMetaInd)
			if err != nil {
				return nil, err
			} else {
				for _, cP := range boundaryPoints {
					// Prevent duplicate boundary point insertion (which will cause infinite loop)
					// Hard code for 2D
					boundKey := fmt.Sprintf("%s-%s", trunc2String(cP[0]), trunc2String(cP[1]))
					if _, exists := boundaryMap[boundKey]; !exists {
						knnBp := new(KNNPoint)
						knnBp.vals = cP
						knnBp.distance = PointDistance(centerData, cP)

						if knnBp.distance < currentBoundDistance {
							fmt.Printf("A closer boundary was proposed later. cubeInd %d, meta%d \n", cubeInd, currMetaInd)
							fmt.Println(cachedCube)
							fmt.Println("boundary")
							forPrint, forPrint2, _ := worker.dTree.Nodes[cubeInd].Boundary(currMetaInd)
							forPrint[0] = forPrint[0] - centerData[0]
							forPrint[1] = forPrint[1] - centerData[1]
							forPrint2[0] = forPrint2[0] - centerData[0]
							forPrint2[1] = forPrint2[1] - centerData[1]
							fmt.Println(forPrint)
							fmt.Println(forPrint2)
							fmt.Println("end boundary")
						}
						/*
							forPrint = [2]float64{}
							forPrint[0] = cP[0] - centerData[0]
							forPrint[1] = cP[1] - centerData[1]
							fmt.Printf("%.15f,%.15f\n", forPrint[0], forPrint[1])
						*/
						heap.Push(boundaryPointsPQ, knnBp)
						//if knnBp.distance > currentDataDistance {
						//}
						boundaryMap[boundKey] = true
					}
				}
			}
		}

		botBPoint := heap.Pop(boundaryPointsPQ).(*KNNPoint)

		for dataPointsPQ.Len() > 0 {
			// TODO: Need to dump redundant boundaryConstrains
			// For example, the previous extended point is pushed back as well
			// Need to analyze whether this condition will happen
			botDPoint := heap.Pop(dataPointsPQ).(*KNNPoint)
			// Check algorithm/implementation correctness, and track current max distance
			if botDPoint.distance > botBPoint.distance {
				// Then, current DPoint should be push back again since current data may not be nearest
				heap.Push(dataPointsPQ, botDPoint)
				// And break loop
				break
			}
			if botDPoint.distance < currentDataDistance {
				err := errors.New(fmt.Sprintf("Data Priority Queue not in ascending order, len %d", dataPointsPQ.Len()))
				fmt.Println(err)
				return nil, err
			} else {
				currentBoundDistance = botDPoint.distance
			}
			outputDataPoints = append(outputDataPoints, *(botDPoint.dPoint))
			// immediately return if enough points are found
			if len(outputDataPoints) == query.K {
				return outputDataPoints, nil
			}
		}

		if botBPoint.distance < currentBoundDistance {
			err := errors.New(fmt.Sprintf("Boundary Priority Queue not in ascending order, len %d", boundaryPointsPQ.Len()))
			fmt.Println(err)
			return nil, err
		} else {
			currentBoundDistance = botBPoint.distance
		}
		/*
			forPrint = [2]float64{}
			forPrint[0] = botBPoint.vals[0] - centerData[0]
			forPrint[1] = botBPoint.vals[1] - centerData[1]
			fmt.Println(forPrint)
		*/
		extendedData = PointExtension(centerData, botBPoint.vals)
	}

	return nil, nil

}
