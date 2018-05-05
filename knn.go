// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"container/heap"
	"errors"
	"fmt"
	"math"
)

const (
	extensionRatio float64 = 1. + 1e-13
)

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
	for i, cubeInd := range cachedCube {
		if err := dTree.Nodes[cubeInd].checkRangeByVal(nil, extendedData); err == nil {
			// nil err means the data is within range
			return cubeInd
		}
	}
	return -1
}

func (worker *Worker) KNNQuery(db *DB, query *Query) ([]DataPoint, error) {
	centerData, err := query.ToDimFloatVal(worker.dTree)
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

	for len(outputDataPoints) < query.K {
		extensionValid := true
		cubeInd := CheckCachedCube(worker.dTree, cachedCube, extendedData)
		if cubeInd == -1 {
			cubeInds, err = worker.dTree.EquatlitySearch(nil, extendedData)
			if err != nil {
				if cubeInds != nil && cubeInds[0] == -1 {
					// Possible Non-error condition: extended Point is out of the DTree range
					// Need to skip current extended Point
					extensionValid = false
				} else {
					return nil, err
				}
			}
			cubeInd = cubeInds[0]
		}

		if extensionValid {
			currMetaInd, err := worker.dTree.Nodes[cubeInd].MapIndByVal(nil, extendedData)
			if err != nil {
				return nil, err
			}

			metaIndList := make([]int, 1)
			metaIndList[0] = currMetaInd
			//Perform readBatch to force using cache, since ReadSingle doesn't cache metaCube
			dataPoints := db.ReadBatch(cubeInd, metaIndList)
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
					knnBp := new(KNNPoint)
					knnBp.vals = cP
					knnBp.distance = PointDistance(centerData, cP)
					// Prevent duplicate boundary point insertion (which will cause infinite loop)
					if knnBp.distance > currentBoundDistance {
						heap.Push(boundaryPointsPQ, knnBp)
					}
				}
			}
			boundaryPoints, err := worker.dTree.Nodes[cubeInd].BoundaryConstrain(centerData, currMetaInd)
			if err != nil {
				return nil, err
			} else {
				for _, cP := range boundaryPoints {
					knnBp := new(KNNPoint)
					knnBp.vals = cP
					knnBp.distance = PointDistance(centerData, cP)
					// Prevent duplicate boundary point insertion (which will cause infinite loop)
					if knnBp.distance > currentBoundDistance {
						heap.Push(boundaryPointsPQ, knnBp)
					}
				}
			}
		}

		botDPoint := heap.Pop(dataPointsPQ).(*KNNPoint)
		botBPoint := heap.Pop(boundaryPointsPQ).(*KNNPoint)

		// TODO: Need to dump redundant boundaryConstrains
		// For example, the previous extended point is pushed back as well
		// Need to analyze whether this condition will happen
		for botDPoint.distance < botBPoint.distance {
			// Check algorithm/implementation correctness, and track current max distance
			if botDPoint.distance < currentDataDistance {
				err := errors.New(fmt.Sprintf("Data Priority Queue not in ascending order, len %d", dataPointsPQ.Len()))
				fmt.Println(err)
				return nil, err
			} else {
				currentBoundDistance = botDPoint.distance
			}
			outputDataPoints = append(outputDataPoints, *(botDPoint.dPoint))
			botDPoint = heap.Pop(dataPointsPQ).(*KNNPoint)
			// immediately return if enough points are found
			if len(outputDataPoints) == query.K {
				return outputDataPoints, nil
			}
		}

		// Then, current DPoint should be push back again
		heap.Push(dataPointsPQ, botDPoint)
		if botBPoint.distance < currentBoundDistance {
			err := errors.New(fmt.Sprintf("Boundary Priority Queue not in ascending order, len %d", boundaryPointsPQ.Len()))
			fmt.Println(err)
			return nil, err
		} else {
			currentBoundDistance = botBPoint.distance
		}
		extendedPoint := PointExtension(centerData, botBPoint.vals)

	}

	return nil, nil

}
