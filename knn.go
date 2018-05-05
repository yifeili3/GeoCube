// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

// A PriorityQueue implements heap.Interface and holds DataPoints.
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

/*
func (pq *PQDataPoints) Push(x interface{}) {
	item := x.(*DataPoint)
	pq.points = append(pq.points, item)
	pq.distances = append(pq.distances, pq.DistanceToCenter(item))
}
*/
func (pq *PQDataPoints) Pop() interface{} {
	n := len(pq.points)
	item := pq.points[n-1]
	pq.points = pq.points[0 : n-1]
	pq.distances = pq.distances[0 : n-1]
	return item
}

/*
func (dTree *DTree) KNNQuery(db *DB, query *Query) ([]DataPoint, error) {
	cubeInds, err := dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, err
	}
	// KNN query need to gaurantee the full spatial info(or even more) is provided
	cubeInd := cubeInds[0]
	metaInd, err := dTree.Nodes[cubeInd].MapIndByVal(query.QueryDims, query.QueryDimVals)

	// TODO: BFS Implementation
	var dataPoints []DataPoint
	return dataPoints, nil

}
*/
