// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"fmt"
)

type Worker struct {
	dTree *DTree
}

func (worker *Worker) EqualityQuery(db *DB, query *Query) ([]DataPoint, error) {
	cubeInds, err := worker.dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, err
	}
	//fmt.Println(cubeInds)

	var metaInds []int
	for _, cubeInd := range cubeInds {
		metaInd, err := worker.dTree.nodes[cubeInd].MapIndByVal(query.QueryDims, query.QueryDimVals)
		if err != nil {
			return nil, err
		} else {
			metaInds = append(metaInds, metaInd)
		}
	}

	var dataPoints []DataPoint
	for i, cubeInd := range cubeInds {

		dPoints := db.ReadSingle(cubeInd, metaInds[i])
		//fmt.Println(fmt.Sprintf("CubeInd: %d, MetaInd %d", cubeInd, metaInds[i]))
		//fmt.Println(dPoints)
		for _, dp := range dPoints {
			if query.CheckPoint(&dp) {
				//fmt.Println("found")
				dataPoints = append(dataPoints, dp)
			}
		}
	}
	return dataPoints, nil
}

func (dTree *DTree) KNNQuery(db *DB, query *Query) ([]DataPoint, error) {
	cubeInds, err := dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, err
	}
	// KNN query need to gaurantee the full spatial info(or even more) is provided
	cubeInd := cubeInds[0]
	metaInd, err := dTree.nodes[cubeInd].MapIndByVal(query.QueryDims, query.QueryDimVals)
	fmt.Println(metaInd)

	// TODO: BFS Implementation
	var dataPoints []DataPoint
	return dataPoints, nil
}
