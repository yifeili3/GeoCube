// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package geocube

func (dTree *DTree) EqualityQuery(db *DB, query *Query) ([]DataPoint, error) {
	cubeInds, err := dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, err
	}

	var metaInds []int
	for _, cubeInd := range cubeInds {
		metaInd, err := dTree.nodes[cubeInd].MapIndByVal(query.QueryDims, query.QueryDimVals)
		if err != nil {
			return nil, err
		} else {
			metaInds = append(metaInds, metaInd)
		}
	}

	var dataPoints []DataPoint
	for i, cubeInd := range cubeInds {
		dPoints = db.ReadSingle(cubeInd, metaInds[i])
		for _, dp := range dPoints {
			if query.CheckPoint(&dp) {
				dataPoints = append(dataPoints, dp)
			}
		}

	}
	return dataPoints, nil
}
