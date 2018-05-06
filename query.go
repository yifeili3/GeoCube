package main

import (
	"errors"
	"fmt"
	"math"
)

type Query struct {
	//QueryType = 0, equal, 1, range, 2, knn
	QueryType int
	// QueryDims can be duplicated, so that both > < can be
	// supported at the same time
	QueryDims    []uint
	QueryDimVals []float64
	// Query Operations in each dim: 0 =; 1 >; -1 <, etc
	QueryDimOpts []int
	// Value K is QueryType = 2, KNN
	K int
	// Later Usage
	Client string
}

func InitQuery(qType int, qDims []uint, qDimVals []float64, qDimOpts []int, k int, client string) *Query {
	q := new(Query)
	q.QueryType = qType
	q.QueryDims = make([]uint, len(qDims))
	copy(q.QueryDims, qDims)
	q.QueryDimVals = make([]float64, len(qDimVals))
	copy(q.QueryDimVals, qDimVals)
	q.QueryDimOpts = make([]int, len(qDimOpts))
	copy(q.QueryDimOpts, qDimOpts)

	q.K = k
	q.Client = client
	return q
}

// Check Whether DataPoint satisfies the query requirement
func (query *Query) CheckPoint(dPoint *DataPoint) bool {
	for i, d := range query.QueryDims {
		diff := dPoint.getFloatValByDim(d) - query.QueryDimVals[i]
		if query.QueryDimOpts[i] == 0 {
			if diff != 0 {
				return false
			}
		} else {
			if diff*float64(query.QueryDimOpts[i]) < 0 {
				return false
			}
		}
	}
	return true
}

// Compute the Euclidean distance between the datapoint and query center
func (query *Query) DistanceToCenter(dPoint *DataPoint) float64 {
	if query.QueryType != 2 || query.K <= 0 {
		return float64(-1)
	}
	distance := float64(0)
	for i, d := range query.QueryDims {
		diff := dPoint.getFloatValByDim(d) - query.QueryDimVals[i]
		distance += math.Pow(diff, 2)
	}
	return math.Sqrt(distance)
}

// Convert the query to a float array storing the knn center info
func (query *Query) ToDimFloatVal(dTree *DTree) ([]float64, error) {
	if query.K < 0 {
		err := errors.New(fmt.Sprintf("Query is not KNN, but tries to convert fake data"))
		fmt.Println(err)
		return nil, err
	}

	// For safety in future, need to do dimension check

	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, d := range query.QueryDims {
		qDict[d] = query.QueryDimVals[i]
	}

	centerData := make([]float64, len(dTree.Dims))
	for i, d := range dTree.Dims {
		centerData[i] = qDict[d]
	}
	return centerData, nil
}
