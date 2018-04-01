package main

import "math"

type Query struct {
	QueryType int
	// QueryDims can be duplicated, so that both > < can be
	// supported at the same time
	QueryDims    []uint
	QueryDimVals []float64
	// Query Operations in each dim: 0 =; 1 >; -1 <, etc
	QueryDimOpts []int
	// Value K is QueryType = 1, KNN
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
	distance := float64(0)
	for i, d := range query.QueryDims {
		diff := dPoint.getFloatValByDim(d) - query.QueryDimVals[i]
		distance += math.Pow(diff, 2)
	}
	return math.Sqrt(distance)
}

/******* Supported Query operations ********/

/* "all"	Return all document IDs (slow!)
Find: {"eq": #, "in": [#], "limit": #}	Index value lookup
Aggregate: {"sum": , "in": [#], "limit": #}
Aggregate: {"avg": , "in": [#], "limit": #}
Range Query: {"from": #, "to": #, "in": [#], "limit": #}	Hash lookup over a range of integers

{"or": [sub-query1, sub-query2..]}	Evaluate union of sub-query results.
{"and": [sub-query1, sub-query2..]}	Evaluate intersection of sub-query results.
{"not": [sub-query1, sub-query2..]}	Evaluate complement of sub-query results.
*/

// Query operation selecting
/*
func evalQuery(query interface{}, result *map[int]struct{}) (err error) {
	switch expr := query.(type) {
	case []interface{}: // process sub query  [subquery 1, subquery 2]
		return EvalUnion(expr, src, result)
	case string:
		if expr == "all" {
			return EvalAllIDs(result)
		}
	case map[string]interface{}:
		if a := expr[""]; lookup {
			return Lookup(lookupValue, expr, result)
		} else if hasPath, exist := expr["has"]; exist {

		}
	}

}

func EvalQuery(query interface{}, result *map[int]struct{}) (err error) {
	return evalQuery(query, result)
}
*/
