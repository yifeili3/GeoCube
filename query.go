package geocube

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
