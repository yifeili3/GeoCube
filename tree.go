// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
)

// Too small may have truncation error
// Too large may cause stepping over
// 1e-8 is about right
const tinyMoveRatio = 0.00000001

type Cube struct {
}

type DTreeNode struct {
	IsLeaf bool
	//parent,left,right node index
	//pInd uint
	LInd uint
	RInd uint

	//min max for each dimension
	Mins     []float64
	Maxs     []float64
	CellVals []float64

	//Capacity in each node for each dim
	//depends on the cache design, dimension can be sorted by priority
	//least important data comes first
	Dims  []uint
	DCaps []uint

	Capacity uint
	CurrNum  uint

	SplitDim uint
	SplitVal float64
}

func (node *DTreeNode) initTreeNode(Mins []float64, Maxs []float64, Dims []uint, DCaps []uint) {
	node.Mins = make([]float64, len(Mins))
	copy(node.Mins, Mins)
	node.Maxs = make([]float64, len(Maxs))
	copy(node.Maxs, Maxs)
	node.Dims = make([]uint, len(Dims))
	copy(node.Dims, Dims)
	node.DCaps = make([]uint, len(DCaps))
	copy(node.DCaps, DCaps)

	node.CellVals = make([]float64, len(node.Mins))
	node.Capacity = uint(1)
	for i, c := range node.DCaps {
		node.CellVals[i] = (node.Maxs[i] - node.Mins[i]) / float64(c)
		node.Capacity *= c
	}
	node.IsLeaf = true
	node.LInd = 0 //0 is an invalid value for child ind
	node.RInd = 0
	node.CurrNum = 0
}

// Check the query values are in the min max range
// Need to by filtered out unrelated dim beforehand
func (node *DTreeNode) checkRangeByVal(queryDims []uint, queryDimVals []float64) error {
	for i, d := range queryDims {
		v := queryDimVals[i]
		checked := false
		for j, d2 := range node.Dims {
			if d != d2 {
				continue
			}
			if v < node.Mins[j] {
				err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds minimum %f", v, d, node.Mins[j]))
				fmt.Println(err)
				return err
			} else if v > node.Maxs[j] {
				err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds maximun %f", v, d, node.Maxs[j]))
				fmt.Println(err)
				return err
			}
			checked = true
		}
		if !checked {
			err := errors.New(fmt.Sprintf("Dimension %d not found", d))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

func (node *DTreeNode) checkRange(point *DataPoint) error {
	for i, d := range node.Dims {
		if int(d) >= len(point.FArr) {
			err := errors.New(fmt.Sprintf("Try to access dim %d, exceeds len %d", d, len(point.FArr)))
			fmt.Println(err)
			return err
		}
		v := point.getFloatValByDim(d)
		if v < node.Mins[i] {
			err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds minimum %f", v, d, node.Mins[i]))
			fmt.Println(err)
			return err
		} else if v > node.Maxs[i] {
			err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds maximun %f", v, d, node.Maxs[i]))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

func mapInd1d(x, xmin, cell float64) int {
	//fmt.Println("")
	return int(math.Floor((x - xmin) / cell))
}

func (node *DTreeNode) MapInd(point *DataPoint) int {

	ind := 0
	for i, d := range node.Dims {
		v := point.getFloatValByDim(d)
		ind *= int(node.DCaps[i])
		ind += mapInd1d(v, node.Mins[i], node.CellVals[i])
	}
	point.Idx = ind
	return ind
}

func (node *DTreeNode) MapIndByVal(queryDims []uint, queryDimVals []float64) (int, error) {

	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, d := range queryDims {
		qDict[d] = queryDimVals[i]
	}

	ind := 0
	for i, d := range node.Dims {
		v, exists := qDict[d]
		if !exists {
			err := errors.New(fmt.Sprintf("Dimension %d not exist in data", d))
			fmt.Println(err)
			return -1, err
		}
		ind *= int(node.DCaps[i])
		ind += mapInd1d(v, node.Mins[i], node.CellVals[i])
	}
	return ind, nil
}

func (node *DTreeNode) FixValueOrder(queryDims []uint, queryDimVals []float64) ([]float64, error) {
	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, d := range queryDims {
		qDict[d] = queryDimVals[i]
	}
	val := make([]float64, len(node.Dims))
	for i, d := range node.Dims {
		v, exists := qDict[d]
		if !exists {
			err := errors.New(fmt.Sprintf("Dimension %d not exist in data", d))
			fmt.Println(err)
			return nil, err
		}
		val[i] = v
	}
	return val, nil
}

// Given a node, return the corners of the node
func (node *DTreeNode) Corners() ([][]float64, error) {
	// The first dim of corners specify each corner
	// The second dim specifies the dimension of each value
	// Consistent to node.Dims' order
	corners := make([][]float64, int(math.Pow(2, float64(len(node.Dims)))))
	for i := range corners {
		corners[i] = make([]float64, len(node.Dims))
		j := i
		for k := range node.Dims {
			if j%2 == 0 { //min corner of that dim
				corners[i][k] = node.Mins[k]
			} else { //max corner of that dim
				corners[i][k] = node.Maxs[k]
			}
			j /= 2
		}
	}
	return corners, nil
}

// Given a central position, return the constrain point on the boundary line
// datapoint is assumed to have same dim info as node
func (node *DTreeNode) BoundaryConstrain(dataDimVals []float64) ([][]float64, error) {
	// The first dim of outliers specify each outlier
	// The second dim specifies the dimension of each value
	// Consistent to node.Dims' order
	constrainPoints := make([][]float64, 2*len(node.Dims))
	for i := range constrainPoints {
		constrainPoints[i] = make([]float64, len(node.Dims))
		copy(constrainPoints[i], dataDimVals)
		dim := i / 2
		if i%2 == 0 {
			constrainPoints[i][dim] = node.Mins[dim]
		} else {
			constrainPoints[i][dim] = node.Maxs[dim]
		}
	}
	return constrainPoints, nil
}

// Return False immediately if any requirement is not satisfied to save time
// Query Operations in each dim: 0 =; 1 >; -1 <, etc
func (node *DTreeNode) RangeCheck(queryDimVals []float64, queryDimOpts []int, qDict map[uint][]int) bool {

	for _, dim := range node.Dims {
		if _, exists := qDict[dim]; !exists {
			// Skip this dimension (satisfied)
			continue
		}
		for _, qInd := range qDict[dim] {
			if queryDimOpts[qInd] == 0 {
				if queryDimVals[qInd] < node.Mins[dim] || queryDimVals[qInd] > node.Max[dim] {
					return false
				}
			} else if queryDimOpts[qInd] < 0 && queryDimVals[qInd] < node.Mins[dim] {
				return false
			} else if queryDimVals[qInd] > node.Maxs[dim] {
				return false
			}
		}
	}
	return true
}

type DTree struct {
	Nodes    []DTreeNode
	NodeData [][]DataPoint
	Dims     []uint
	//Capacity in each node for each dim
	DCaps      []uint
	Capacity   uint
	SplitThres uint

	// nodeBatchMap []uint
	Warnings []string
}

// Initialize the DTree structure, must be called after declaration
func InitTree(pDims []uint, pCaps []uint, SplitThresRatio float64, initMins []float64, initMaxs []float64) *DTree {
	dTree := new(DTree)
	dTree.Dims = make([]uint, len(pDims))
	copy(dTree.Dims, pDims)

	dTree.DCaps = make([]uint, len(pCaps))
	copy(dTree.DCaps, pCaps)

	dTree.Capacity = 1
	for _, c := range pCaps {
		dTree.Capacity *= c
	}
	dTree.SplitThres = uint(math.Floor(float64(dTree.Capacity) * SplitThresRatio))

	dTree.Nodes = append(dTree.Nodes, DTreeNode{})
	dTree.Nodes[0].initTreeNode(initMins, initMaxs, dTree.Dims, dTree.DCaps)
	//fmt.Println(initMins)
	//fmt.Println(dTree.Nodes[0].Mins)
	dTree.NodeData = append(dTree.NodeData, nil)
	return dTree
}

// Assign single data point to the correct node
func (dTree *DTree) assignData(point *DataPoint, startNodeInd uint) error {
	currNodeInd := startNodeInd
	if startNodeInd == 0 {
		if err := dTree.Nodes[currNodeInd].checkRange(point); err != nil {
			return err
		}
	}

	//find leaf node
	for dTree.Nodes[currNodeInd].IsLeaf == false {
		v := point.getFloatValByDim(dTree.Nodes[currNodeInd].SplitDim)
		if v < dTree.Nodes[currNodeInd].SplitVal {
			currNodeInd = dTree.Nodes[currNodeInd].LInd
		} else {
			currNodeInd = dTree.Nodes[currNodeInd].RInd
		}
	}

	dTree.NodeData[currNodeInd] = append(dTree.NodeData[currNodeInd], *point)
	dTree.Nodes[currNodeInd].CurrNum += 1
	dTree.Nodes[currNodeInd].MapInd(&dTree.NodeData[currNodeInd][len(dTree.NodeData[currNodeInd])-1])

	//!!!fmt.Printf("NodeInd, %d, Threshold number: %d, current length %d\n", currNodeInd, int(dTree.SplitThres), len(dTree.NodeData[currNodeInd]))
	if len(dTree.NodeData[currNodeInd]) >= int(dTree.SplitThres) {
		//if dTree.Nodes[currNodeInd].CurrNum >= dTree.SplitThres {
		//fmt.Println("split")
		err := dTree.splitLeaf(currNodeInd)
		if err != nil {
			return err
		}
	}
	return nil
}

func (dTree *DTree) MedianDeviation(splitNodeInd uint) (bestSplit int, SplitDim uint, SplitVal float64) {
	dimCandidateValue := make([]float64, len(dTree.Dims))
	dimCandidateMetric := make([]float64, len(dTree.Dims))
	for j, d := range dTree.Dims {
		extractedData := make([]float64, len(dTree.NodeData[splitNodeInd]))
		for i, p := range dTree.NodeData[splitNodeInd] {
			extractedData[i] = p.getFloatValByDim(d)
		}
		targetPosition := len(extractedData) / 2
		QuickSelect(Float64Slice(extractedData), targetPosition)
		dimCandidateValue[j] = extractedData[targetPosition]

		dimCandidateMetric[j] = math.Abs(dimCandidateValue[j]-
			(dTree.Nodes[splitNodeInd].Maxs[j]+dTree.Nodes[splitNodeInd].Mins[j])/2.) /
			(dTree.Nodes[splitNodeInd].Maxs[j] - dTree.Nodes[splitNodeInd].Mins[j])
	}

	bestSplit = argmax(dimCandidateMetric)
	SplitDim = dTree.Dims[bestSplit]
	SplitVal = dimCandidateValue[bestSplit]
	return
}

/*
func (dTree *DTree) GiniCoeifficient(splitNodeInd uint) (SplitDim uint, SplitVal float64) {
}
*/

/*
useHeuristic = 0: argMax, to split on uniform distributed (to solve the medium skew issue)
useHeuristic = 1: argMin, to split on skewly distributed (to make child Nodes unformly distributed)
useHeuristic = 2: argMin first, when medium skew occurs, use argMax
*/
func (dTree *DTree) DiscreteEntropy(splitNodeInd uint, useHeuristic uint) (bestSplit int, SplitDim uint, SplitVal float64) {

	// Compute entropy for each dim
	entropies := make([]float64, len(dTree.Dims))
	for j, d := range dTree.Dims {

		columnCount := make([]float64, int(dTree.Nodes[splitNodeInd].DCaps[j]))
		for i, _ := range columnCount {
			columnCount[i] = 0
		}

		xmin := dTree.Nodes[splitNodeInd].Mins[j]
		cell := dTree.Nodes[splitNodeInd].CellVals[j]
		for _, data := range dTree.NodeData[splitNodeInd] {
			val := data.getFloatValByDim(d)
			columnCount[mapInd1d(val, xmin, cell)] += 1
		}
		totalCount := float64(len(dTree.NodeData[splitNodeInd]))
		entropies[j] = float64(0)
		for _, count := range columnCount {
			pValue := count / totalCount
			if pValue > 0 {
				entropies[j] += -pValue * math.Log2(pValue)
			}
		}
	}
	//fmt.Printf("entropy, %f, %f\n", entropies[0], entropies[1])
	if useHeuristic < 2 {
		if useHeuristic == 0 {
			bestSplit = argmax(entropies)
		} else {
			bestSplit = argmin(entropies)
		}
		SplitDim = dTree.Dims[bestSplit]
		//fmt.Printf("Best splt  %d, dim %d, val %f\n", bestSplit, SplitDim, SplitVal)
		extractedData := make([]float64, len(dTree.NodeData[splitNodeInd]))
		for i, p := range dTree.NodeData[splitNodeInd] {
			extractedData[i] = p.getFloatValByDim(SplitDim)
		}
		targetPosition := len(extractedData) / 2
		QuickSelect(Float64Slice(extractedData), targetPosition)
		SplitVal = extractedData[targetPosition]
		return
	} else {
		bestSplit = argmin(entropies)
		SplitDim = dTree.Dims[bestSplit]
		extractedData := make([]float64, len(dTree.NodeData[splitNodeInd]))
		for i, p := range dTree.NodeData[splitNodeInd] {
			extractedData[i] = p.getFloatValByDim(SplitDim)
		}
		targetPosition := len(extractedData) / 2
		QuickSelect(Float64Slice(extractedData), targetPosition)
		SplitVal = extractedData[targetPosition]

		leftNum := float64(0)
		rightNum := float64(0)
		for _, p := range extractedData {
			if p < SplitVal {
				leftNum += 1
			} else {
				rightNum += 1
			}
		}
		sqr := (leftNum + rightNum) * (leftNum + rightNum)
		if leftNum*rightNum/sqr > 0.2 {
			//fmt.Printf("Best splt  %d, dim %d, val %f\n", bestSplit, SplitDim, SplitVal)
			return
		}

		bestSplit = argmax(entropies)
		SplitDim = dTree.Dims[bestSplit]
		//fmt.Printf("Best splt  %d, dim %d, val %f\n", bestSplit, SplitDim, SplitVal)
		extractedData = make([]float64, len(dTree.NodeData[splitNodeInd]))
		for i, p := range dTree.NodeData[splitNodeInd] {
			extractedData[i] = p.getFloatValByDim(SplitDim)
		}
		QuickSelect(Float64Slice(extractedData), targetPosition)
		SplitVal = extractedData[targetPosition]
		return
	}
}

// Split the specific node in Tree and update the Tree accordingly
func (dTree *DTree) splitLeaf(splitNodeInd uint) error {
	//fmt.Printf("Split node index: %d\n", splitNodeInd)

	if dTree.Nodes[splitNodeInd].CurrNum < uint(len(dTree.NodeData[splitNodeInd])) {
		//To do: acquire data from worker, currently WRONG if data are not stored
		dTree.NodeData[splitNodeInd] = append(dTree.NodeData[splitNodeInd], dTree.NodeData[splitNodeInd][0])
		if dTree.Nodes[splitNodeInd].CurrNum != uint(len(dTree.NodeData[splitNodeInd])) {
			err := errors.New(fmt.Sprintf("Incomplete data on node %d", splitNodeInd))
			fmt.Println(err)
			return err
		}
	}

	bestSplit := int(0) // index of best split dim
	//bestSplit, dTree.Nodes[splitNodeInd].SplitDim, dTree.Nodes[splitNodeInd].SplitVal = dTree.MedianDeviation(splitNodeInd)
	bestSplit, dTree.Nodes[splitNodeInd].SplitDim, dTree.Nodes[splitNodeInd].SplitVal = dTree.DiscreteEntropy(splitNodeInd, 2)

	/*
		fmt.Printf("SplitDim %d, splitval %.15f\n", dTree.Nodes[splitNodeInd].SplitDim, dTree.Nodes[splitNodeInd].SplitVal)
		for _, p := range dTree.NodeData[splitNodeInd] {
			fmt.Println(p.getFloatValByDim(dTree.Nodes[splitNodeInd].SplitDim), p.getFloatValByDim(uint(1-dTree.Nodes[splitNodeInd].SplitDim)))
		}*/

	leftMaxs := make([]float64, len(dTree.Dims))
	copy(leftMaxs, dTree.Nodes[splitNodeInd].Maxs)
	leftMaxs[bestSplit] = dTree.Nodes[splitNodeInd].SplitVal

	rightMins := make([]float64, len(dTree.Dims))
	copy(rightMins, dTree.Nodes[splitNodeInd].Mins)
	rightMins[bestSplit] = dTree.Nodes[splitNodeInd].SplitVal

	dTree.Nodes = append(dTree.Nodes, DTreeNode{})
	leftInd := uint(len(dTree.Nodes) - 1)
	dTree.Nodes[leftInd].initTreeNode(dTree.Nodes[splitNodeInd].Mins, leftMaxs, dTree.Dims, dTree.DCaps)
	dTree.NodeData = append(dTree.NodeData, nil)
	dTree.Nodes[splitNodeInd].LInd = leftInd

	dTree.Nodes = append(dTree.Nodes, DTreeNode{})
	rightInd := leftInd + 1
	dTree.Nodes[rightInd].initTreeNode(rightMins, dTree.Nodes[splitNodeInd].Maxs, dTree.Dims, dTree.DCaps)
	dTree.NodeData = append(dTree.NodeData, nil)
	dTree.Nodes[splitNodeInd].RInd = rightInd

	// This line needs to act before assigning data
	dTree.Nodes[splitNodeInd].IsLeaf = false
	// move data into left right children
	//!!!fmt.Printf("Start assign after split, node ind %d\n", splitNodeInd)
	//!!!fmt.Printf("Num to assign %d\n", len(dTree.NodeData[splitNodeInd]))
	for _, p := range dTree.NodeData[splitNodeInd] {
		dTree.assignData(&p, splitNodeInd)
	}
	dTree.NodeData[splitNodeInd] = nil
	return nil
}

// Batch update the tree assuming the tree has been loaded in the memory
func (dTree *DTree) UpdateTree(points []DataPoint) error {
	perm := rand.Perm(len(points))
	for _, pInd := range perm {
		//fmt.Printf("Assign %d th new data \n", i+1)
		if err := dTree.assignData(&points[pInd], 0); err != nil {
			fmt.Printf("Error happen in data index %d \n", pInd)
			return err
		} else {
			// Debug
			//for _, n := range dTree.Nodes {
			//	fmt.Printf("####%d, %d\n", n.LInd, n.RInd)
			//}
			//fmt.Println(" successfully imported")
		}

	}
	return nil
}

// Find the correct tree node, used in KNN query and where == query
// Retrun the index of node
func (dTree *DTree) EquatlitySearch(queryDims []uint, queryDimVals []float64) ([]int, error) {
	//remove dimensions not used in tree spliting
	//fmt.Println(queryDims)
	//fmt.Println(queryDimVals)
	var qDims []uint
	var qDimVals []float64

	var dict map[uint]bool
	dict = make(map[uint]bool, 4)
	for _, d := range dTree.Dims {
		dict[d] = true
	}

	for i, qD := range queryDims {
		if _, exists := dict[qD]; exists {
			qDims = append(qDims, qD)
			qDimVals = append(qDimVals, queryDimVals[i])
		}
	}

	// map from search dimension to value
	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, qD := range qDims {
		qDict[qD] = qDimVals[i]
	}

	if err := dTree.Nodes[0].checkRangeByVal(qDims, qDimVals); err != nil {
		return []int{0}, err
	}

	var finalNodeList []int
	currNodeInd := uint(0)

	for dTree.Nodes[currNodeInd].IsLeaf == false {
		v := qDict[dTree.Nodes[currNodeInd].SplitDim] //Change Later
		if v < dTree.Nodes[currNodeInd].SplitVal {
			currNodeInd = dTree.Nodes[currNodeInd].LInd
		} else {
			currNodeInd = dTree.Nodes[currNodeInd].RInd
		}
	}

	// TODO: Remove this line later
	finalNodeList = append(finalNodeList, int(currNodeInd))
	return finalNodeList, nil
}

// Find all related tree nodes
// Retrun the indices of node
func (dTree *DTree) RangeSearch(queryDims []uint, queryDimVals []float64, queryDimOpts []int) ([]int, error) {
	//remove dimensions not used in tree spliting
	//fmt.Println(queryDims)
	//fmt.Println(queryDimVals)
	var qDims []uint
	var qDimVals []float64
	var qDimOpts []int

	var dict map[uint]bool
	dict = make(map[uint]bool, 4)
	for _, d := range dTree.Dims {
		dict[d] = true
	}

	for i, qD := range queryDims {
		if _, exists := dict[qD]; exists {
			qDims = append(qDims, qD)
			qDimVals = append(qDimVals, queryDimVals[i])
			qDimOpts = append(qDimOpts, queryDimOpts[i])
		}
	}

	// map from search dimension to ARRAY OF INDEX
	var qDict map[uint][]int
	qDict = make(map[uint][]int, 4)
	for i, qD := range qDims {
		if _, exists := qDict[qD]; exists {
			qDict[qD] = append(qDict[qD], i)
		} else {
			qDict[qD] = make([]int, 0)
			qDict[qD] = append(qDict[qD], i)
		}
	}

	finalNodeList := make([]int, 0)
	currList := [0]int{}
	nextList := [1]int{0}
	// find the list of related leaf nodes
	for len(nextList) > 0 {
		currList = nextList
		nextList := make([]int, 0)
		for _, nodeInd := range currList {
			if dTree.Nodes[nodeInd].RangeCheck(qDimVals, qDimOpts, qDict) {
				if dTree.Nodes[nodeInd].IsLeaf {
					finalNodeList = append(finalNodeList, nodeInd)
				} else {
					nextList = append(nextList, dTree.Nodes[nodeInd].LInd)
					nextList = append(nextList, dTree.Nodes[nodeInd].RInd)
				}
			}
		}
	}

	return finalNodeList, nil
}

func (dTree *DTree) ToDataBatch() []DataBatch {
	var dataBatches []DataBatch
	for i, node := range dTree.Nodes {
		if node.IsLeaf {
			dataBatches = append(dataBatches, DataBatch{i, node.Capacity, node.Dims, node.Mins, node.Maxs, dTree.NodeData[i]})
		}
		dTree.NodeData[i] = nil
	}
	return dataBatches
}

/*
Convert the DTree information into a string format
*/
func (dTree *DTree) ToString(filename string) []byte {

	mResult, err := json.Marshal(dTree)
	if err != nil {
		fmt.Println("Error Converting Treee to String:", err)
	}
	if filename != "" {
		err = ioutil.WriteFile(filename, mResult, 0644)
		if err != nil {
			fmt.Println("Error Writing Tree file:", err)
		}
	}
	return mResult
}

/*
Load the DTree string and construct a DTree structure
*/
func LoadDTree(filename string, jsonArray []byte) *DTree {
	var err error
	err = nil
	if filename != "" {
		jsonArray, err = ioutil.ReadFile(filename)
		if err != nil {
			fmt.Println("Error Read Tree File:", err)
		}
	}

	dTree := new(DTree)
	if jsonArray != nil {
		err = json.Unmarshal(jsonArray, &dTree)
		if err != nil {
			fmt.Println("Error Parse Json Tree:", err)
		}
	}
	return dTree
}
