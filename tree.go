// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"errors"
	"fmt"
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
	isLeaf bool
	//parent,left,right node index
	//pInd uint
	lInd uint
	rInd uint

	//min max for each dimension
	mins     []float64
	maxs     []float64
	cellVals []float64

	//capacity in each node for each dim
	//depends on the cache design, dimension can be sorted by priority
	//least important data comes first
	dims  []uint
	dCaps []uint

	capacity uint
	currNum  uint

	splitDim uint
	splitVal float64
}

func (node *DTreeNode) initTreeNode(mins []float64, maxs []float64, dims []uint, dCaps []uint) {
	node.mins = make([]float64, len(mins))
	copy(node.mins, mins)
	node.maxs = make([]float64, len(maxs))
	copy(node.maxs, maxs)
	node.dims = make([]uint, len(dims))
	copy(node.dims, dims)
	node.dCaps = make([]uint, len(dCaps))
	copy(node.dCaps, dCaps)

	node.cellVals = make([]float64, len(node.mins))
	node.capacity = uint(1)
	for i, c := range node.dCaps {
		node.cellVals[i] = (node.maxs[i] - node.mins[i]) / float64(c)
		node.capacity *= c
	}
	node.isLeaf = true
	node.lInd = 0 //0 is an invalid value for child ind
	node.rInd = 0
	node.currNum = 0
}

// Check the query values are in the min max range
// Need to by filtered out unrelated dim beforehand
func (node *DTreeNode) checkRangeByVal(queryDims []uint, queryDimVals []float64) error {
	for i, d := range queryDims {
		v := queryDimVals[i]
		checked := false
		for j, d2 := range node.dims {
			if d != d2 {
				continue
			}
			if v < node.mins[j] {
				err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds minimum %f", v, d, node.mins[j]))
				fmt.Println(err)
				return err
			} else if v > node.maxs[j] {
				err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds maximun %f", v, d, node.maxs[j]))
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
	for i, d := range node.dims {
		if int(d) >= len(point.FArr) {
			err := errors.New(fmt.Sprintf("Try to access dim %d, exceeds len %d", d, len(point.FArr)))
			fmt.Println(err)
			return err
		}
		v := point.getFloatValByDim(d)
		if v < node.mins[i] {
			err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds minimum %f", v, d, node.mins[i]))
			fmt.Println(err)
			return err
		} else if v > node.maxs[i] {
			err := errors.New(fmt.Sprintf("Data has value %f on dim %d, exceeds maximun %f", v, d, node.maxs[i]))
			fmt.Println(err)
			return err
		}
	}
	return nil
}

func (node *DTreeNode) MapInd(point *DataPoint) int {
	mapInd1d := func(x, xmin, cell float64) int {
		//fmt.Println("")
		return int(math.Floor((x - xmin) / cell))
	}

	ind := 0
	for i, d := range node.dims {
		v := point.getFloatValByDim(d)
		ind *= int(node.dCaps[i])
		ind += mapInd1d(v, node.mins[i], node.cellVals[i])
	}
	point.Idx = ind
	return ind
}

func (node *DTreeNode) MapIndByVal(queryDims []uint, queryDimVals []float64) (int, error) {
	mapInd1d := func(x, xmin, cell float64) int {
		//fmt.Println("")
		return int(math.Floor((x - xmin) / cell))
	}

	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, d := range queryDims {
		qDict[d] = queryDimVals[i]
	}

	ind := 0
	for i, d := range node.dims {
		v, exists := qDict[d]
		if !exists {
			err := errors.New(fmt.Sprintf("Dimension %d not exist in data", d))
			fmt.Println(err)
			return -1, err
		}
		ind *= int(node.dCaps[i])
		ind += mapInd1d(v, node.mins[i], node.cellVals[i])
	}
	return ind, nil
}

func (node *DTreeNode) FixValueOrder(queryDims []uint, queryDimVals []float64) ([]float64, error) {
	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, d := range queryDims {
		qDict[d] = queryDimVals[i]
	}
	val := make([]float64, len(node.dims))
	for i, d := range node.dims {
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
	// Consistent to node.dims' order
	corners := make([][]float64, int(math.Pow(2, float64(len(node.dims)))))
	for i := range corners {
		corners[i] = make([]float64, len(node.dims))
		j := i
		for k := range node.dims {
			if j%2 == 0 { //min corner of that dim
				corners[i][k] = node.mins[k]
			} else { //max corner of that dim
				corners[i][k] = node.maxs[k]
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
	// Consistent to node.dims' order
	constrainPoints := make([][]float64, 2*len(node.dims))
	for i := range constrainPoints {
		constrainPoints[i] = make([]float64, len(node.dims))
		copy(constrainPoints[i], dataDimVals)
		dim := i / 2
		if i%2 == 0 {
			constrainPoints[i][dim] = node.mins[dim]
		} else {
			constrainPoints[i][dim] = node.maxs[dim]
		}
	}
	return constrainPoints, nil
}

type DTree struct {
	nodes    []DTreeNode
	nodeData [][]DataPoint
	dims     []uint
	//capacity in each node for each dim
	dCaps      []uint
	capacity   uint
	splitThres uint

	// nodeBatchMap []uint
	warnings []string
}

// Initialize the DTree structure, must be called after declaration
func InitTree(pDims []uint, pCaps []uint, splitThresRatio float64, initMins []float64, initMaxs []float64) *DTree {
	dTree := new(DTree)
	dTree.dims = make([]uint, len(pDims))
	copy(dTree.dims, pDims)

	dTree.dCaps = make([]uint, len(pCaps))
	copy(dTree.dCaps, pCaps)

	dTree.capacity = 1
	for _, c := range pCaps {
		dTree.capacity *= c
	}
	dTree.splitThres = uint(math.Floor(float64(dTree.capacity) * splitThresRatio))

	dTree.nodes = append(dTree.nodes, DTreeNode{})
	dTree.nodes[0].initTreeNode(initMins, initMaxs, dTree.dims, dTree.dCaps)
	//fmt.Println(initMins)
	//fmt.Println(dTree.nodes[0].mins)
	dTree.nodeData = append(dTree.nodeData, nil)
	return dTree
}

// Assign single data point to the correct node
func (dTree *DTree) assignData(point *DataPoint, startNodeInd uint) error {
	currNodeInd := startNodeInd
	if startNodeInd == 0 {
		if err := dTree.nodes[currNodeInd].checkRange(point); err != nil {
			return err
		}
	}

	//find leaf node
	for dTree.nodes[currNodeInd].isLeaf == false {
		v := point.getFloatValByDim(dTree.nodes[currNodeInd].splitDim)
		if v < dTree.nodes[currNodeInd].splitVal {
			currNodeInd = dTree.nodes[currNodeInd].lInd
		} else {
			currNodeInd = dTree.nodes[currNodeInd].rInd
		}
	}

	dTree.nodeData[currNodeInd] = append(dTree.nodeData[currNodeInd], *point)
	dTree.nodes[currNodeInd].currNum += 1
	dTree.nodes[currNodeInd].MapInd(&dTree.nodeData[currNodeInd][len(dTree.nodeData[currNodeInd])-1])
	//fmt.Println(currNodeInd)

	if dTree.nodes[currNodeInd].currNum >= dTree.splitThres {
		//fmt.Println("split")
		err := dTree.splitLeaf(currNodeInd)
		if err != nil {
			return err
		}
	}
	return nil
}

// Split the specific node in Tree and update the Tree accordingly
func (dTree *DTree) splitLeaf(splitNodeInd uint) error {

	if dTree.nodes[splitNodeInd].currNum < uint(len(dTree.nodeData[splitNodeInd])) {
		//To do: acquire data from worker, currently WRONG if data are not stored
		dTree.nodeData[splitNodeInd] = append(dTree.nodeData[splitNodeInd], dTree.nodeData[splitNodeInd][0])
		if dTree.nodes[splitNodeInd].currNum != uint(len(dTree.nodeData[splitNodeInd])) {
			err := errors.New(fmt.Sprintf("Incomplete data on node %d", splitNodeInd))
			fmt.Println(err)
			return err
		}
	}

	dimCandidateValue := make([]float64, len(dTree.dims))
	dimCandidateMetric := make([]float64, len(dTree.dims))
	for j, d := range dTree.dims {
		extractedData := make([]float64, len(dTree.nodeData[splitNodeInd]))
		for i, p := range dTree.nodeData[splitNodeInd] {
			extractedData[i] = p.getFloatValByDim(d)
		}
		targetPosition := len(extractedData) / 2
		QuickSelect(Float64Slice(extractedData), targetPosition)
		dimCandidateValue[j] = extractedData[targetPosition]

		dimCandidateMetric[j] = math.Abs(dimCandidateValue[j]-
			(dTree.nodes[splitNodeInd].maxs[j]+dTree.nodes[splitNodeInd].mins[j])/2.) /
			(dTree.nodes[splitNodeInd].maxs[j] - dTree.nodes[splitNodeInd].mins[j])
	}

	bestSplit := argmax(dimCandidateMetric)
	dTree.nodes[splitNodeInd].splitDim = dTree.dims[bestSplit]
	dTree.nodes[splitNodeInd].splitVal = dimCandidateValue[bestSplit]

	leftMaxs := make([]float64, len(dTree.dims))
	copy(leftMaxs, dTree.nodes[splitNodeInd].maxs)
	leftMaxs[bestSplit] = dTree.nodes[splitNodeInd].splitVal

	rightMins := make([]float64, len(dTree.dims))
	copy(rightMins, dTree.nodes[splitNodeInd].mins)
	rightMins[bestSplit] = dTree.nodes[splitNodeInd].splitVal

	dTree.nodes = append(dTree.nodes, DTreeNode{})
	leftInd := uint(len(dTree.nodes) - 1)
	dTree.nodes[leftInd].initTreeNode(dTree.nodes[splitNodeInd].mins, leftMaxs, dTree.dims, dTree.dCaps)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].lInd = leftInd

	dTree.nodes = append(dTree.nodes, DTreeNode{})
	rightInd := leftInd + 1
	dTree.nodes[rightInd].initTreeNode(rightMins, dTree.nodes[splitNodeInd].maxs, dTree.dims, dTree.dCaps)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].rInd = rightInd

	// This line needs to act before assigning data
	dTree.nodes[splitNodeInd].isLeaf = false
	// move data into left right children
	for _, p := range dTree.nodeData[splitNodeInd] {
		dTree.assignData(&p, splitNodeInd)
	}
	dTree.nodeData[splitNodeInd] = nil
	return nil
}

// Batch update the tree assuming the tree has been loaded in the memory
func (dTree *DTree) UpdateTree(points []DataPoint) error {
	perm := rand.Perm(len(points))
	for _, pInd := range perm {
		if err := dTree.assignData(&points[pInd], 0); err != nil {
			fmt.Printf("Error happen in data index %d \n", pInd)
			return err
		} else {
			// Debug
			//for _, n := range dTree.nodes {
			//	fmt.Printf("####%d, %d\n", n.lInd, n.rInd)
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
	for _, d := range dTree.dims {
		dict[d] = true
	}

	for i, qD := range queryDims {
		if _, exists := dict[qD]; exists {
			qDims = append(qDims, qD)
			qDimVals = append(qDimVals, queryDimVals[i])
		}
	}
	//fmt.Println(qDims)
	//fmt.Println(qDimVals)

	// map from search dimension to value
	var qDict map[uint]float64
	qDict = make(map[uint]float64, 4)
	for i, qD := range qDims {
		qDict[qD] = qDimVals[i]
	}
	//fmt.Println(qDict)

	if err := dTree.nodes[0].checkRangeByVal(qDims, qDimVals); err != nil {
		return []int{0}, err
	}

	var finalNodeList []int
	currNodeInd := uint(0)
	// TODO: To make it able to go to two branches: currList, nextList, finalList
	// find the correct leaf node
	for dTree.nodes[currNodeInd].isLeaf == false {
		v := qDict[dTree.nodes[currNodeInd].splitDim] //Change Later
		if v < dTree.nodes[currNodeInd].splitVal {
			currNodeInd = dTree.nodes[currNodeInd].lInd
		} else {
			currNodeInd = dTree.nodes[currNodeInd].rInd
		}
	}

	// TODO: Remove this line later
	finalNodeList = append(finalNodeList, int(currNodeInd))
	return finalNodeList, nil
}

func (dTree *DTree) ToDataBatch() []DataBatch {
	var dataBatches []DataBatch
	for i, node := range dTree.nodes {
		if node.isLeaf {
			dataBatches = append(dataBatches, DataBatch{i, node.capacity, node.dims, node.mins, node.maxs, dTree.nodeData[i]})
		}
		dTree.nodeData[i] = nil
	}
	return dataBatches
}
