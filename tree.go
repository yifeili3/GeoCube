// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package geocube

import (
	"errors"
	"fmt"
	"math"
)

const defaultBufSize = 128

type Cube struct {
}

type DTreeNode struct {
	isLeaf bool
	//parent,left,right node index
	//pInd uint
	lInd uint
	rInd uint

	//min max for each dimension
	mins  []float64
	maxs  []float64
	cells []float64

	//capacity in each node for each dim
	//depends on the cache design, dimension can be sorted by priority
	//least important data comes first
	dims  []uint
	dCaps []uint

	currNum uint

	splitDim uint
	splitVal float64
}

func initTreeNode(mins []float64, maxs []float64, dims []uint, dCaps []uint) *DTreeNode {
	node := new(DTreeNode)
	node.mins = make([]float64, len(mins))
	node.maxs = make([]float64, len(maxs))
	node.dims = make([]uint, len(dims))
	node.dCaps = make([]uint, len(dCaps))

	node.cells = make([]float64, len(node.mins))
	for i, c := range node.dCaps {
		node.cells[i] = (node.maxs[i] - node.mins[i]) / c
	}
	node.isLeaf = true
	return &node
}

// Check the query values are in the min max range
// Need to by filtered out unrelated dim beforehand
func (node *DTreeNode) checkRangeByVal(queryDims []uint, queryDimVals []float64) error {
	err := nil
	for i, d := range queryDims {
		v := queryDimVals[i]
		checked := false
		for j, d2 := range node.dims {
			if d != d2 {
				continue
			}
			if v < node.mins[j] {
				err := errors.New(fmt.Sprintf("Data has value %d on dim %d, exceeds minimum", v, d))
				fmt.Println(err)
			} else if v > node.maxs[j] {
				err := errors.New(fmt.Sprintf("Data has value %d on dim %d, exceeds minimum", v, d))
				fmt.Println(err)
			}
			checked = true
		}
		if !checked {
			err := errors.New(fmt.Sprintf("Dimension %d not found", d))
			fmt.Println(err)
		}
	}
	return err
}

func (node *DTreeNode) checkRange(point *DataPoint) error {
	err := nil
	for i, d := range node.dims {
		v := point.getValByDim(d)
		if v < node.mins[i] {
			err := errors.New(fmt.Sprintf("Data has value %d on dim %d, exceeds minimum", v, d))
			fmt.Println(err)
		} else if v > node.maxs[i] {
			err := errors.New(fmt.Sprintf("Data has value %d on dim %d, exceeds minimum", v, d))
			fmt.Println(err)
		}
	}
	return err
}

func (node *DTreeNode) MapInd(point *DataPoint) int {
	mapInd1d := func(x, xmin, cell float64) int {
		//fmt.Println("")
		return int(math.Floor((x - xmin) / cell))
	}

	ind := 0
	for i, d := range node.dims {
		v := point.getValByDim(d)
		ind *= node.dCaps[i]
		ind += mapInd1d(v, node.mins[i], node.cells[i])
	}
	point.Idx = ind
	return ind
}

type DTree struct {
	nodes    []DTreeNode
	nodeData [][]DataPoint
	dims     []uint
	//capacity in each node for each dim
	dCaps      []uint
	capacity   uint
	splitThres uint

	warnings []string
}

// Initialize the DTree structure, must be called after declaration
func initTree(pDims []uint, pCaps []uint,
	splitThresRatio float64, initMins []float64,
	initMaxs []float64) *DTree {
	dTree := new(DTree)
	dTree.dims = make([]uint, len(pDims))
	dTree.dims[:] = pDims[:]

	dTree.pCaps = make([]uint, len(pCaps))
	dTree.pCaps[:] = pCaps[:]

	dTree.capacity = 1
	for _, c := range pCaps {
		dTree.capacity *= c
	}
	dTree.splitThres = uint(math.Floor(dTree.capacity * splitThresRatio))

	dTree.nodes = append(dTree.nodes, *initTreeNode(initMins, initMaxs, dTree.dims, dTree.dCaps))
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
		v := point.getValByDim(dTree.nodes[currNodeInd].splitDim)
		if v < dTree.nodes[currNodeInd].splitVal {
			currNodeInd = dTree.nodes[currNodeInd].lInd
		} else {
			currNodeInd = dTree.nodes[currNodeInd].rInd
		}
	}

	dTree.nodeData[currNodeInd] = append(dTree.nodeData[currNodeInd], *point)
	dTree.nodes[currNodeInd].currNum += 1
	dTree.nodes[currNodeInd].MapInd(&dTree.nodeData[currNodeInd])

	if dTree.nodes[currNodeInd].currNum >= splitThres {
		err := dTree.splitLeaf(currNodeInd)
		if err != nil {
			return err
		}
	}
}

// Split the specific node in Tree and update the Tree accordingly
func (dTree *DTree) splitLeaf(splitNodeInd uint) error {

	if dTree.nodes[splitNodeInd].currNum < len(dTree.nodeData[splitNodeInd]) {
		//To do: acquire data from worker, currently WRONG if data are not stored
		dTree.nodeData[splitNodeInd] = append(dTree.nodeData[splitNodeInd], dTree.nodeData[splitNodeInd])
		if dTree.nodes[splitNodeInd].currNum != len(dTree.nodeData[splitNodeInd]) {
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
			extractedData[i] = p.getValByDim(d)
		}
		targetPosition := uint(math.Floor(len(extractedData) / 2.))
		QuickSelect(quickselect.Float64Slice(extractedData), targetPosition)
		dimCandidateValue[j] = extractedData[targetPosition]

		dimCandidateMetric[j] = math.Abs(dimCandidateValue[j]-(dTree.nodeData[splitNodeInd].maxs[j]+dTree.nodeData[splitNodeInd].mins[j])/2) / (dTree.nodeData[splitNodeInd].maxs[j] - dTree.nodeData[splitNodeInd].mins[j])
	}

	bestSplit := argmax(dimCandidateMetric)
	dTree.nodes[splitNodeInd].splitDim = dTree.dims[bestSplit]
	dTree.nodes[splitNodeInd].splitThres = dimCandidateValue[bestSplit]

	leftMaxs := make([]float64, len(dTree.dims))
	leftMaxs[:] = dTree.nodeData[splitNodeInd].maxs[:]
	leftMax[bestSplit] = dimCandidateValue[bestSplit]

	rightMins := make([]float64, len(dTree.dims))
	rightMins[:] = dTree.nodeData[splitNodeInd].mins[:]
	rightMins[bestSplit] = dimCandidateValue[bestSplit]

	leftNode := initTreeNode(dTree.nodeData[splitNodeInd].mins, leftMax, dims, dCaps)
	dTree.nodes = append(dTree.nodes, *leftNode)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].lInd = len(dTree.nodes) - 1

	rightNode := initTreeNode(rightMins, dTree.nodeData[splitNodeInd].maxs, dims, dCaps)
	dTree.nodes = append(dTree.nodes, *rightNode)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].rInd = len(dTree.nodes) - 1

	// This line needs to act before assigning data
	dTree.nodes[splitNodeInd].isLeaf = false
	// move data into left right children
	for _, p := range dTree.nodeData[splitNodeInd] {
		dTree.assignData(&p, splitNodeInd)
	}
	dTree.nodeData[splitNodeInd] = nil

}

// Batch update the tree assuming the tree has been loaded in the memory
func (dTree *DTreeNode) updateTree(points []DataPoint) error {
	for i, p := range points {
		if err := dTree.assignData(&p, 0); err != nil {
			return err
		}
	}
}

// Find the correct tree node, used in KNN query and where == query
// Retrun the index of node
func (dTree *DTree) EquatlitySearch(queryDims []uint, queryDimVals []float64) ([]uint, error) {
	//remove dimensions not used in tree spliting
	var qDims []uint
	var qDimVals []float64

	var dict map[uint]bool
	for i, d := range dTree.dims {
		dict[d] = true
	}

	for i, qD := range queryDims {
		usedInSplit := false
		if elem, exists := dict[qD]; exists {
			qDims = append(qDims, qD)
			qDimVals = append(qDimVals, queryDimVals[i])
		}
	}

	// map from search dimension to value
	var qDict map[uint]float64
	for i, qD := range qDims {
		qDict[qD] = qDimVals[i]
	}

	if err := dTree.nodes[0].checkRangeByVal(qDims, qDimVals); err != nil {
		return 0, err
	}

	var finalNodeList []uint
	currNodeInd := 0
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
	finalNodeList = append(finalNodeList, currNodeInd)
	return finalNodeList, nil
}
