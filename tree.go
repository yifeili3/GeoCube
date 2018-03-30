// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package geocube

import (
	"math"
	"errors"
	"fmt"
	"external_libs/quickselect"
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

func initTreeNode(mins, maxs, dims, dCaps) *DTreeNode {
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

func (node *DTreeNode) checkRange(point *DataPoint) error {
	err := nil
	for i, d := range node.dims {
		v := point.getByDim(d)
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

func (node *DTreeNode) MapInd(point *DataPoint) uint {
	mapInd1d := func(x, xmin, cell float64) int {
		//fmt.Println("")
		return int(math.Floor((x - xmin) / cell))
	}

	ind := 0
	for i, d := range node.dims {
		v := point.getByDim(d)
		ind *= node.dCaps[i]
		ind += mapInd1d(v, node.mins[i], node.cells[i])
	}
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
func initTree(pDims []uint, pCaps []uint, splitThresRatio float64, initMins, initMaxs []float64) *DTree {
	dTree := new(DTree)
	dTree.dims = make([]uint, len(pDims))
	dTree.dims[:] = pDims[:]

	dTree.pCaps = make([]uint, len(pCaps))
	dTree.pCaps[:] = pCaps[:]

	dTree.capacity = 1
	for _,c := range pCaps{
		dTree.capacity *= c
	}
	dTree.splitThres = uint(math.Floor(dTree.capacity * splitThresRatio))

	dTree.nodes = make([]DTreeNode, 1)
	dTree.nodeData = make([][]DataPoint, 1)

	dTree.nodes[0] = initTreeNode(initMins, initMaxs, dTree.dims, dTree.dCaps)
	return dTree
}

// Assign single data point to the correct node 
func (dTree *DTree) assignData(point *DataPoint, startNodeInd uint) error {
	currNodeInd := startNodeInd
	if startNodeInd == 0{
		if err := dTree.nodes[currNodeInd].checkRange(point); err != nil{
			return err
		}	
	}

	//find leaf node
	for dTree.nodes[currNodeInd].isLeaf == false {
		v := point.getByDim(dTree.nodes[currNodeInd].splitDim)
		if v < dTree.nodes[currNodeInd].splitVal {
			currNodeInd = dTree.nodes[currNodeInd].lInd
		} else {
			currNodeInd = dTree.nodes[currNodeInd].rInd
		}
	}

	if dTree.nodeData[currNodeInd] == nil {
		dTree.nodeData[currNodeInd] = make([]DataPoint, 0)
	}

	if len(dTree.nodeData[currNodeInd]) == cap(dTree.nodeData[currNodeInd]){
		dTree.nodeData[currNodeInd] = append(dTree.nodeData[currNodeInd], *point)
	}else{
		dTree.nodeData[len(dTree.nodeData[currNodeInd])] = *point
	}
	dTree.nodes[currNodeInd].currNum += 1

	if dTree.nodes[currNodeInd].currNum >= splitThres{
		err := dTree.splitLeaf(currNodeInd)
		if err != nil{
			return err
		}
	}
}

// Split the specific node in Tree and update the Tree accordingly
func (dTree *DTree) splitLeaf(splitNodeInd uint) error{
	
	if dTree.nodes[splitNodeInd].currNum < len(dTree.nodeData[splitNodeInd]){
		//To do: acquire data from worker, currently WRONG if data are not stored
		dTree.nodeData[splitNodeInd] = append(dTree.nodeData[splitNodeInd], dTree.nodeData[splitNodeInd])
		if dTree.nodes[splitNodeInd].currNum != len(dTree.nodeData[splitNodeInd]){
			err := errors.New(fmt.Sprintf("Incomplete data on node %d", splitNodeInd))
			fmt.Println(err)
			return err
		}
	}

	dimCandidateValue := make(float64, len(dTree.dims))
	dimCandidateMetric := make(float64, len(dTree.dims))
	for j,d := range dTree.dims{
		extractedData := make(float64, len(dTree.nodeData[splitNodeInd]))
		for i,p := range dTree.nodeData[splitNodeInd]{
			extractedData[i] = p.getByDim(d)
		}
		targetPosition := uint(math.Floor(len(extractedData)/2.))
		quickselect.QuickSelect(quickselect.Float64Slice(extractedData),targetPosition)
		dimCandidateValue[j] = extractedData[targetPosition]

		dimCandidateMetric[j] = math.Abs(dimCandidateValue[j] 
			- (dTree.nodeData[splitNodeInd].maxs[j]+dTree.nodeData[splitNodeInd].mins[j])/2) 
			/ (dTree.nodeData[splitNodeInd].maxs[j]-dTree.nodeData[splitNodeInd].mins[j])
	}

	bestSplit := argmax(dimCandidateMetric)
	dTree.nodes[splitNodeInd].splitDim = dTree.dims[bestSplit] 
	dTree.nodes[splitNodeInd].splitThres = dimCandidateValue[bestSplit] 
	
	leftMaxs := make(float64, len(dTree.dims))
	leftMaxs[:] = dTree.nodeData[splitNodeInd].maxs[:]
	leftMax[bestSplit] = dimCandidateValue[bestSplit]
	
	rightMins := make(float64, len(dTree.dims))
	rightMins[:] = dTree.nodeData[splitNodeInd].mins[:]
	rightMins[bestSplit] = dimCandidateValue[bestSplit]

	leftNode := initTreeNode(dTree.nodeData[splitNodeInd].mins, leftMax, dims, dCaps)
	dTree.nodes = append(dTree.nodes, *leftNode)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].lInd := len(dTree.nodes)-1

	rightNode := initTreeNode(rightMins, dTree.nodeData[splitNodeInd].maxs, dims, dCaps)
	dTree.nodes = append(dTree.nodes, *rightNode)
	dTree.nodeData = append(dTree.nodeData, nil)
	dTree.nodes[splitNodeInd].rInd := len(dTree.nodes)-1	

	// This line needs to act before assigning data
	dTree.nodes[splitNodeInd].isLeaf = false
	// move data into left right children
	for _,p := range dTree.nodeData[splitNodeInd]{
		dTree.assignData(&p, splitNodeInd)
	}
}

// Batch update the tree assuming the tree has been loaded in the memory
func (dTree *DTreeNode) updateTree(points []DataPoint) error {
	for i,p := range points{
		dTree.assignData(&p, 0)
	}
}
