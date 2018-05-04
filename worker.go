// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"log"
	"net"
	"strconv"
)

const (
	tcpClientListenerPort = 9008
	tcpPeerListenerPort   = 7008
)

type peerInfo struct {
	id      int
	address net.TCPAddr
}

//Worker ...
type Worker struct {
	id             int
	dTree          *DTree
	peerList       []peerInfo
	clientListener net.Listener
	peerListener   net.Listener
	db             *DB
}

//InitWorker ...
func InitWorker() (w *Worker, err error) {
	log.Println("Start worker...")

	clientConn, err := net.Listen("tcp", ":"+strconv.Itoa(tcpClientListenerPort))
	if err != nil {
		log.Println(err)
	}
	peerConn, err := net.Listen("tcp", ":"+strconv.Itoa(tcpPeerListenerPort))
	if err != nil {
		log.Println(err)
	}
	tempdb, err := InitDB()
	if err != nil {
		panic(err)
	}

	w = &Worker{
		id:             util.GetID(),
		peerList:       make([]peerInfo, 13),
		clientListener: clientConn,
		peerListener:   peerConn,
		db:             tempdb,
	}

	for i := 0; i < 14; i++ {
		if i != w.id-1 {
			w.peerList[i] = peerInfo{
				id:      i + 1,
				address: net.TCPAddr{IP: net.ParseIP(util.CalculateIP(i + 1)), Port: tcpPeerListenerPort},
			}
		}
	}
	return w, err

}

//HandleClientRequests ..
func (w *Worker) HandleClientRequests(client net.Conn) {

	var buf = make([]byte, 20000)
	count := 0
	for {
		//TODO: n := ioutil.ReadAll(client)
		n, err := client.Read(buf[count:])
		if n == 0 {
			break
		}
		count += n
		if err != nil {
			break
		}
	}
	//TODO:Unmarshal Query, select different queries

}

//ClientListener ...
func (w *Worker) ClientListener() chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			client, err := w.clientListener.Accept()
			if err != nil {
				log.Println("can not accept:" + err.string())
				continue
			}
			ch <- client
		}
	}()
	return ch
}

//PeerListener ...
func (w *Worker) PeerListener() chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			peer, err := w.peerListener.Accept()
			if err != nil {
				log.Println("can not accept:" + err.string())
				continue
			}
			ch <- peer
		}
	}()
	return ch
}

//HandlePeerResults  ...
func (w *Worker) HandlePeerResults(peer net.Conn) {
	var buf = make([]byte, 20000000)
	count := 0
	for {
		n, err := peer.Read(buf[count:])
		if n == 0 {
			break
		}
		count += n
		if err != nil {
			break
		}
	}
	//TODO:Unmarshal Message********

}

func (w *Worker) feedToDB(databatches []DataBatch) {
	for _, batch := range databatches {
		w.db.Feed(batch)
	}
}

func (worker *Worker) EqualityQuery(db *DB, query *Query) ([]DataPoint, int, error) {
	cubeInds, err := worker.dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, 0, err
	}
	//fmt.Println(cubeInds)

	var metaInds []int
	for _, cubeInd := range cubeInds {
		metaInd, err := worker.dTree.Nodes[cubeInd].MapIndByVal(query.QueryDims, query.QueryDimVals)
		if err != nil {
			return nil, 0, err
		} else {
			metaInds = append(metaInds, metaInd)
		}
	}

	var dataPoints []DataPoint
	var conflictNum = 0
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
		conflictNum = len(dPoints) - len(dataPoints)
	}
	return dataPoints, conflictNum, nil
}

func (worker *Worker) RangeQuery(db *DB, query *Query) ([]DataPoint, int, error) {
	cubeInds, err := worker.dTree.RangeSearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, 0, err
	}
	//fmt.Println(cubeInds)

	var dataPoints []DataPoint
	totalDrawnNum := int(0)
	for i, cubeInd := range cubeInds {

		dPoints := db.ReadAll(cubeInd)
		//fmt.Println(fmt.Sprintf("CubeInd: %d, MetaInd %d", cubeInd, metaInds[i]))
		//fmt.Println(dPoints)
		for _, dp := range dPoints {
			if query.CheckPoint(&dp) {
				//fmt.Println("found")
				dataPoints = append(dataPoints, dp)
			}
		}
		totalDrawnNum += len(dPoints)
	}
	overDrawnNum := totalDrawnNum - len(dataPoints)
	return dataPoints, overDrawnNum, nil
}
