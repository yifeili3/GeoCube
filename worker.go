// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"encoding/json"
	"io/ioutil"
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

	idip := map[int]string{1: "172.22.154.227", 2: "172.22.156.227", 3: "172.22.158.227",
		4: "172.22.154.228", 5: "172.22.156.228", 6: "172.22.158.228",
		7: "172.22.154.229", 8: "172.22.156.229", 9: "172.22.158.229",
		10: "172.22.154.230", 11: "172.22.156.230", 12: "172.22.158.230",
		13: "172.22.154.231", 14: "172.22.156.231", 15: "172.22.158.231",
	}

	w = &Worker{
		id:             util.GetID(idip),
		peerList:       make([]peerInfo, 13),
		clientListener: clientConn,
		peerListener:   peerConn,
		db:             tempdb,
	}

	for i := 1; i < 14; i++ {
		if i != w.id {
			w.peerList[i] = peerInfo{
				id:      i,
				address: net.TCPAddr{IP: net.ParseIP(idip[i]), Port: tcpPeerListenerPort},
			}
		}
	}
	return w, err

}

//HandleClientRequests ..
func (w *Worker) HandleClientRequests(client net.Conn) {
	b, err := ioutil.ReadAll(client)
	if err != nil {
		log.Println("unable to read from coordinator")
	}

	msg := new(Message)
	if b != nil {
		err = json.Unmarshal(b, &msg)
		if err != nil {
			log.Println("Error Parse message:", err)
		}
	}
	switch msg.Type {
	case "Tree":
		w.dTree = UnMarshalTree(msg.MsgBytes)
	case "DataBatch":
		w.db.Feed(UnmarshalBytetoDB(msg.MsgBytes))
	case "Query":
		//TODO:: parse query and execute it
	default:
		log.Println("Unrecognized message")
	}

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
	b, err := ioutil.ReadAll(peer)
	//TODO:Unmarshal Message********

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
