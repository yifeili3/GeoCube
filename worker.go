// Copyright (c) 2018 The geocube Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.
package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
)

const (
	tcpWorkerListenerPort = 9008
	tcpClientListenerPort = 7008
)

type peerInfo struct {
	id      int
	address net.TCPAddr
}

//Worker ...
type Worker struct {
	id             int
	dTree          *DTree
	peerList       map[int]peerInfo
	clientListener net.Listener
	clientInfo     peerInfo
	db             *DB
}

//InitWorker ...
func InitWorker() (w *Worker, err error) {
	log.Println("Start worker...")

	clientConn, err := net.Listen("tcp", ":"+strconv.Itoa(tcpWorkerListenerPort))
	if err != nil {
		log.Println(err)
	}
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
		id:             GetID(idip),
		peerList:       make(map[int]peerInfo, 13),
		clientListener: clientConn,
		db:             tempdb,
		clientInfo:     peerInfo{id: 1, address: net.TCPAddr{IP: net.ParseIP(idip[1]), Port: tcpClientListenerPort}},
	}

	for i := 0; i < 14; i++ {
		if i != w.id-1 {
			w.peerList[i] = peerInfo{
				id:      i + 1,
				address: net.TCPAddr{IP: net.ParseIP(idip[i+1]), Port: tcpWorkerListenerPort},
			}
		}
	}
	log.Println("Done initializing...")
	return w, err

}

//HandleClientRequests ..
func (w *Worker) HandleClientRequests(client net.Conn) {
	//log.Println("Start handling request...")
	var buf bytes.Buffer
	_, err := io.Copy(&buf, client)
	if err != nil {
		fmt.Println("Error copying from connection!")
	}

	msg := new(Message)
	err = json.Unmarshal(buf.Bytes(), &msg)
	if err != nil {
		log.Println("Error Parse message:", err)
	}

	log.Printf("Incoming message %s\n", msg.Type)
	switch msg.Type {
	case "Tree":
		w.dTree = UnMarshalTree(msg.MsgBytes)
		log.Println(w.dTree)
		log.Println("Finish updating tree")
	case "DataBatch":
		w.db.Feed(UnmarshalBytetoDB(msg.MsgBytes))
	case "Query":
		//TODO:: parse query and execute it
		q := UnMarshalQuery(msg.MsgBytes)
		dataPoints, err := w.executeQuery(q)
		if err != nil {
			log.Println("No results found")
			b, _ := json.Marshal(Message{Type: "Error"})

			w.send(w.clientInfo.address.String(), b)
		}
		//Send query back to client
		b := MarshalDataPoints(dataPoints)
		res, _ := json.Marshal(Message{Type: "DataPoints", MsgBytes: b})
		log.Println("Sending results back to client..")
		w.send(w.clientInfo.address.String(), res)
	case "PeerRequest":
		// cubeIdx := msg.CubeIndex
		// metaIdx := msg.MetaIndex
		// //Read cube from db
		// dPoints := w.db.ReadSingle(cubeIdx, metaIdx)
		// b := MarshalDataPoints(dPoints)
		// w.send(peer, b)
	case "DataPoints":
		// use a channel here to pass dataPoints to RangeQuery
		dp := new([]DataPoint)
		*dp = UnmarshalDataPoints(msg.MsgBytes)
		// dpChan <-dp
	default:
		log.Println("Unrecognized message")
	}
}

func (w *Worker) send(dest string, msg []byte) {

	conn, err := net.Dial("tcp", dest)
	defer conn.Close()
	if err != nil {
		log.Printf("Cannot connect")
		return
	}
	_, err = conn.Write(msg)
	if err != nil {
		log.Printf("Cannot send query to worker")
	}
}

//ClientListener ...
func (w *Worker) ClientListener() {
	accept := 0
	for {
		//log.Println("Accepting Requests >>>>")
		client, err := w.clientListener.Accept()
		if err != nil {
			log.Println("can not accept:", err)
		}
		accept++
		//log.Printf("Accepted: %d\n", accept)
		go w.HandleClientRequests(client)
	}
}

func (w *Worker) executeQuery(q *Query) (dp []DataPoint, err error) {
	switch q.QueryType {
	case 0:
		dp, _, err = w.EqualityQuery(q)
	case 1:
	case 2:
	}
	return
}

func (worker *Worker) EqualityQuery(query *Query) ([]DataPoint, int, error) {
	cubeInds, err := worker.dTree.EquatlitySearch(query.QueryDims, query.QueryDimVals)
	if err != nil {
		return nil, 0, err
	}
	fmt.Println(cubeInds)

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

		dPoints := worker.db.ReadSingle(cubeInd, metaInds[i])
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

func (worker *Worker) RangeQuery(query *Query) ([]DataPoint, int, error) {
	cubeInds, err := worker.dTree.RangeSearch(query.QueryDims, query.QueryDimVals, query.QueryDimOpts)
	if err != nil {
		return nil, 0, err
	}
	//fmt.Println(cubeInds)

	var dataPoints []DataPoint
	totalDrawnNum := int(0)
	for _, cubeInd := range cubeInds {

		dPoints := worker.db.ReadAll(cubeInd)
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
