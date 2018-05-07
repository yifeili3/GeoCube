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
	"sync"
)

const (
	tcpWorkerListenerPort = 9008
	tcpClientListenerPort = 7008
	udpPeerListenerPort   = 8008
	udpPeerSenderPort     = 6008
)

type peerInfo struct {
	id      int
	address net.TCPAddr
	udpaddr net.UDPAddr
}

//Worker ...
type Worker struct {
	id             int
	dTree          *DTree
	peerList       map[int]peerInfo
	cubeList       map[int]int
	clientListener net.Listener
	peerConn       *net.UDPConn
	clientInfo     peerInfo
	db             *DB
	peerChan       chan []byte
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

	peermsgconn, err := net.ListenUDP("udp", &net.UDPAddr{IP: net.ParseIP(idip[GetID(idip)]), Port: udpPeerListenerPort})

	w = &Worker{
		id:             GetID(idip),
		peerList:       make(map[int]peerInfo, 13),
		cubeList:       make(map[int]int),
		clientListener: clientConn,
		db:             tempdb,
		clientInfo:     peerInfo{id: 1, address: net.TCPAddr{IP: net.ParseIP(idip[1]), Port: tcpClientListenerPort}},
		peerChan:       make(chan []byte),
		peerConn:       peermsgconn,
	}

	for i := 0; i < 14; i++ {
		if i != w.id {
			w.peerList[i] = peerInfo{
				id:      i,
				address: net.TCPAddr{IP: net.ParseIP(idip[i]), Port: tcpWorkerListenerPort},
				udpaddr: net.UDPAddr{IP: net.ParseIP(idip[i]), Port: udpPeerListenerPort},
			}
		} else {
			w.peerList[i] = peerInfo{
				id:      i + 1,
				address: net.TCPAddr{IP: net.ParseIP(idip[i]), Port: tcpWorkerListenerPort},
				udpaddr: net.UDPAddr{IP: net.ParseIP(idip[i]), Port: udpPeerSenderPort},
			}
		}

	}

	return w, err

}

//HandleClientRequests ..
func (w *Worker) HandleClientRequests(client net.Conn) {

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

	//log.Printf("Incoming message %s\n", msg.Type)
	switch msg.Type {
	case "Tree":
		w.dTree = UnMarshalTree(msg.MsgBytes)
		log.Println("Finish updating tree")
		w.Split()
	case "DataBatch":
		var databatch DataBatch
		err = json.Unmarshal(msg.MsgBytes, &databatch)
		if err != nil {
			log.Println("Unable to unmarshal databatch")
		}

		w.db.Feed(&databatch)
	case "Query":
		q := UnMarshalQuery(msg.MsgBytes)
		dataPoints, err := w.executeQuery(q)
		if err != nil {
			log.Println("No results found")
			b, _ := json.Marshal(Message{Type: "Error"})

			w.send(w.clientInfo.address.String(), b)
		}
		//Send query back to client
		b, _ := json.Marshal(dataPoints)
		res, _ := json.Marshal(Message{Type: "DataPoints", MsgBytes: b})
		//log.Printf("Sending results back to client.. Size:%d\n", len(b))
		w.send(w.clientInfo.address.String(), res)

	default:
		log.Println("Unrecognized message")
	}
}

func (w *Worker) getDataBatch(node *DTreeNode, nodeInd int, workerInd int) {
	if node.IsLeaf {
		w.cubeList[nodeInd] = workerInd + 2
	} else {
		leftInd := int(w.dTree.Nodes[nodeInd].LInd)
		rightInd := int(w.dTree.Nodes[nodeInd].RInd)
		w.getDataBatch(&w.dTree.Nodes[leftInd], leftInd, workerInd)
		w.getDataBatch(&w.dTree.Nodes[rightInd], rightInd, workerInd)
	}
}

func (w *Worker) Split() {
	idx := []int{0, 0, 0}
	for i := 0; i < 8; i++ {
		t := i
		idx[0] = t / 4
		t = t % 4
		idx[1] = t / 2
		t = t % 2
		idx[2] = t
		nodeInd := w.dTree.ObtainInd(idx)
		w.getDataBatch(&w.dTree.Nodes[nodeInd], nodeInd, i)
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
	//ch := make(chan net.Conn)
	accept := 0
	for {
		//log.Println("Accepting Requests >>>>")
		client, err := w.clientListener.Accept()
		if err != nil {
			log.Println("can not accept:", err)
			continue
		}
		accept++
		//log.Printf("Accepted: %d\n", accept)

		go w.HandleClientRequests(client)
	}
}

func (w *Worker) PeerListener() {

	p := make([]byte, 10000000)
	for {
		log.Println("fuck")
		n, remote, _ := w.peerConn.ReadFromUDP(p)
		log.Println(remote)
		if n == 0 {

			continue
		} else {
			var msg Message
			err := json.Unmarshal(p[:n], &msg)

			if err != nil {
				log.Println("Error Parse message:", err)
			}

			log.Printf("Incoming message %s\n", msg.Type)
			switch msg.Type {
			case "PeerRequestAll":
				cubeInds := msg.CubeIndex
				//Read cube from db
				var dp []DataPoint
				for _, cubeInd := range cubeInds {
					dPoints := w.db.ReadAll(cubeInd)
					dp = append(dp, dPoints...)
				}
				log.Println("get data point")
				b, _ := json.Marshal(dp)
				dpmsg, _ := json.Marshal(Message{Type: "DataPoints", MsgBytes: b})
				src := w.peerList[w.id].udpaddr
				src.Port = udpPeerSenderPort
				dest := w.peerList[msg.SenderID].udpaddr
				dest.Port = udpPeerListenerPort
				for {
					conn, err := net.DialUDP("udp", &src, &dest)
					if err == nil {
						conn.Write(dpmsg)
						conn.Close()
						break
					} else {
						log.Println(err)
					}
				}
			case "PeerRequestBatch":
				//cubeInds := msg.CubeIndex
				//metaIdx := msg.MetaIndex

			case "DataPoints":
				// use a channel here to pass dataPoints to RangeQuery
				w.peerChan <- msg.MsgBytes
			}
		}
	}
}

func (w *Worker) executeQuery(q *Query) (dp []DataPoint, err error) {
	switch q.QueryType {
	case 0:
		dp, _, err = w.EqualityQuery(q)
	case 1:
		dp, _, err = w.RangeQuery(q)
	case 2:
	}
	return
}

func (worker *Worker) EqualityQuery(query *Query) ([]DataPoint, int, error) {
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

		dPoints := worker.db.ReadSingle(cubeInd, metaInds[i])
		//fmt.Println(fmt.Sprintf("CubeInd: %d, MetaInd %d", cubeInd, metaInds[i]))
		//fmt.Println(dPoints)
		for _, dp := range dPoints {
			if query.CheckPoint(&dp) {
				//fmt.Println("found")
				for i:=0;i<100;i++{
					dataPoints = append(dataPoints, dp)
				}
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

	var dataPoints []DataPoint
	totalDrawnNum := int(0)

	dPoints := worker.getAll(cubeInds)

	//wait for results

	//Check dpoints
	for _, dp := range dPoints {
		if query.CheckPoint(&dp) {
			//fmt.Println("found")
			dataPoints = append(dataPoints, dp)
		}
	}
	totalDrawnNum += len(dPoints)
	overDrawnNum := totalDrawnNum - len(dataPoints)
	return dataPoints, overDrawnNum, nil
}

func (w *Worker) getAll(cubeInds []int) []DataPoint {
	m := make(map[int][]int)
	for _, cubeInd := range cubeInds {
		m[w.cubeList[cubeInd]] = append(m[w.cubeList[cubeInd]], cubeInd)
	}

	var dPoints []DataPoint
	var wg sync.WaitGroup
	nbGoroutines := len(m)
	wg.Add(nbGoroutines)
	go func() {
		for wid, v := range m {
			if wid == w.id {
				for _, cubeInd := range v {
					temp := w.db.ReadAll(cubeInd)
					dPoints = append(dPoints, temp...)
				}
			} else {
				dest := w.peerList[wid].udpaddr
				src := w.peerList[w.id].udpaddr
				log.Printf("Requesting datapoints from %d\n", wid)
				log.Println("sending udp message")
				for {
					conn, err := net.DialUDP("udp", &src, &dest)
					log.Println("Sending udp packet")
					if err == nil {
						msg, _ := json.Marshal(Message{Type: "PeerRequestAll", CubeIndex: v, SenderID: w.id})
						conn.Write(msg)
						conn.Close()
						break
					}
				}

				log.Println("Wait here")
				dpbuf := <-w.peerChan
				var dp []DataPoint
				json.Unmarshal(dpbuf, &dp)
				dPoints = append(dPoints, dp...)
			}
		}
		wg.Done()
		log.Println("Done one")
	}()
	return dPoints
}
