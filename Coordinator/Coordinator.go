package Coordinator

import (
	"log"
	"net"
	"strconv"
)

const (
	tcpClientListenerPort = 9008
	tcpWorkerListenerPort = 8008
	tcpPeerListenerPort   = 7008
)

type WorkerInfo struct {
	id      int
	address net.TCPAddr
}

type CoordInfo struct {
	id      int
	address net.TCPAddr
}

type Coord struct {
	id             int
	workerList     []WorkerInfo
	peerList       []CoordInfo
	treeMetadata   *DTree
	clientListener net.Listener
	workerListener net.Listener
}

//InitCoord ...
func InitCoord() (coord *Coord, err error) {
	log.Println("Start coordinator...")

	clientConn, err := net.Listen("tcp", ":"+strconv.Itoa(tcpClientListenerPort))
	if err != nil {
		log.Println(err)
	}
	workerConn, err := net.Listen("tcp", ":"+strconv.Itoa(tcpWorkerListenerPort))
	if err != nil {
		log.Println(err)
	}

	pDims := []uint{1, 0}
	pCaps := []uint{200, 200}

	initMins := []float64{40.75 - 0.3, -73.925 - 0.3}
	initMaxs := []float64{40.75 + 0.3, -73.925 + 0.3}
	splitThresRatio := 0.4

	log.Println("Initializing DTree...")

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)

	coord = &Coord{
		id:             util.GetID(),
		workerList:     make([]WorkerInfo, 10),
		peerList:       make([]CoordInfo, 3),
		treeMeta:       dTree,
		clientListener: clientConn,
		workerListener: workerConn,
	}
	//fill peer info
	for i := 0; i < 4; i++ {
		if i != coord.id-1 {
			peerList[i] = CoordInfo{
				id:      i + 1,
				address: net.TCPAddr{IP: net.ParseIP(util.CalculateIP(i + 1)), Port: tcpPeerListenerPort},
			}
		}
	}
	//fill worker info
	for i := 0; i < 10; i++ {
		workerList[i] = WorkerInfo{
			id:      3 + i + 1,
			address: net.TCPAddr{IP: net.ParseIP(util.CalculateIP(3 + i + 1)), Port: tcpWorkerListenerPort},
		}
	}
	return coord, err

}

//HandleClientRequests ..
func (co *Coord) HandleClientRequests(client net.Conn) {

	var buf = make([]byte, 20000)
	count := 0
	for {
		n, err := client.Read(buf[count:])
		if n == 0 {
			break
		}
		count += n
		if err != nil {
			break
		}
	}
	//TODO:Unmarshal Query
}

//ClientListener ...
func (co *Coord) ClientListener() chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			client, err := co.clientListener.Accept()
			if err != nil {
				log.Println("can not accept:" + err.string())
				continue
			}
			ch <- client
		}
	}()
	return ch
}

//WorkerListener ...
func (co *Coord) WorkerListener() chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			worker, err := co.workerListener.Accept()
			if err != nil {
				log.Println("can not accept:" + err.string())
				continue
			}
			ch <- worker
		}
	}()
	return ch
}

//HandleWorkerResults ..
func (co *Coord) HandleWorkerResults(worker net.Conn) {
	var buf = make([]byte, 20000000)
	count := 0
	for {
		n, err := worker.Read(buf[count:])
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

func (co *Coord) sync() (err error) {
	//TODO:: marshal****
	b := util.marshalTree(co.treeMetadata)

	for _, peer := range co.peerList {
		conn, err := net.Dial("tcp", peer.address)
		if err != nil {
			return err
		}
		conn.Write()
		conn.Close()
	}
	return nil
}

func (co *Coord) send() {

}

func (co *Coord) feedTree(dPoints []DataPoint) {
	err = co.treeMetadata.UpdateTree(dPoints)
	if err != nil {
		panic(err)
	}
}

func (co *Coord) executeQuery(q Query) {

}
