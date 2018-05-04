package main

import (
	"fmt"
	"io/ioutil"
	"log"
	"net"
	"strconv"
)

const (
	clientListenerTCPPort = 9008
	tcpWorkerListenerPort = 8008
	tcpPeerListenerPort   = 7008
	wokerNumber           = 14
)

type WorkerInfo struct {
	id      int
	address net.TCPAddr
}

type ClientInfo struct {
	id      int
	address net.TCPAddr
}

type Client struct {
	id         int
	workerList []WorkerInfo
	// peerList       []ClientInfo
	treeMetadata   *DTree
	clientListener net.Listener
}

//InitClient ...
func InitClient() (client *Client, err error) {
	log.Println("Start client...")
	// Client only have one listener port, listening result from other workers
	// It could send out TCP message to worker like Tree data, data batch to construct workers' DB's Metadata and sending out queries.
	clientConn, err := net.Listen("tcp", ":"+strconv.Itoa(clientListenerTCPPort)) // ...It is actually the listener port for client
	if err != nil {
		log.Println(err)
	}

	pDims := []uint{1, 0}
	pCaps := []uint{200, 200}

	initMins := []float64{40.75 - 0.3, -73.925 - 0.3}
	initMaxs := []float64{40.75 + 0.3, -73.925 + 0.3}
	splitThresRatio := 0.4

	log.Println("Initializing DTree in client...")

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)

	client = &Client{
		id:         GetID(),
		workerList: make([]WorkerInfo, wokerNumber),
		// peerList:       make([]ClientInfo, 3),
		treeMetadata:   dTree,
		clientListener: clientConn,
	}

	//fill worker info
	for i := 0; i < wokerNumber; i++ {
		client.workerList[i] = WorkerInfo{
			id:      1 + i + 1, // Client ID = 1, worker ID = 2 - 15
			address: net.TCPAddr{IP: net.ParseIP(CalculateIP(1 + i + 1)), Port: tcpWorkerListenerPort},
		}
	}
	return client, err

}

//ClientListener ...
func (cl *Client) ClientListener() chan net.Conn {
	ch := make(chan net.Conn)
	go func() {
		for {
			connection, err := cl.clientListener.Accept()
			if err != nil {
				log.Println("can not accept:" + err.Error())
				continue
			}
			ch <- connection
		}
	}()
	return ch
}

//HandleClientRequests ..
func (co *Client) HandleWorkerResult(connection net.Conn) {
	defer connection.Close()
	byteData, _ := ioutil.ReadAll(connection)
	// TODO: (Jade) Unmarshal the result data and print the result on screen

	/*
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
		//TODO:Unmarshal Query
	*/
}

/*
func main()
	con := ClientListener()
	for{
		go HandleClientRequests(<-con)
	}
*/

// Start do the following job:
// Simulate our test path, 1. get data from file, 2. Construct the tree accordingly
// 3. Determine which dp should be send to which worker
// 4. Send the whole tree, and DataBatches to workers accordingly
func (cl *Client) Start(dataPath string) (err error) {
	rawDataPoints, err := ImportData(dataPath)
	if err != nil {
		panic(err)
	}
	fmt.Println("Start init client tree...")
	err = cl.treeMetadata.UpdateTree(rawDataPoints)
	if err != nil {
		panic(err)
	}
	// TODO: (Jade) Maybe generate query also
	fmt.Printf("Tree build finished. Total number of nodes, include non-leaf, %d\n", len(cl.treeMetadata.nodes))
	// TODO: (Jade) List of Databatches by tree
	// TODO: (Jade) Also the map of which index is in which worker
	// TODO: (Jade) Sending out databatches, map, and the whole tree. (sync)
	return err
}

// Execute List of queries and calculate the time duration of receiving all results
func (cl *Client) Execute(qs []*Query) (err error) {

	return nil
}

func (cl *Client) sync() (err error) {
	//TODO:: marshal****
	b := marshalTree(cl.treeMetadata)

	for _, peer := range cl.workerList {
		conn, err := net.Dial("tcp", peer.address.String())
		if err != nil {
			return err
		}
		conn.Write(b)
		conn.Close()
	}
	return nil
}

func (co *Client) send() {

}

func (co *Client) feedTree(dPoints []DataPoint) {
	err := co.treeMetadata.UpdateTree(dPoints)
	if err != nil {
		panic(err)
	}
}

func (co *Client) executeQuery(q Query) {

}
