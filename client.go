package main

import (
	"fmt"
	"log"
	"net"
	"sync"
)

const (
	workerListernerPort = 9008
	wokerNumber         = 14
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
	id             int
	workerList     []WorkerInfo
	treeMetadata   *DTree
	clientListener net.Listener
}

//InitClient ...
func InitClient() (client *Client, err error) {
	log.Println("Start client...")
	// Client only have one listener port, listening result from other workers
	// It could send out TCP message to worker like Tree data, data batch, queries, map of treeleaf

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
		id:           GetID(),
		workerList:   make([]WorkerInfo, wokerNumber),
		treeMetadata: dTree,
	}

	//fill worker info
	idip := map[int]string{1: "172.22.154.227", 2: "172.22.156.227", 3: "172.22.158.227",
		4: "172.22.154.228", 5: "172.22.156.228", 6: "172.22.158.228",
		7: "172.22.154.229", 8: "172.22.156.229", 9: "172.22.158.229",
		10: "172.22.154.230", 11: "172.22.156.230", 12: "172.22.158.230",
		13: "172.22.154.231", 14: "172.22.156.231", 15: "172.22.158.231",
	}
	for i := 0; i < wokerNumber; i++ {
		client.workerList[i] = WorkerInfo{
			id:      1 + i + 1, // Client ID = 1, worker ID = 2 - 15
			address: net.TCPAddr{IP: net.ParseIP(idip[i]), Port: workerListernerPort},
		}
	}
	return client, err

}

// Start do the following job:
// Simulate our test path, 1. get data from file, 2. Construct the tree accordingly
// 3. Determine which dp should be send to which worker
// 4. Send the whole tree, and DataBatches to workers accordingly
func (cl *Client) Run(dataPath string) (err error) {
	rawDataPoints, err := ImportData(dataPath)
	if err != nil {
		panic(err)
	}
	fmt.Println("Start init client tree...")
	err = cl.treeMetadata.UpdateTree(rawDataPoints)
	if err != nil {
		panic(err)
	}

	fmt.Printf("Tree build finished. Total number of nodes, include non-leaf, %d\n", len(cl.treeMetadata.nodes))
	// TODO: (Jade) List of Databatches by tree. Also the map of which index is in which worker
	//ToDataBatch()

	// TODO: (Jade) Sending out list of databatches, and the whole tree. (sync)
	err = cl.Sync()
	if err != nil {
		log.Println("Can not sync..")
	}
	//Generate and Execute query (To be replaced by)
	var qs []*Query
	for _, dp := range rawDataPoints {
		qs = append(qs, generateFakeQuery(&dp))
	}
	cl.Execute(qs)

	return err
}

// Execute List of queries and calculate the time duration of receiving all results
func (cl *Client) Execute(qs []*Query) (err error) {
	var wg sync.WaitGroup
	wg.Add(len(qs))
	for i := 0; i < len(qs); i++ {
		go func() {
			err := cl.executeQuery(qs[i])
			if err != nil {
				fmt.Printf("fail: %v\n", err)
			}
			wg.Done()
		}()
	}
	wg.Wait()
	return nil
}

func (cl *Client) Sync() (err error) {

	//TODO: Send Tree
	//tree := marshalTree(cl.treeMetadata)
	//leafmap databatches :=
	for _, peer := range cl.workerList {
		conn, err := net.Dial("tcp", peer.address.String())
		if err != nil {
			return err
		}
		//conn.Write(tree)
		//conn.Write(leafmap)
		conn.Close()
	}
	return nil
}

func (cl *Client) executeQuery(q *Query) (err error) {
	//TreeSearch to find which worker to route query to

	//send query to worker

	// wait for results

	return nil
}

func generateFakeQuery(dPoint *DataPoint) *Query {
	q1 := InitQuery(1, []uint{1, 0}, []float64{dPoint.getFloatValByDim(uint(1)), dPoint.getFloatValByDim(uint(0))}, []int{0, 0}, 5, "lala")
	return q1
}
