package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"strconv"
	"time"
)

const (
	workerListernerPort = 9008
	clientListenerPort  = 7008
	workerNumber        = 8
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
	workerList     map[int]WorkerInfo
	treeMetadata   *DTree
	leafMap        map[int][]DataBatch //key is worker number
	cubeList       map[int]int         //key: cube val: worker
	clientListener net.Listener
	msgChan        chan []byte
	start          time.Time
}

//InitClient ...
func InitClient() (client *Client, err error) {
	log.Println(">>>>>>>>>>>>>>Init client>>>>>>>>>>>>>>")
	// Client only have one listener port, listening result from other workers
	// It could send out TCP message to worker like Tree data, data batch, queries, map of treeleaf

	if err != nil {
		log.Println(err)
	}

	pDims := []uint{1, 0}
	pCaps := []uint{100, 100}

	initMins := []float64{40.75 - 0.3, -73.925 - 0.3}
	initMaxs := []float64{40.75 + 0.3, -73.925 + 0.3}
	splitThresRatio := 0.4

	log.Println("Initializing DTree in client...")

	dTree := InitTree(pDims, pCaps, splitThresRatio, initMins, initMaxs)

	log.Println("Initializing client structure...")
	clientConn, _ := net.Listen("tcp", ":"+strconv.Itoa(clientListenerPort))

	client = &Client{
		workerList:     make(map[int]WorkerInfo, workerNumber),
		treeMetadata:   dTree,
		leafMap:        make(map[int][]DataBatch, workerNumber),
		cubeList:       make(map[int]int),
		msgChan:        make(chan []byte),
		clientListener: clientConn,
	}

	log.Println("Fill worker info...")

	//fill worker info
	idip := map[int]string{1: "172.22.154.227", 2: "172.22.156.227", 3: "172.22.158.227",
		4: "172.22.154.228", 5: "172.22.156.228", 6: "172.22.158.228",
		7: "172.22.154.229", 8: "172.22.156.229", 9: "172.22.158.229",
		10: "172.22.154.230", 11: "172.22.156.230", 12: "172.22.158.230",
		13: "172.22.154.231", 14: "172.22.156.231", 15: "172.22.158.231",
	}
	for i := 1; i <= workerNumber; i++ {
		client.workerList[i+1] = WorkerInfo{
			id:      i + 1, // Client ID = 1, worker ID = 2-9
			address: net.TCPAddr{IP: net.ParseIP(idip[i+1]), Port: workerListernerPort},
		}

	}

	return client, err
}

// Start do the following job:
// Simulate our test path, 1. get data from file, 2. Construct the tree accordingly
// 3. Determine which dp should be send to which worker
// 4. Send the whole tree, and DataBatches to workers accordingly
func (cl *Client) Run(dataPath string) (err error) {
	log.Println(">>>>>>>>>>>>>>Running Client>>>>>>>>>>>>>>")
	rawDataPoints, err := ImportData(dataPath)

	//log.Println("Start init client tree...")
	err = cl.treeMetadata.UpdateTree(rawDataPoints)

	if err != nil {
		panic(err)
	}

	cl.Split()
	//log.Printf("Tree build finished. Total %d dataPoints. Total number of nodes, include non-leaf, %d\n", len(rawDataPoints), len(cl.treeMetadata.Nodes))
	//cl.leafMap[2] = cl.treeMetadata.ToDataBatch()
	// for _, batch := range cl.leafMap[2] {
	// 	log.Println(len(batch.DPoints))
	// }
	//cl.leafMap[3] = cl.treeMetadata.ToDataBatch()
	// for _, batch := range cl.leafMap[3] {
	// 	log.Println(len(batch.DPoints))
	// }

	err = cl.Sync()

	var qs []*Query
	// for _, dp := range rawDataPoints {
	// 	qs = append(qs, dp.GenerateFakeEqualityQuery())
	// }
	// qs = qs[:100]
	for _, dp := range rawDataPoints {
		qs = append(qs, dp.GenerateFakeRangeQuery())
	}
	qs = qs[:2]

	//Start benchmark
	time.Sleep(2 * time.Second)
	cl.start = time.Now()
	cl.Execute(qs)

	//end benchmark
	return err
}

// Execute List of queries and calculate the time duration of receiving all results
func (cl *Client) Execute(qs []*Query) (err error) {
	for _, q := range qs {
		err := cl.executeQuery(q)
		if err != nil {
			log.Println(err)
		}
	}
	return nil
}

func (cl *Client) Sync() (err error) {

	tree := MarshalTree(cl.treeMetadata)
	treeMsg, _ := json.Marshal(Message{Type: "Tree", MsgBytes: tree})

	for _, w := range cl.workerList {
		log.Printf("Syncing...%d\n", w.id)
		conn, err := net.Dial("tcp", w.address.String())
		if err != nil {
			log.Printf("Cannot connect to worker %d \n", w.id)
			continue
		}

		_, err = conn.Write(treeMsg)
		conn.Close()

		if err != nil {
			log.Printf("Cannot send tree to worker %d \n", w.id)
		}
		log.Println("Tree Sent...")

		//log.Println(cl.leafMap[w.id])
		for _, batch := range cl.leafMap[w.id] {
			b, _ := json.Marshal(&batch)
			//log.Println("Sending databatch..%d\n", batch.CubeId)
			dataBatchMsg, _ := json.Marshal(Message{Type: "DataBatch", MsgBytes: b})
			conn, err = net.Dial("tcp", w.address.String())
			_, err = conn.Write(dataBatchMsg)
			conn.Close()
			if err != nil {
				log.Printf("Cannot send databatches to worker %d \n", w.id)
			}
		}

		log.Println("Leaf Map Sent...")
	}
	return nil
}

func (cl *Client) findWorker(q *Query) int {
	if q.QueryType == 0 {
		cubeInds, _ := cl.treeMetadata.EquatlitySearch(q.QueryDims, q.QueryDimVals)
		//log.Println(cubeInds)
		return cl.cubeList[cubeInds[0]]
	} else if q.QueryType == 1 {
		return 2
	} else {
		return 0
	}
}

//TODO:
func (cl *Client) executeQuery(q *Query) (err error) {
	//TODO: TreeSearch to find which worker to route query to
	workerid := cl.findWorker(q)
	//send query to worker
	query := MarshalQuery(q)
	qmsg, _ := json.Marshal(Message{Type: "Query", MsgBytes: query})
	dest := cl.workerList[workerid]
	conn, err := net.Dial("tcp", dest.address.String())
	if err != nil {
		log.Printf("Cannot connect to worker %d \n", dest.id)
		return err
	}
	_, err = conn.Write(qmsg)
	defer conn.Close()
	if err != nil {
		log.Printf("Cannot send query to worker %d \n", dest.id)
	}

	return nil
}

func (cl *Client) TCPListener() {
	for {
		c, err := cl.clientListener.Accept()
		//log.Println("got connection")
		if err != nil {
			log.Println("err")
		}
		go cl.HandleTCPConn(c)

	}
}

func (cl *Client) HandleTCPConn(c net.Conn) {
	var buf bytes.Buffer
	_, err := io.Copy(&buf, c)
	if err != nil {
		fmt.Println("Error copying from connection!")
	}

	msg := new(Message)
	err = json.Unmarshal(buf.Bytes(), &msg)
	if msg.Type == "Error" {
		log.Println("Error when executing query")
	}

	//convert to DataPoints
	var b []DataPoint
	json.Unmarshal(msg.MsgBytes, &b)
	log.Println(b)
	// log.Println("Received results")
	// elapsed := time.Since(cl.start)
	// log.Printf("Total time used in executing queries: %dns \n ", elapsed.Nanoseconds)
	if len(b) == 0 {
		log.Println("No results found")
	}

}

func (dTree *DTree) ObtainInd(indices []int) int {
	currInd := int(0)
	currNode := dTree.Nodes[0]
	for _, ind := range indices {
		if ind == 0 {
			currInd = int(currNode.LInd)
			currNode = dTree.Nodes[currInd]
		} else {
			currInd = int(currNode.RInd)
			currNode = dTree.Nodes[currInd]
		}
	}
	return currInd
}

func (cl *Client) getDataBatch(node *DTreeNode, nodeInd int, workerInd int) {
	if node.IsLeaf {
		cl.cubeList[nodeInd] = workerInd + 2
		cl.leafMap[workerInd+2] = append(cl.leafMap[workerInd+2], DataBatch{nodeInd, node.Capacity, node.Dims, node.Mins, node.Maxs, cl.treeMetadata.NodeData[nodeInd]})
		//log.Printf("leaf index %d, lenth %d, worker id %d, datapoint len %d\n", nodeInd, len(cl.leafMap[workerInd+2]), workerInd+2, len(cl.leafMap[workerInd+2][0].DPoints))

	} else {
		leftInd := int(cl.treeMetadata.Nodes[nodeInd].LInd)
		rightInd := int(cl.treeMetadata.Nodes[nodeInd].RInd)
		cl.getDataBatch(&cl.treeMetadata.Nodes[leftInd], leftInd, workerInd)
		cl.getDataBatch(&cl.treeMetadata.Nodes[rightInd], rightInd, workerInd)
	}
}

func (cl *Client) Split() {
	idx := []int{0, 0, 0}
	for i := 0; i < 8; i++ {
		t := i
		idx[0] = t / 4
		t = t % 4
		idx[1] = t / 2
		t = t % 2
		idx[2] = t
		nodeInd := cl.treeMetadata.ObtainInd(idx)
		cl.leafMap[i+2] = make([]DataBatch, 0)
		cl.getDataBatch(&cl.treeMetadata.Nodes[nodeInd], nodeInd, i)
		//log.Println(cl.leafMap[i+2])
	}
}
