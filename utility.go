package main

import (
	"encoding/json"
	"io/ioutil"
	"log"
	"net"
	"strconv"
	"unsafe"
)

const (
	serverBase = "172.22.154.132"
)

type DataPoint struct {
	// Idx is the fake 2d index of the data point for the particular
	// treenode (cube), need to be updated everytime the inherited
	// node is splited
	Idx  int
	FArr []float64
	IArr []int
	SArr []string
}

type Message struct {
	Type     string //Tree/DataBatch/DataPoints/Query
	MsgBytes []byte
}

type DataBatch struct {
	CubeId   int
	Capacity uint
	Dims     []uint
	Mins     []float64
	Maxs     []float64
	dPoints  []DataPoint
}

func (point *DataPoint) getFloatValByDim(d uint) float64 {
	return point.FArr[d]
}

func (point *DataPoint) getIntValByDim(d uint) int {
	d = d - uint(len(point.FArr))
	return point.IArr[d]
}

func (point *DataPoint) getStringValByDim(d uint) string {
	d = d - uint(len(point.FArr))
	d = d - uint(len(point.IArr))
	return point.SArr[d]
}

// ExitOnErr print the err message and then exit the program
func ExitOnErr(err error) {
	if err != nil {
		log.Fatal(err)
	}
}

// InitTCPListener initialize the net.Listener of TCP according to the port id and return the Listener
func InitTCPListener(port int) (listener net.Listener) {
	l, err := net.Listen("tcp", ":"+strconv.Itoa(port))
	ExitOnErr(err)
	listener = l
	return listener
}

// TCPListenerIdle is a sample idling function with for loop that listens to the tcp listener
// if any info comes in, it will go to process
func TCPListenerIdle(listener net.Listener) {
	for {
		c, err := listener.Accept()
		ExitOnErr(err)
		go HandleTCPConnection(c)
	}
}

// HandleTCPConnection read the data from connection and returns as []byte
func HandleTCPConnection(conn net.Conn) ([]byte, error) {
	defer conn.Close()
	return ioutil.ReadAll(conn)
}

// SendTCPdata send byte array data to srcAddr and return if there's any error
func SendTCPdata(srcAddr string, b []byte) (err error) {
	conn, err := net.Dial("tcp", srcAddr)
	if err != nil {
		return err
	}
	conn.Write(b)
	conn.Close()
	return nil
}

type blockDataSequence struct {
}

type dataSequence []KeyValueSequence

type KeyValueSequence struct {
	dataPair string
	offset   uint32
}

type DataInterface interface {
	MarshalSequenceData() ([]byte, error)
}

func (d *dataSequence) MarshalSequenceData() ([]byte, error) {
	var buf []byte
	/*
		make(buf, 0, 0)
		for kvP := range d {

		}
	*/
	return buf, nil
}

/*** Utility Function******/

//GetID ...
func GetID() int {
	return CalculateID(GetIpv4Address())
}

//GetIpv4Address ..
func GetIpv4Address() string {
	addrs, _ := net.InterfaceAddrs()
	var ipaddr string
	for _, a := range addrs {
		if ipnet, ok := a.(*net.IPNet); ok && !ipnet.IP.IsLoopback() {
			if ipnet.IP.To4() != nil {
				ipaddr = ipnet.IP.String()
			}
		}
	}
	return ipaddr
}

//CalculateID ...Map current ip address base off vm1 ip address
func CalculateID(serverAddr string) int {
	addr, err := strconv.Atoi(serverAddr[12:14])
	if err != nil {
		log.Fatal(">Wrong ip Address")
	}
	base, _ := strconv.Atoi(serverBase[12:14])
	return addr - base + 1
}

//CalculateIP ...Map current id base off vm1 ip address
func CalculateIP(id int) string {
	base, _ := strconv.Atoi(serverBase[12:14])
	return serverBase[0:12] + strconv.Itoa(base+id-1)
}

//function for marshal tree

// function for unmarshal tree

/*
	MarshalDBtoByte marshal databatch into byte array
*/
func MarshalDBtoByte(batch DataBatch) []byte {

	data := make([]byte, 0)

	intSize := unsafe.Sizeof(batch.CubeId)
	uintSize := unsafe.Sizeof(batch.Capacity) // capacity(uint) and Dims(utin[])
	float64Size := unsafe.Sizeof(float64(0))
	dimLength := len(batch.Dims)
	minsLength := len(batch.Mins)
	maxsLength := len(batch.Maxs)
	dPointLength := len(batch.dPoints)
	// first intSize byte for cubeid
	byteData, _ := json.Marshal(batch.CubeId)
	data = append(data, byteData...)
	// next uintSize byte for capacity
	byteData, _ = json.Marshal(batch.Capacity)
	data = append(data, byteData...)
	// next intSize byte for lengof of Dims
	byteData, _ = json.Marshal(dimLength)
	data = append(data, byteData...)
	// next intSize byte for length of minslength
	byteData, _ = json.Marshal(minsLength)
	data = append(data, byteData...)
	// next intSize byte for length of maxslength
	byteData, _ = json.Marshal(maxsLength)
	data = append(data, byteData...)
	// next intSize byte for length of dPointLength
	byteData, _ = json.Marshal(dPointLength)
	data = append(data, byteData...)

	// trans dims into byte
	byteData = marshalUintArray(batch.Dims)
	data = append(data, byteData...)

	// trans mins into byte
	byteData = marshalFloat64Array(batch.Mins)
	data = append(data, byteData...)

	// trans maxs into byte
	byteData = marshalFloat64Array(batch.Maxs)
	data = append(data, byteData...)

	// trans dPoints into byte
	for _, dp := range batch.dPoints {
		header, body := convertDPoint(dp)
		data = append(data, header...)
		data = append(data, body...)
	}
	return data
}

func UnmarshalBytetoDB(data []byte) *DataBatch {

	batch := new(DataBatch)

	intSize := uint64(unsafe.Sizeof(int(0)))
	uintSize := uint64(unsafe.Sizeof(uint(0)))
	float64Size := uint64(unsafe.Sizeof(float64(0)))
	var curData []byte
	offset := uint64(0)
	// first intSize byte for cubeid
	curData = data[offset : offset+intSize]
	var cubeID int
	json.Unmarshal(curData, cubeID)
	batch.CubeId = cubeID
	offset += intSize
	// next uintSize byte for capacity
	var capacity uint
	curData = data[offset : offset+uintSize]
	json.Unmarshal(curData, capacity)
	batch.Capacity = capacity
	offset += uintSize
	// next intSize byte for lengof of Dims
	var dimsLength int
	curData = data[offset : offset+intSize]
	json.Unmarshal(curData, dimsLength)
	batch.Dims = make([]uint, dimsLength)
	offset += intSize
	// next intSize byte for length of minslength
	var minsLength int
	curData = data[offset : offset+intSize]
	json.Unmarshal(curData, minsLength)
	batch.Mins = make([]float64, minsLength)
	offset += intSize
	// next intSize byte for length of maxslength
	var maxsLength int
	curData = data[offset : offset+intSize]
	json.Unmarshal(curData, maxsLength)
	batch.Maxs = make([]float64, maxsLength)
	offset += intSize
	// next intSize byte for length of dPointLength
	var dPointLength int
	curData = data[offset : offset+intSize]
	json.Unmarshal(curData, dPointLength)
	batch.dPoints = make([]DataPoint, dPointLength)
	offset += intSize
	// trans byte into dims (uint[])
	for i := 0; i < dimsLength; i++ {
		curData = data[offset : offset+uintSize]
		json.Unmarshal(curData, batch.Dims[i])
		offset += uintSize
	}
	// trans byte into mins (float64[])
	for i := 0; i < minsLength; i++ {
		curData = data[offset : offset+float64Size]
		json.Unmarshal(curData, batch.Mins[i])
		offset += float64Size
	}
	// trans byte into maxs (float64[])
	for i := 0; i < maxsLength; i++ {
		curData = data[offset : offset+float64Size]
		json.Unmarshal(curData, batch.Maxs[i])
		offset += float64Size
	}
	// trans byte into dPoints (DataPoint[])
	for i := 0; i < dPointLength; i++ {
		// header
		header := data[offset : offset+20]
		offset += 20
		_, totalLength, floatNum, intNum, stringNum := getDataHeader(header)
		dArr := data[offset : offset+uint64(totalLength)]
		batch.dPoints[i] = convertByteTodPoint(dArr, floatNum, intNum, stringNum)
		offset += uint64(totalLength)
	}

	return batch

}

func marshalUintArray(inArray []uint) []byte {
	ret := make([]byte, 0)
	for _, uintNum := range inArray {
		byteData, _ := json.Marshal(uintNum)
		ret = append(ret, byteData...)
	}
	return ret
}

func marshalFloat64Array(fArray []float64) []byte {
	ret := make([]byte, 0)
	for _, fNum := range fArray {
		byteData, _ := Float64bytes(fNum)
		ret = append(ret, byteData...)
	}
	return ret
}

func MarshalTree() {

}

func UnMarshalTree() {

}
