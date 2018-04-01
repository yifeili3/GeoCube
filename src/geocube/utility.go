package geocube

import (
	"io/ioutil"
	"log"
	"net"
	"strconv"
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

type DataBatch struct {
	CubeId  int
	Dims    []uint
	Mins    []float64
	Maxs    []float64
	dPoints []DataPoint
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
