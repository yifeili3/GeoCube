package geocube

import (
	"io/ioutil"
	"log"
	"net"
	"strconv"
)

type DataPoint struct{
	FArr	[]float64	
	IArr	[]int
	SArr	[]string
}

type DataBatch struct{
	Dims []int
	dPoint	[]DataPoint
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
func HandleTCPConnection(conn net.Conn) ([]byte, err) {
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
	make(buf, 0, 0)
	for kvP := range d {

	}
}
