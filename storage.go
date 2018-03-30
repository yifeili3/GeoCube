package geocube

import (
    "fmt"
    "io/ioutil"
    "os"
    "strconv"
    "strings"
    "utility"
)

type Worker struct{
   CubeMetahMap      map[int]string //  key: treeNodeidx Value: metafilepath
   DataArr  []byte
   Cube     MetaCube
}

type CubeCell struct {
    Count       int
    CellHead    int // Offset (listhead) of cubelist
    CellTail    int // listTail of cubelist
}

// MapInd() int


type MetaCube struct{
   DimSize      []int
   CubeArr      []CubeCell
}

func (w *Worker) Read() error {

    
}


func (w *Worker) Write(batch DataBatch) error{
 cubeSize := calculateCubeSize(batch.Dims)
 //TODO: Change to sync.pool?
 w.Cube.CubeArr = make([]CubeCell, cubeSize)
//TODO: missing index function
 for d := range batch.dPoint {  
    byteArr, header := convertDPoint(d)
    w.Cube.CubeArr.updateCubeCell()
 } 

}

func (w *Worker) updateCubeCell(){

} 


// Header format: | totalLength | FloatNum | IntNum | StringNum |  
func convertDPoint(d DataPoint) (res []byte,  header []byte) {
    lenFloat := len(d.FArr)
    if lenFloat > 0 {
        for fl:= range d.FArr{
            append(res, json.Marshal(fl))
        }
    }
    lenInt := len(d.IArr)
    if lenInt > 0{
        for iNum := range d.IArr{
            append(res, json.Marshal(iNum))
        }
    }
    lenString := len(d.SArr)
    if lenString > 0 {
        for str := range d.SArr{
            append(res, json.Marshal(str+"\t"))
        }
    }
    totalLength := len(res)
    // TODO: (Yeech) spare the space later, maybe change uint32 to uint16
    bs := make([]byte, 4)
    binary.LittleEndian.PutUint32(bs, totalLength)
    append(header, bs)
    binary.LittleEndian.PutUint32(bs, lenFloat)
    append(header, bs)
    binary.LittleEndian.PutUint32(bs, lenInt)
    append(header, bs)
    binary.LittleEndian.PutUint32(bs, lenString)
    append(header, bs)
    return res, header


}

func calculateCubeSize(dimSize []int) int{
    cubeSize := 1
    for i := range dimSize {
        cubeSize *= i
    }
    return cubeSize
}















