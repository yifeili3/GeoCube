package geocube

import (
	"log"
    "fmt"
    "io/ioutil"
    "os"
    "strconv"
    "strings"
    "utility"
)

const (
    dbRootPath = "./db/"
    dataArraySize = 10000000    // Jade: should this dataArraySize to be initialized as this much?
)

type DB struct{
   CubeMetaMap      map[int]string //  key: treeNodeidx Value: metafilepath
   Cube     MetaCube
}

type CubeCell struct {
    Count       int
    CellHead    int // Offset (listhead) of cubelist
    CellTail    int // listTail of cubelist
}

// MapInd() int


type MetaCube struct{
   Cubesize     int
   CellArr      []CubeCell
   GlobalOffset int //global offset in DataArr
   DataArr      []byte
}

// Init DB initialize the metadata info from dbRootPath, construct a map of index -> metadatafilePath, where
// index is the name of the file. e.g. map[1][dbRootPath/1.meta]
func InitDB() (*DB, error){
    files, err := ioutil.ReadDir(dbRootPath)
    if err!=nil {
        log.Fatal(err)
    }

    db := new(DB)

    for _, file := range files {
        index = strconv.Atoi(file.Name().split(".")[0])
        path = dbRootPath + file.Name()
        db.CubeMetaMap[index] = path
    }
    return db, err

}

func (db *DB) Read() error {

    
}

func (db *DB) Load() error {

}

// Create function create the cube from cubeId (index of tree node) and cubeSize (size of dimension)
// then we need to feed the data from unorganized batch of datapoints to the cube or read data from files
func (db *DB) CreateMetaCube(cubeId int, cubeSize int) error {
    if err: os.MkdirAll(path.Join(dbRootPath, cubeId), 0700); err != nil{
        return err
    }
    //TODO: Change to sync.pool?
    // free last MetaCube
    if db.Cube != nil {
        db.Cube = nil
    }
    db.Cube := MetaCube{CubeSize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, DataArr: make([]byte, dataArraySize)}
    return nil
}


func (db *DB) CubeExists(cubeId int) bool {
    return db.CubeMetaMap[cubeId] != nil
}

func (db *DB) Feed(batch DataBatch) error{
    cubeSize := calculateCubeSize(batch.Dims)
    
    if !db.CubeExists(batch.CubeID) {  // cube not existed (This is a new cube file), then we
        // could just feed a new cube
        if err := db.CreateMetaCube(batch.CubeID, cubeSize); err != nil {
            fmt.Println("Fail to create cube")
            return err
        }
        db.feedBatchToCube(batch.dPoint)
    } else { // load Cube fisrt

    }
    


}

//TODO: Can be optimized
func (db *DB) feedBatchToCube(dPoints []DataPoint){
    
    for _, p := range dPoints {
        // TODO: missing index function for each datpoint's index 
        // => commented by Jade: it doesn't matter since this is mapped to a 1-dim array
        db.feedCubeCell(p)
    } 
}

// feedCubeCell feed the Datapoint data to db's current cubeCell and then 
func (db *DB) feedCubeCell(p DataPoint){
    //update metadata
    c := &db.Cube.CellArr[p.Idx]
    c.Count+=1
    globalOffsetCopy := db.Cube.GlobalOffset
    if c.CellHead == 0 && globalOffsetCopy != 0 {
        //only when no node in this cell
        c.CellHead = globalOffsetCopy
        c.CellTail = c.CellHead
    } else{
        c.CellTail = globalOffsetCopy
    }
    //Write node into byte arrary
    byteArr, header := convertDPoint(p)
    offset := make([]byte, 4)
    binary.LittleEndian.PutUint32(offset, 0)
    lenHeader := len(header)
    lenByteArr := len(byteArr)
    
    db.writeEntry(offset, globalOffsetCopy, 4)
    db.Cube.GlobalOffset += 4
    db.writeEntry(header, globalOffsetCopy, lenHeader)
    db.Cube.GlobalOffset += lenHeader
    db.writeEntry(byteArr, globalOffsetCopy, lenByteArr)
    db.Cube.GlobalOffset += lenByteArr

    // update previous pointer to point this node
    binary.LittleEndian.PutUint32(offset, globalOffsetCopy)    
    db.WriteEntry(offset, globalOffsetCopy, 4)

} 


func (db *DB) writeEntry(data []byte, offset int, length int){
    for i := 0; i < length; i++{
        db.Cube.DataArr[offset+i] = data[i]
    }
}

func (db * DB) readEntry(offset int, length int) data []byte {
    for i := 0; i < length; i++{
        data[i] = db.Cube.DataArr[offset+i]
    }
}

// Header format: | totalLength | FloatNum | IntNum | StringNum |  
func convertDPoint(d DataPoint) (res []byte,  header []byte) {
    lenFloat := len(d.FArr)
    if lenFloat > 0 {
        for _, fl:= range d.FArr{
            append(res, json.Marshal(fl))
        }
    }
    lenInt := len(d.IArr)
    if lenInt > 0{
        for _, iNum := range d.IArr{
            append(res, json.Marshal(iNum))
        }
    }
    lenString := len(d.SArr)
    if lenString > 0 {
        for _, str := range d.SArr{
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















