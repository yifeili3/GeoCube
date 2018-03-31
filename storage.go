package geocube

import (
	"log"
    "fmt"
    "io/ioutil"
    "os"
    "strconv"
    "strings"
    "math/rand"
)

const (
    dbRootPath = "./db/"
    dataArraySize = 10000000    // Jade: should this dataArraySize to be initialized as this much?
    LRUSize = 1
)

type DB struct{
   CubeMetaMap      map[int]string //  key: treeNodeidx Value: metafilepath
   Cube     map[int]MetaCube    // fixed size
}

type CubeCell struct {
    Count       int
    CellHead    int // Offset (listhead) of cubelist
    CellTail    int // listTail of cubelist
}

// MapInd() int


type MetaCube struct{
   Cubesize     int
   CubeIndex    int
   CellArr      []CubeCell
   GlobalOffset int //global offset in DataArr
   DataArr      []byte
}

// Init DB initialize the metadata info from dbRootPath, construct a map of index -> metadatafilePath, where
// index is the name of the file. e.g. map[1][dbRootPath/index/index.meta]
func InitDB() (*DB, error){
    dirs, err := ioutil.ReadDir(dbRootPath)
    if err!=nil {
        log.Fatal(err)
    }

    db := new(DB)

    for _, dir := range dirs {
        if !dir.IsDir {
            continue
        }
        files, err := ioutil.ReadDir(dir)
        for _, file := range files{
            if strings.Contains(file.Name(), "meta"){
                index = strconv.Atoi(file.Name().split(".")[0])
                path = dbRootPath +"/" + strconv.Itoa(index)+ "/"+ file.Name()
                db.CubeMetaMap[index] = path
            }
        }
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
    // TODO: Change to sync.pool?
    // free last MetaCube used
    // TODO: LRU => current size 1, change randomize replace to be LRU style
    if len(db.CubeMetaMap) < LRUSize{
        db.CubeMetaMap[cubeId] = MetaCube{Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, DataArr: make([]byte, dataArraySize)}
    } else{
        // randomly choose an index from current CubeMataMap and then replace it.
        toReplaceIdx = rand.Intn(len(db.CubeMetaMap))
        // before delete the entry, write back meta info and data to disk

        delete(db.CubeMetaMap, toReplaceIdx)
        db.CubeMetaMap[cubeId] = MetaCube{Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, DataArr: make([]byte, dataArraySize)}
    }
    return nil
}


func (db *DB) CubeExists(cubeId int) bool {
    return db.CubeMetaMap[cubeId] != nil
}

func (db *DB) Feed(batch DataBatch) error{
    cubeSize := calculateCubeSize(batch.Dims)
    
    if !db.CubeExists(batch.CubeID) {  // even cube file does not existed (This is a new cube file), then we
        // could just feed a new cube
        if err := db.CreateMetaCube(batch.CubeID, cubeSize); err != nil {
            fmt.Println("Fail to create cube")
            return err
        }
        db.feedBatchToCube(batch.dPoints, batch.CubeId)
    } else { // load Cube fisrt from disk
        // check if this cube is in cubeMap of db
        if _, exists := db.Cube[batch.CubeId]; exists {
            // this cube data is currently in momery, do append data to cube
            db.feedBatchToCube(batch.dPoints, batch.CubeId)
        }else{
            // load Cube File from disk

        }

    }
    


}

//TODO: Can be optimized
func (db *DB) feedBatchToCube(dPoints []DataPoint, cubeIdx int){
    cube := &db.Cube[cubeIdx]
    for _, p := range dPoints {
        // TODO: missing index function for each datpoint's index 
        // => commented by Jade: it doesn't matter since this is mapped to a 1-dim array
        cube.feedCubeCell(p)
    } 
}

func (c *MetaCube) writeToDisk() error{

}

// feedCubeCell feed the Datapoint data to db's current cubeCell and then 
func (cube *MetaCube) feedCubeCell(p DataPoint){
    //update metadata
    // c is cube cell
    c := &cube.CellArr[p.Idx]
    c.Count+=1
    globalOffsetCopy := cube.GlobalOffset
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
    
    

    cube.writeEntry(offset, globalOffsetCopy, 4)
    cube.GlobalOffset += 4
    cube.writeEntry(header, globalOffsetCopy, lenHeader)
    cube.GlobalOffset += lenHeader
    cube.writeEntry(byteArr, globalOffsetCopy, lenByteArr)
    cube.GlobalOffset += lenByteArr

    // update previous pointer to point this node
    binary.LittleEndian.PutUint32(offset, globalOffsetCopy)    
    cube.WriteEntry(offset, globalOffsetCopy, 4)

} 

// TODO: change this format
func (c *MetaCube) writeEntry(data []byte, offset int, length int){
    for i := 0; i < length; i++{
        c.DataArr[offset+i] = data[i]
    }
}

// TODO: change this format
func (c *MetaCube) readEntry(offset int, length int) data []byte {
    for i := 0; i < length; i++{
        data[i] = c.DataArr[offset+i]
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















