package geocube

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"log"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
)

const (
	dbRootPath    = "./db/"
	dataArraySize = 10000000 // Jade: should this dataArraySize to be initialized as this much?
	LRUSize       = 1
)

type DB struct {
	CubeMetaMap map[int]string    //  key: treeNodeidx Value: metafilepath
	Cube        map[int]*MetaCube // fixed size
}

type CubeCell struct {
	Count    int
	CellHead int // Offset (listhead) of cubelist
	CellTail int // listTail of cubelist
}

// MapInd() int

type MetaInfo struct {
	Cubesize     int
	CubeIndex    int
	Dims         []uint
	Mins         []float64
	Maxs         []float64
	CellArr      []CubeCell
	GlobalOffset int //global offset in DataArr
}

type MetaCube struct {
	Metainfo MetaInfo
	DataArr  []byte
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Init DB initialize the metadata info from dbRootPath, construct a map of index -> metadatafilePath, where
// index is the name of the file. e.g. map[1][dbRootPath/index/index.meta]
func InitDB() (*DB, error) {
	dirs, err := ioutil.ReadDir(dbRootPath)
	if err != nil {
		log.Fatal(err)
	}

	db := new(DB)

	for _, dir := range dirs {
		if !dir.IsDir() {
			continue
		}
		files, err := ioutil.ReadDir(dir.Name())
		for _, file := range files {
			if strings.Contains(file.Name(), "meta") {
				index, _ := strconv.Atoi(strings.Split(file.Name(), ".")[0])
				path := dbRootPath + "/" + strconv.Itoa(index) + "/" + file.Name()
				db.CubeMetaMap[index] = path
			}
		}
	}
	return db, err

}

func (db *DB) Read() error {
	return nil
}

func (db *DB) Load() error {
	return nil
}

// CreateMetaCube function create the cube from cubeId (index of tree node) and cubeSize (size of dimension)
// then we need to feed the data from unorganized batch of datapoints to the cube or read data from files
func (db *DB) CreateMetaCube(cubeId int, cubeSize int, dims []uint, maxs []float64, mins []float64) error {
	if err := os.MkdirAll(path.Join(dbRootPath, strconv.Itoa(cubeId)), 0700); err != nil {
		return err
	}
	// TODO: Change to sync.pool?
	// free last MetaCube used
	// TODO: LRU => current size 1, change randomize replace to be LRU style
	if len(db.Cube) < LRUSize {
		db.Cube[cubeId] = &MetaCube{Metainfo: MetaInfo{Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, Dims: dims, Maxs: maxs, Mins: mins}, DataArr: make([]byte, dataArraySize)}
	} else {
		// randomly choose an index from current CubeMataMap and then replace it.
		toReplaceIdx := rand.Intn(len(db.Cube))
		// before delete the entry, write back meta info and data to disk
		err := db.Cube[toReplaceIdx].writeToDisk()
		check(err)
		delete(db.Cube, toReplaceIdx)
		db.Cube[cubeId] = &MetaCube{Metainfo: MetaInfo{Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, Dims: dims, Maxs: maxs, Mins: mins}, DataArr: make([]byte, dataArraySize)}
	}
	return nil
}

func (db *DB) CubeExists(cubeId int) bool {
	_, exists := db.CubeMetaMap[cubeId]
	return exists
}

// Feed accepts data batch from upper layer and add data to DB's cube map, according to whether the cube is in map
// if the cube is not in map, then there's a replacement of memory from IO
func (db *DB) Feed(batch DataBatch) error {
	cubeSize := calculateCubeSize(batch.Dims)
	var err error
	if !db.CubeExists(batch.CubeId) { // even cube file does not existed (This is a new cube file), then we
		// could just feed a new cube
		if err = db.CreateMetaCube(batch.CubeId, cubeSize, batch.Dims, batch.Maxs, batch.Mins); err != nil {
			fmt.Println("Fail to create cube")
			return err
		}
		db.feedBatchToCube(batch.dPoints, batch.CubeId)
	} else {
		// check if this cube is in cubeMap of db
		if _, exists := db.Cube[batch.CubeId]; exists {
			// this cube data is currently in momery, do append data to cube
			db.feedBatchToCube(batch.dPoints, batch.CubeId)
		} else {
			// load Cube File from disk
			// if length is less than LRUSize, just append new
			if len(db.Cube) < LRUSize {
				db.Cube[batch.CubeId], _ = loadCubeFromDisk(batch.CubeId)
			} else {
				// find a randomized entry in cube map, load a new MetaCube of this batch index, replace it and then add batch data to it
				indexToReplace := rand.Intn(len(db.Cube))
				err = db.Cube[indexToReplace].writeToDisk()
				check(err)
				delete(db.Cube, indexToReplace)
				db.Cube[batch.CubeId], _ = loadCubeFromDisk(batch.CubeId)
			}
			// append new data, feed to this cube
			db.feedBatchToCube(batch.dPoints, batch.CubeId)

		}

	}
	return err

}

//TODO: Can be optimized
func (db *DB) feedBatchToCube(dPoints []DataPoint, cubeIdx int) {
	cube := db.Cube[cubeIdx]
	for _, p := range dPoints {
		// TODO: missing index function for each datpoint's index
		// => commented by Jade: it doesn't matter since this is mapped to a 1-dim array
		cube.feedCubeCell(p)
	}
}

func loadCubeFromDisk(index int) (c *MetaCube, err error) {
	c = new(MetaCube)
	indexString := strconv.Itoa(index)
	dataPath := dbRootPath + indexString + "/" + indexString + ".data"
	dataByte, err := ioutil.ReadFile(dataPath)
	copy(c.DataArr, dataByte)

	metaPath := dbRootPath + indexString + "/" + indexString + ".meta"
	dataByte, err = ioutil.ReadFile(metaPath)
	err = json.Unmarshal(dataByte, &c.Metainfo)
	check(err)
	return c, err
}

func (c *MetaCube) writeToDisk() error {
	// save data array to be index.data
	// save the left to be index.meta
	stringIdx := strconv.Itoa(c.Metainfo.CubeIndex)
	// create index file dir if not exists, if not, just mkdir
	if _, err := os.Stat(dbRootPath + stringIdx + "/"); os.IsNotExist(err) {
		os.Mkdir(dbRootPath+stringIdx+"/", os.ModePerm)
	}
	dataFileName := dbRootPath + stringIdx + "/" + stringIdx + ".data"
	metaFileName := dbRootPath + stringIdx + "/" + stringIdx + ".meta"

	// dump data file
	err := ioutil.WriteFile(dataFileName, c.DataArr, os.ModePerm)
	check(err)
	// marshal Metainfo to be []byte
	b, err := json.Marshal(c.Metainfo)
	check(err)
	err = ioutil.WriteFile(dataFileName, b, os.ModePerm)
	check(err)
	return err
}

// feedCubeCell feed the Datapoint data to db's current cubeCell and then
func (cube *MetaCube) feedCubeCell(p DataPoint) {
	//update metadata
	// c is cube cell
	c := &cube.Metainfo.CellArr[p.Idx]
	c.Count++
	globalOffsetCopy := cube.Metainfo.GlobalOffset
	if c.CellHead == 0 && globalOffsetCopy != 0 {
		//only when no node in this cell
		c.CellHead = globalOffsetCopy
		c.CellTail = c.CellHead
	} else {
		c.CellTail = globalOffsetCopy
	}
	//Write node into byte arrary
	byteArr, header := convertDPoint(p)
	offset := make([]byte, 4)
	binary.LittleEndian.PutUint32(offset, 0)
	lenHeader := len(header)
	lenByteArr := len(byteArr)

	cube.writeEntry(offset, globalOffsetCopy, 4)
	cube.Metainfo.GlobalOffset += 4
	cube.writeEntry(header, globalOffsetCopy, lenHeader)
	cube.Metainfo.GlobalOffset += lenHeader
	cube.writeEntry(byteArr, globalOffsetCopy, lenByteArr)
	cube.Metainfo.GlobalOffset += lenByteArr

	// update previous pointer to point this node
	binary.LittleEndian.PutUint32(offset, uint32(globalOffsetCopy))
	cube.writeEntry(offset, globalOffsetCopy, 4)

}

// TODO: change this format
func (c *MetaCube) writeEntry(data []byte, offset int, length int) {
	for i := 0; i < length; i++ {
		c.DataArr[offset+i] = data[i]
	}
}

// TODO: change this format
func (c *MetaCube) readEntry(offset int, length int) (data []byte) {
	for i := 0; i < length; i++ {
		data[i] = c.DataArr[offset+i]
	}
	// ----------------> Need more modification
	return data
}

// Header format: | totalLength | FloatNum | IntNum | StringNum |
func convertDPoint(d DataPoint) (res []byte, header []byte) {
	lenFloat := len(d.FArr)
	if lenFloat > 0 {
		for _, fl := range d.FArr {
			byteData, _ := json.Marshal(fl)
			res = append(res, byteData...)
		}
	}
	lenInt := len(d.IArr)
	if lenInt > 0 {
		for _, iNum := range d.IArr {
			byteData, _ := json.Marshal(iNum)
			res = append(res, byteData...)
		}
	}
	lenString := len(d.SArr)
	if lenString > 0 {
		for _, str := range d.SArr {
			byteData, _ := json.Marshal(str + "\t")
			res = append(res, byteData...)
		}
	}
	totalLength := len(res)
	// TODO: (Yeech) spare the space later, maybe change uint32 to uint16
	bs := make([]byte, 4)
	binary.LittleEndian.PutUint32(bs, uint32(totalLength))
	header = append(header, bs...)
	binary.LittleEndian.PutUint32(bs, uint32(lenFloat))
	header = append(header, bs...)
	binary.LittleEndian.PutUint32(bs, uint32(lenInt))
	header = append(header, bs...)
	binary.LittleEndian.PutUint32(bs, uint32(lenString))
	header = append(header, bs...)
	return res, header
}

func calculateCubeSize(dimSize []uint) int {
	cubeSize := 1
	for i := range dimSize {
		cubeSize *= i
	}
	return cubeSize
}
