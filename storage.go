package main

import (
	"encoding/binary"
	"encoding/json"
	"fmt"
	"io/ioutil"
	"math"
	"math/rand"
	"os"
	"path"
	"strconv"
	"strings"
	"time"
)

const (
	dbRootPath         = "./db/"
	dataArraySize      = 0 // Jade: should this dataArraySize to be initialized as this much?
	cacheSize          = 5000
	batchReadThres     = 20
	tcpPort            = 1003
	randomSampleRatio  = 0.1
	readSingleAllRatio = 0.5
)

type DB struct {
	CubeMetaMap map[int]string    //  key: treeNodeidx Value: metafilepath
	Cube        map[int]*MetaCube // fixed size
}

type CubeCell struct {
	Count    int
	CellHead uint32 // Offset (listhead) of cubelist
	CellTail uint32 // listTail of cubelist
}

// MapInd() int

type MetaInfo struct {
	Cubesize     int
	CubeIndex    int
	Dims         []uint
	Mins         []float64
	Maxs         []float64
	CellArr      []CubeCell
	GlobalOffset uint32 //global offset in DataArr
}

type MetaCube struct {
	Metainfo    MetaInfo
	DataArr     []byte
	InsertTime  int64
	AccessCount int64
}

func check(err error) {
	if err != nil {
		panic(err)
	}
}

// Init DB initialize the metadata info from dbRootPath, construct a map of index -> metadatafilePath, where
// index is the name of the file. e.g. map[1][dbRootPath/index/index.meta]
func InitDB() (*DB, error) {

	db := new(DB)
	db.CubeMetaMap = make(map[int]string)
	db.Cube = make(map[int]*MetaCube)

	/*
		dirs, err := ioutil.ReadDir(dbRootPath)
		if err != nil {
			log.Fatal(err)
		}
		for _, dir := range dirs {
			if !dir.IsDir() {
				continue
			}
			//fmt.Println(dir.Name())
			files, err := ioutil.ReadDir(dbRootPath + dir.Name())
			check(err)
			for _, file := range files {
				if strings.Contains(file.Name(), "meta") {
					index, _ := strconv.Atoi(strings.Split(file.Name(), ".")[0])
					path := dbRootPath + "/" + strconv.Itoa(index) + "/" + file.Name()
					db.CubeMetaMap[index] = path
				}
			}
		}
	*/
	return db, nil

}

// shuffleCube guarantees the cubeIndex Cube is in memory
func (db *DB) shuffleCube(cubeIndex int) {
	if _, exists := db.Cube[cubeIndex]; exists {
		return
	} else {
		if len(db.Cube) < cacheSize {
			db.Cube[cubeIndex], _ = loadMetaFromDisk(cubeIndex)
		} else {
			// find a randomized map entity, shuffle it with cubeIndex
			/*
				randomFig := rand.Intn(len(db.Cube))
				// get the key
				keys := make([]int, 0)
				for k, _ := range db.Cube {
					keys = append(keys, k)
				}
				indexToReplace := keys[randomFig]
			*/
			indexToReplace := db.FindReplaceIndex()
			err := db.Cube[indexToReplace].writeToDisk()
			check(err)
			delete(db.Cube, indexToReplace)
			db.Cube[cubeIndex], _ = loadMetaFromDisk(cubeIndex)
		}
	}
	return

}

func getDataHeader(header []byte) (nextHead uint32, totalLength uint32, floatNum uint32, intNum uint32, stringNum uint32) {
	if len(header) != 20 {
		panic("Data's header size is wrong!")
	}
	nextHead = binary.BigEndian.Uint32(header[0:4])
	totalLength = binary.BigEndian.Uint32(header[4:8])
	floatNum = binary.BigEndian.Uint32(header[8:12])
	intNum = binary.BigEndian.Uint32(header[12:16])
	stringNum = binary.BigEndian.Uint32(header[16:20])
	return
}

func convertByteTodPoint(data []byte, floatNum uint32, intNum uint32, stringNum uint32) DataPoint {
	d := new(DataPoint)
	dataHead := uint32(0)

	d.FArr = make([]float64, floatNum)
	for i := uint32(0); i < floatNum; i++ {
		d.FArr[i] = Float64frombytes(data[dataHead : dataHead+8])
		dataHead += 8
	}

	d.IArr = make([]int, intNum)
	for i := uint32(0); i < intNum; i++ {
		json.Unmarshal(data[dataHead:dataHead+4], &d.IArr[i])
		dataHead += 4
	}

	d.SArr = make([]string, stringNum)
	if stringNum > 0 {

		var str string
		str = string(data[dataHead:len(data)])
		// json.Unmarshal(data[dataHead:len(data)], &str)
		//fmt.Printf("stringNum is %d, str is %s, len of string is %d\n", stringNum, str, len(str))
		d.SArr = strings.Split(str, "\t")
	}

	return *d
}

// ReadSingle is a function that read single point from .data file
// Depending whether the dataArr is in memory or not
func (db *DB) ReadSingle(cubeIndex int, metaIndex int) []DataPoint {
	// check if the cubeIndex is in cubemap, if not, load datacube to map
	db.shuffleCube(cubeIndex)
	// | offset(4bit) | header(| totalLength | FloatNum | IntNum | StringNum |) | data(float|int|string) |
	db.Cube[cubeIndex].AccessCount++ // add one read frequency to the cube
	cubeCell := db.Cube[cubeIndex].Metainfo.CellArr[metaIndex]
	dataNum := cubeCell.Count
	dPoints := make([]DataPoint, dataNum)
	count := 0
	curHead := cubeCell.CellHead
	dataArr := db.Cube[cubeIndex].DataArr
	if len(dataArr) == 0 {
		// read File as pointer
		dataFileName := dbRootPath + strconv.Itoa(cubeIndex) + "/" + strconv.Itoa(cubeIndex) + ".data"
		f, err := os.Open(dataFileName)
		defer f.Close()
		check(err)
		headerData := make([]byte, 20)
		for count < dataNum {
			f.ReadAt(headerData, int64(curHead))
			nextHead, totalLength, floatNum, intNum, stringNum := getDataHeader(headerData)
			dArr := make([]byte, totalLength)
			f.ReadAt(dArr, int64(curHead+20))
			dPoints[count] = convertByteTodPoint(dArr, floatNum, intNum, stringNum)
			curHead = nextHead
			count++
		}
	} else {
		// just load data from dArr
		for count < dataNum {
			nextHead, totalLength, floatNum, intNum, stringNum := getDataHeader(dataArr[curHead : curHead+20])
			data := dataArr[uint32(curHead)+20 : uint32(curHead)+20+totalLength]
			dPoints[count] = convertByteTodPoint(data, floatNum, intNum, stringNum)

			curHead = nextHead
			count++
		}
	}
	return dPoints

}

func (db *DB) ReadBatch(cubeIndex int, metaIndexes []int) []DataPoint {
	dPoints := make([]DataPoint, 0)
	// check if the dataArr is loaded in memory
	if _, exists := db.Cube[cubeIndex]; exists {
		// if the cube's meta is loaded in cache, check if dataArray is loaded
		if len(db.Cube[cubeIndex].DataArr) == 0 {
			db.Cube[cubeIndex].loadDataFromDisk(cubeIndex)
		}
	} else {
		db.shuffleCube(cubeIndex)
		db.Cube[cubeIndex].loadDataFromDisk(cubeIndex)
	}
	// read batch does not not count for the touch count for cube(redundant in readSingle)
	for _, metaIndex := range metaIndexes {
		dPoints = append(dPoints, db.ReadSingle(cubeIndex, metaIndex)...)
	}
	return dPoints
}

func (db *DB) testReadAll(cubeIndex int) {
	db.shuffleCube(cubeIndex)
	db.Cube[cubeIndex].loadDataFromDisk(cubeIndex)
}

func (db *DB) ReadAll(cubeIndex int) []DataPoint {
	dPoints := make([]DataPoint, 0)
	// check if the dataArr is loaded in memory
	if _, exists := db.Cube[cubeIndex]; exists {
		// if the cube's meta is loaded in cache, check if dataArray is loaded
		if len(db.Cube[cubeIndex].DataArr) == 0 {
			db.Cube[cubeIndex].loadDataFromDisk(cubeIndex)
		}
	} else {
		db.shuffleCube(cubeIndex)
		db.Cube[cubeIndex].loadDataFromDisk(cubeIndex)
	}
	// convert all data from dataArr
	// dataFormat: | offset(4bit) | header(| totalLength | FloatNum | IntNum | StringNum |) | data(float|int|string) |
	startindex := uint32(0)
	for startindex < uint32(len(db.Cube[cubeIndex].DataArr)) {
		header := db.Cube[cubeIndex].DataArr[startindex : startindex+20]
		startindex += 20
		_, totalLength, floatNum, intNum, stringNum := getDataHeader(header)
		dArr := db.Cube[cubeIndex].DataArr[startindex : startindex+totalLength]
		dPoints = append(dPoints, convertByteTodPoint(dArr, floatNum, intNum, stringNum))
		startindex += totalLength
	}
	// add k% of total length touch count to this cube, initially k is 50%
	db.Cube[cubeIndex].AccessCount += int64(math.Ceil(readSingleAllRatio * float64(len(dPoints))))
	return dPoints
}

// Read ... Redundant function, plz don't use
func (db *DB) Read() error {
	return nil
}

// Load ... Redundant function, plz don't use
func (db *DB) Load() error {
	return nil
}

// Keys get the key set of a map
func Keys(m map[int]interface{}) (keys []int) {
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

// LFUCache is for calculating which index to be replaced in shuffle
type LFUCache struct {
	AccessCount int64
	InsertTime  int64
}

// FindReplaceIndex find the lest frequent index from a random sampled index of cube cache
func (db *DB) FindReplaceIndex() int {
	// find k random
	k := int(randomSampleRatio * cacheSize)
	cubeToReplace := make(map[int]*LFUCache)
	for len(cubeToReplace) < k || len(cubeToReplace) >= len(db.Cube) {
		randNum := rand.Intn(len(db.Cube))
		if _, exists := cubeToReplace[randNum]; !exists {
			cubeToReplace[randNum] = &LFUCache{AccessCount: db.Cube[randNum].AccessCount, InsertTime: db.Cube[randNum].InsertTime}
		}
	}
	timeNow := time.Now().Unix()
	dropIndex := 0
	minFrequency := math.MaxFloat64
	for index, v := range cubeToReplace {
		frequency := float64(v.AccessCount * 1.0 / (timeNow - v.InsertTime))
		if minFrequency < frequency {
			minFrequency = frequency
			dropIndex = index
		}
	}
	return dropIndex
}

// CreateMetaCube function create the cube from cubeId (index of tree node) and cubeSize (size of dimension)
// then we need to feed the data from unorganized batch of datapoints to the cube or read data from files
func (db *DB) CreateMetaCube(cubeId int, cubeSize int, dims []uint, maxs []float64, mins []float64) error {
	//fmt.Printf("Creating metaCube for index:%d...\n", cubeId)
	if err := os.MkdirAll(path.Join(dbRootPath, strconv.Itoa(cubeId)), 0700); err != nil {
		return err
	}
	// TODO: Change to sync.pool?
	// free last MetaCube used
	// TODO: LRU => current size 1, change randomize replace to be LRU style
	if len(db.Cube) < cacheSize {
		db.Cube[cubeId] = &MetaCube{
			Metainfo:    MetaInfo{CubeIndex: cubeId, Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, Dims: dims, Maxs: maxs, Mins: mins},
			DataArr:     make([]byte, dataArraySize),
			AccessCount: 0,
			InsertTime:  time.Now().Unix()}
	} else {
		// randomly choose an index from current CubeMataMap and then replace it.
		/*
			randomFig := rand.Intn(len(db.Cube))
			keys := make([]int, 0)
			for k, _ := range db.Cube {
				keys = append(keys, k)
			}
			toReplaceIdx := keys[randomFig]
		*/
		toReplaceIdx := db.FindReplaceIndex()
		// before delete the entry, write back meta info and data to disk
		err := db.Cube[toReplaceIdx].writeToDisk()
		check(err)
		delete(db.Cube, toReplaceIdx)
		db.Cube[cubeId] = &MetaCube{
			Metainfo:    MetaInfo{CubeIndex: cubeId, Cubesize: cubeSize, CellArr: make([]CubeCell, cubeSize), GlobalOffset: 0, Dims: dims, Maxs: maxs, Mins: mins},
			DataArr:     make([]byte, dataArraySize),
			AccessCount: 0,
			InsertTime:  time.Now().Unix()}

	}
	return nil
}

func (db *DB) CubeExists(cubeId int) bool {
	_, exists := db.CubeMetaMap[cubeId]
	return exists
}

// Feed accepts data batch from upper layer and add data to DB's cube map, according to whether the cube is in map
// if the cube is not in map, then there's a replacement of memory from IO
func (db *DB) Feed(batch *DataBatch) error {
	cubeSize := int(batch.Capacity)
	var err error
	if !db.CubeExists(batch.CubeId) { // even cube file does not existed (This is a new cube file), then we
		// could just feed a new cube
		if err = db.CreateMetaCube(batch.CubeId, cubeSize, batch.Dims, batch.Maxs, batch.Mins); err != nil {
			fmt.Println("Fail to create cube")
			return err
		}
		db.feedBatchToCube(batch.DPoints, batch.CubeId)
	} else {
		// check if this cube is in cubeMap of db
		if _, exists := db.Cube[batch.CubeId]; exists {
			// this cube data is currently in momery, do append data to cube
			if len(db.Cube[batch.CubeId].DataArr) == 0 {
				// if there's no data arr, only metaData exists, load data from disk first
				db.Cube[batch.CubeId].loadDataFromDisk(batch.CubeId)
			}
			db.feedBatchToCube(batch.DPoints, batch.CubeId)
		} else {
			// load Cube File from disk
			// if length is less than cacheSize, just append new
			if len(db.Cube) < cacheSize {
				db.Cube[batch.CubeId], _ = loadCubeFromDisk(batch.CubeId)
			} else {
				db.shuffleCube(batch.CubeId)
				db.Cube[batch.CubeId].loadDataFromDisk(batch.CubeId)
				// find a randomized entry in cube map, load a new MetaCube of this batch index, replace it and then add batch data to it
				/*
					indexToReplace := rand.Intn(len(db.Cube))
					err = db.Cube[indexToReplace].writeToDisk()
					check(err)
					delete(db.Cube, indexToReplace)
					db.Cube[batch.CubeId], _ = loadCubeFromDisk(batch.CubeId)
				*/
			}
			// append new data, feed to this cube
			db.feedBatchToCube(batch.DPoints, batch.CubeId)
		}
	}
	//fmt.Printf("After feed, data length of cube %d is %d\n", batch.CubeId, len(db.Cube[batch.CubeId].DataArr))
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

// loadDataFromDisk load dataArr from disk according to the index of cube
func (c *MetaCube) loadDataFromDisk(index int) error {
	indexString := strconv.Itoa(index)
	dataPath := dbRootPath + indexString + "/" + indexString + ".data"
	if _, err := os.Stat(dataPath); err == nil {
		dataByte, _ := ioutil.ReadFile(dataPath)
		c.DataArr = append(c.DataArr, dataByte...)
	}

	return nil
}

// loadMetaFromDisk load metadata from disk(disgard dataArr), returns a metaCube with metainfo but a length of dataArr
// of zero, if further need the loading of data, should call loadDataFromDisk
func loadMetaFromDisk(index int) (*MetaCube, error) {
	c := new(MetaCube)
	c.DataArr = make([]byte, 0)
	indexString := strconv.Itoa(index)
	metaPath := dbRootPath + indexString + "/" + indexString + ".meta"
	dataByte, err := ioutil.ReadFile(metaPath)
	err = json.Unmarshal(dataByte, &c.Metainfo)
	c.InsertTime = time.Now().Unix()
	c.AccessCount = 0
	check(err)
	return c, err
}

// loadCubeFromDisk load the whole cube include data array and meta data
func loadCubeFromDisk(index int) (c *MetaCube, err error) {
	c = new(MetaCube)
	indexString := strconv.Itoa(index)
	dataPath := dbRootPath + indexString + "/" + indexString + ".data"
	var dataByte []byte
	if _, err := os.Stat(dataPath); err == nil {
		dataByte, _ = ioutil.ReadFile(dataPath)
		copy(c.DataArr, dataByte)
	}

	metaPath := dbRootPath + indexString + "/" + indexString + ".meta"
	dataByte, err = ioutil.ReadFile(metaPath)
	err = json.Unmarshal(dataByte, &c.Metainfo)
	c.InsertTime = time.Now().Unix()
	c.AccessCount = 0
	check(err)
	return c, err
}

func (c *MetaCube) writeToDisk() error {
	// save data array to be index.data
	// save the left to be index.meta
	//fmt.Printf("Writing back cube %d to disk...\n", c.Metainfo.CubeIndex)
	stringIdx := strconv.Itoa(c.Metainfo.CubeIndex)
	// create index file dir if not exists, if not, just mkdir
	if _, err := os.Stat(dbRootPath + stringIdx + "/"); os.IsNotExist(err) {
		os.Mkdir(dbRootPath+stringIdx+"/", os.ModePerm)
	}
	dataFileName := dbRootPath + stringIdx + "/" + stringIdx + ".data"
	metaFileName := dbRootPath + stringIdx + "/" + stringIdx + ".meta"

	// dump data file
	if len(c.DataArr) > 0 {
		err := ioutil.WriteFile(dataFileName, c.DataArr, os.ModePerm)
		check(err)
	}
	// marshal Metainfo to be []byte
	b, err := json.Marshal(c.Metainfo)
	check(err)
	err = ioutil.WriteFile(metaFileName, b, os.ModePerm)
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
	//fmt.Printf("GlobalOffset = %d\n", cube.Metainfo.GlobalOffset)
	TailCopy := c.CellTail
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
	binary.BigEndian.PutUint32(offset, 0)
	lenHeader := uint32(len(header))
	lenByteArr := uint32(len(byteArr))

	cube.writeEntry(offset)
	cube.Metainfo.GlobalOffset += 4
	cube.writeEntry(header)
	cube.Metainfo.GlobalOffset += lenHeader
	cube.writeEntry(byteArr)
	cube.Metainfo.GlobalOffset += lenByteArr

	// update previous pointer to point this node
	binary.BigEndian.PutUint32(offset, uint32(globalOffsetCopy))

	cube.replaceEntry(offset, TailCopy, 4)

}

func (c *MetaCube) replaceEntry(data []byte, start uint32, length uint32) {
	//fmt.Printf("In replaceEntry: start = %d, dataArr_Len = %d\n", start, len(c.DataArr))
	for i := uint32(0); i < length; i++ {
		if (start + i) >= uint32(len(c.DataArr)) {
			//fmt.Println("replace byte for data is wrong...Index out of range")
			c.DataArr = append(c.DataArr, data[i:]...)
			return
		} else {
			c.DataArr[start+i] = data[i]
		}

	}
}

// TODO: change this format
func (c *MetaCube) writeEntry(data []byte) {
	c.DataArr = append(c.DataArr, data...)

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
			byteData, _ := Float64bytes(fl)
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
	//fmt.Printf("lenString: %d, content:%s\n", lenString, d.SArr)
	if lenString > 0 {
		for _, str := range d.SArr {
			byteData := []byte(str + "\t")
			res = append(res, byteData...)
			//fmt.Printf("length of res after append:%d\n", len(res))
		}
	}
	totalLength := len(res)
	// TODO: (Yeech) spare the space later, maybe change uint32 to uint16
	bs := make([]byte, 4)
	binary.BigEndian.PutUint32(bs, uint32(totalLength))
	header = append(header, bs...)
	binary.BigEndian.PutUint32(bs, uint32(lenFloat))
	header = append(header, bs...)
	binary.BigEndian.PutUint32(bs, uint32(lenInt))
	header = append(header, bs...)
	binary.BigEndian.PutUint32(bs, uint32(lenString))
	header = append(header, bs...)
	return res, header
}

func calculateCubeSize(dimSize []uint) int {
	cubeSize := uint(1)
	for _, i := range dimSize {
		cubeSize *= i
	}
	return int(cubeSize)
}

func Float64frombytes(bytes []byte) float64 {
	bits := binary.LittleEndian.Uint64(bytes)
	float := math.Float64frombits(bits)
	return float
}

func Float64bytes(float float64) ([]byte, error) {
	bits := math.Float64bits(float)
	bytes := make([]byte, 8)
	binary.LittleEndian.PutUint64(bytes, bits)
	return bytes, nil
}
