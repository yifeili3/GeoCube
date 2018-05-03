package main

import (
	"encoding/json"
	"unsafe"
)

/*
Pick the first occurence when duplicates exist
*/
func argmax(values []float64) int {
	max_v := values[0]
	max_ind := 0
	for i, v := range values {
		if v > max_v {
			max_v = v
			max_ind = i
		}
	}
	return max_ind
}

/*
Pick the first occurence when duplicates exist
*/
func argmin(values []float64) int {
	min_v := values[0]
	min_ind := 0
	for i, v := range values {
		if v < min_v {
			min_v = v
			min_ind = i
		}
	}
	return min_ind
}

/*
	MarshalDPtoByte marshal databatch into byte array
*/
func MarshalDPtoByte(batch DataBatch) []byte {

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

func UnmarshalBytetoDP(data []byte) *DataBatch {

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
