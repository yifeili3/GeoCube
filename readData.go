package geocube

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
)

//Example
func ImportData(path string) ([]DataPoint, error) {
	//dropoff_datetime, pickup_datetime, dropoff_longitude, dropoff_latitude, pickup_longitude, pickup_latitude, trip_distance, total_amount, tip_amount
	a := AttributeDataPointMapping{
		FloatArr:  []int{2, 3, 4, 5, 6, 7, 8},
		StringArr: []int{0, 1},
	}
	return importCSV2DataPoint(path, a)
}

//AttributeDataPointMapping ..
type AttributeDataPointMapping struct {
	FloatArr  []int
	IntArr    []int
	StringArr []int
}

func importCSV2DataPoint(path string, attributeOrder AttributeDataPointMapping) ([]DataPoint, error) {
	csvFile, _ := os.Open(path)
	reader := csv.NewReader(bufio.NewReader(csvFile))
	var dPointArr []DataPoint
	for {
		line, err := reader.Read()
		if err == io.EOF {
			break
		} else if err != nil {
			fmt.Println("Can not read from file")
			return nil, err
		}
		var fArr []float64
		for order := range attributeOrder.FloatArr {
			f, _ := strconv.ParseFloat(line[order], 64)
			fArr = append(fArr, f)
		}

		var iArr []int
		for order := range attributeOrder.IntArr {
			temp, _ := strconv.ParseInt(line[order], 10, 32)
			iArr = append(iArr, int(temp))
		}

		var sArr []string
		for order := range attributeOrder.StringArr {
			sArr = append(sArr, line[order])
		}

		dPointArr = append(dPointArr, DataPoint{
			Idx:  -1,
			FArr: fArr,
			IArr: iArr,
			SArr: sArr,
		})

	}
	return dPointArr, nil
}
