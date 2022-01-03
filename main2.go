package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/xitongsys/parquet-go-source/local"
	"github.com/xitongsys/parquet-go/reader"
)

const ParquetFilename = "be_test.parquet"

type AppkeyPlatformFormat struct {
	appkey   string `parquet:"name=appkey, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	platform string `parquet:"name=platform, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
	action   string `parquet:"name=action, type=BYTE_ARRAY, convertedtype=UTF8, encoding=PLAIN_DICTIONARY"`
}

type AppkeyPlatformSummary struct {
	appkey      string
	platform    string
	plays       int64
	empties     int64
	impressions int64
}

var TotalValues map[string]AppkeyPlatformSummary = make(map[string]AppkeyPlatformSummary, 0)

var wg = sync.WaitGroup{}

func main() {
	c := make(chan []AppkeyPlatformFormat)

	for i := 0; i < 4; i++ {
		go worker(c)
	}

	var err error
	fr, err := local.NewLocalFileReader(ParquetFilename)
	if err != nil {
		log.Println("Can't open file")
		return
	}

	pr, err := reader.NewParquetReader(fr, nil, 3)
	if err != nil {
		log.Println("Can't create parquet reader", err)
		return
	}

	numOfRows := int(pr.GetNumRows())
	numOfChunks := numOfRows / 10000
	for i := 0; i < numOfChunks; i++ {
		appKeys1 := make([]AppkeyPlatformFormat, 10000)

		if err = pr.Read(&appKeys1); err != nil {
			log.Println("Read error", err)
		}
		c <- appKeys1

	}

	printTotalStats()
}

func worker(c chan []AppkeyPlatformFormat) {
	var data []AppkeyPlatformFormat
	for {
		data = <-c
		for _, value := range data {
			processParquetRow(value)
		}
	}
}

func processParquetRow(appkeyPlatformFormat AppkeyPlatformFormat) {
	if val, ok := TotalValues[appkeyPlatformFormat.appkey+appkeyPlatformFormat.platform]; ok {
		switch appkeyPlatformFormat.action {
		case "empty":
			{
				val.empties++
				break
			}
		case "play":
			{
				val.plays++
				break
			}
		case "impression":
			{
				val.impressions++
				break
			}
		}
	} else {
		var appkeyPlatformSummary AppkeyPlatformSummary
		appkeyPlatformSummary.appkey = appkeyPlatformFormat.appkey
		appkeyPlatformSummary.platform = appkeyPlatformFormat.platform
		switch appkeyPlatformFormat.action {
		case "empty":
			{
				appkeyPlatformSummary.empties++
				break
			}
		case "play":
			{
				appkeyPlatformSummary.plays++
				break
			}
		case "impression":
			{
				appkeyPlatformSummary.impressions++
				break
			}
		}
		TotalValues[appkeyPlatformFormat.appkey+appkeyPlatformFormat.platform] = appkeyPlatformSummary
	}
	wg.Done()
}

func printTotalStats() {
	for _, value := range TotalValues {
		fmt.Println(value)
	}
}

func initializeEmptyArray(appKeys []AppkeyPlatformFormat, numElements int) {
	for i := 0; i < numElements; i++ {
		appKeys[i] = AppkeyPlatformFormat{}
	}
}
