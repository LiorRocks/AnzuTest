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
	wg.Add(numOfRows)
	for i := 0; i < numOfRows; i++ {
		go processParquetRow(pr)
	}
	wg.Wait()
	printTotalStats()
}

func processParquetRow(pr *reader.ParquetReader) {
	var appkeyPlatformFormat AppkeyPlatformFormat = AppkeyPlatformFormat{}
	pr.Read(&appkeyPlatformFormat)
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
