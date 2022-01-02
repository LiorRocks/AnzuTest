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
	wt1ch := make(chan []AppkeyPlatformFormat)
	wt2ch := make(chan []AppkeyPlatformFormat)
	wt3ch := make(chan []AppkeyPlatformFormat)
	wt4ch := make(chan []AppkeyPlatformFormat)

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
	for i := 0; i < 4; i++ {
		switch i {
		case 0:
			{
				go worker(wt1ch)
				appKeys1 := make([]AppkeyPlatformFormat, numOfRows/4)
				initializeEmptyArray(appKeys1, numOfRows/4)
				if err = pr.Read(&appKeys1); err != nil {
					log.Println("Read error", err)
				}
				wt1ch <- appKeys1

				break
			}
		case 1:
			{
				go worker(wt2ch)
				appKeys2 := make([]AppkeyPlatformFormat, numOfRows/4)
				initializeEmptyArray(appKeys2, numOfRows/4)
				if err = pr.Read(&appKeys2); err != nil {
					log.Println("Read error", err)
				}
				wt2ch <- appKeys2

				break
			}
		case 2:
			{
				go worker(wt3ch)
				appKeys3 := make([]AppkeyPlatformFormat, numOfRows/4)
				initializeEmptyArray(appKeys3, numOfRows/4)
				if err = pr.Read(&appKeys3); err != nil {
					log.Println("Read error", err)
				}
				wt3ch <- appKeys3

				break
			}
		case 3:
			{
				go worker(wt1ch)
				appKeys4 := make([]AppkeyPlatformFormat, numOfRows/4)
				initializeEmptyArray(appKeys4, numOfRows/4)
				if err = pr.Read(&appKeys4); err != nil {
					log.Println("Read error", err)
				}
				wt4ch <- appKeys4

				break
			}
		}
	}
	printTotalStats()
}

func worker(c chan []AppkeyPlatformFormat) {
	var data []AppkeyPlatformFormat
	for {
		data = <-c
		for _, value := range data {
			go processParquetRow(value)
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
