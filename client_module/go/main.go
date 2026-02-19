package main

import (
	"fmt"
	"log"
	"os"
	smartclient "smartresource/clientmodule/go/rdb"
	"smartresource/clientmodule/go/test"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

func main() {
	err := smartclient.InitWithConfig("config.yaml")
	if err != nil {
		fmt.Println("Configure error:", err)
		return
	}

	if len(os.Args) > 1 && os.Args[1] == "m" {
		count, _ := strconv.ParseInt(os.Args[2], 10, 64)
		test.MakeTestData(int(count))
		return
	}

	count := 100000
	if len(os.Args) > 1 {
		cnt, _ := strconv.ParseInt(os.Args[1], 10, 64)
		count = int(cnt)
	}
	log.Printf("Starting test with %d concurrent clients\n", count)

	var countOK, countError atomic.Int64
	start := time.Now()
	wg := sync.WaitGroup{}
	batchSize := 10000
	loopCount := count / batchSize
	if count%batchSize != 0 {
		loopCount++
	}
	for i := 0; i < loopCount; i++ {
		for j := range batchSize {
			if i*batchSize+j >= count {
				break
			}
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				result, err := test.Test1()
				if err != nil {
					log.Printf("Test error: %v", err)
					countError.Add(1)
				} else {
					if j == 0 {
						log.Printf("Test result: %+v", result)
					}
					countOK.Add(1)
				}
			}(j)
		}
		wg.Wait()
		log.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
	}
	fmt.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
}
