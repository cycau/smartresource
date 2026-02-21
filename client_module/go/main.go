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

	count := 1
	batchSize := 1000
	if len(os.Args) > 1 {
		num, _ := strconv.ParseInt(os.Args[1], 10, 64)
		count = int(num)
	}
	if len(os.Args) > 2 {
		num, _ := strconv.ParseInt(os.Args[2], 10, 64)
		batchSize = int(num)
	}
	log.Printf("*** Starting Test %d times with Concurrents %d\n", count, batchSize)

	var countOK, countError atomic.Int64
	start := time.Now()
	wg := sync.WaitGroup{}
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

				var err error
				var result *smartclient.Records
				funcIdx := 0 // rand.Intn(4)
				switch funcIdx {
				case 0:
					result, err = test.TestTxCase1()
				case 1:
					result, err = test.TestTxCase2()
				case 2:
					result, err = test.TestRandomSelect()
				case 3:
					_, err = test.TestRandomUpdate()
				case 4:
					result, err = test.TestRandomTx()
				}
				if err != nil {
					log.Printf("Test error: %v", err)
					countError.Add(1)
					return
				}
				countOK.Add(1)
				if j == 0 {
					log.Printf("Test result: %+v", result)
				}
			}(j)
		}
		wg.Wait()
		log.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
	}
	fmt.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
}
