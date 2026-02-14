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
	for i := 0; i < count/100000; i++ {
		for j := range 100000 {
			wg.Add(1)
			go func(j int) {
				defer wg.Done()
				err := test.Test1()
				if err != nil {
					log.Printf("Test error: %v", err)
					countError.Add(1)
				} else {
					countOK.Add(1)
				}
			}(j)
		}
		wg.Wait()
		log.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
	}
	fmt.Printf("Time: %dms, OK: %d, Error: %d\n", time.Since(start).Milliseconds(), countOK.Load(), countError.Load())
}
