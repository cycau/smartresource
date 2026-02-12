package main

import (
	"fmt"
	"log"
	"os"
	smartclient "smartresource/clientmodule/go/rdb"
	"smartresource/clientmodule/go/test"
	"strconv"
	"sync"
	"time"
)

func main() {
	count := 10
	if len(os.Args) > 1 {
		cnt, _ := strconv.ParseInt(os.Args[1], 10, 64)
		count = int(cnt)
	}
	log.Printf("Starting test with %d concurrent clients\n", count)

	err := smartclient.InitWithConfig("config.yaml")
	if err != nil {
		fmt.Println("Configure error:", err)
		return
	}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < count; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			test.Test1()
		}(i)
	}
	wg.Wait()
	end := time.Now()
	fmt.Printf("Time: %d\n", end.Sub(start).Milliseconds())
}
