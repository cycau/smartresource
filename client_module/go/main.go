package main

import (
	"fmt"
	smartclient "smartresource/clientmodule/go/rdb"
	"smartresource/clientmodule/go/test"
	"sync"
	"time"
)

func main() {
	err := smartclient.Configure("config.yaml")
	if err != nil {
		fmt.Println("Configure error:", err)
		return
	}
	start := time.Now()
	wg := sync.WaitGroup{}
	for i := 0; i < 20; i++ {
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
