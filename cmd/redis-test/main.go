package main

import (
	"context"
	"fmt"
	"sync"

	"github.com/go-redis/redis/v8"
)

func set(client *redis.Client, key string, i int, wg *sync.WaitGroup) {
	defer wg.Done()
	ctx := context.Background()
	res, err := client.Incr(ctx, key).Result()
	if err != nil {
		fmt.Printf("i: %d, err: %s\n", i, err)
		return
	}
	fmt.Printf("i: %d, result: %d\n", i, res)
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "10.0.5.137:26379",
	})
	wg := &sync.WaitGroup{}
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go set(client, "key", i, wg)
	}
	wg.Wait()
}
