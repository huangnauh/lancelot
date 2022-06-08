package main

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/go-redis/redis/v8"
)

const SCRIPT = `
local limitKey = tostring(KEYS[1])
local limit = tonumber(ARGV[1])
local expireS = tonumber(ARGV[2])
local current = tonumber(redis.call('get', limitKey) or '0')
if (current + 1 > limit) then
 return 0
end
redis.call("incr", limitKey)
if (current == 0) then
 redis.call("expire", limitKey, expireS)
end
return 1
`

func set(key string, i int, wg *sync.WaitGroup) {
	client := redis.NewClient(&redis.Options{
		Addr: "10.0.5.137:26379",
	})
	defer wg.Done()
	ctx := context.Background()
	ret, err := client.Eval(ctx, SCRIPT, []string{key}, 100, 100).Result()
	// time.Sleep(time.Duration(i) * time.Millisecond)
	// pipe := client.TxPipeline()
	// pipe.Get(ctx, key)
	// pipe.Incr(ctx, key)
	// pipe.Expire(ctx, key, time.Hour)
	// ret, err := pipe.Exec(ctx)

	// ret, err := pipe.Get(ctx, key).Result()
	// if err != nil && err != redis.Nil {
	// 	fmt.Printf("i: %d, err: %s\n", i, err)
	// 	return
	// }
	// fmt.Printf("i: %d, before: %s\n", i, ret)
	// res, err := client.Incr(ctx, key).Result()
	// if err != nil {
	// 	fmt.Printf("i: %d, err: %s\n", i, err)
	// 	return
	// }
	// fmt.Printf("i: %d, after: %d\n", i, res)
	// rev, err := client.Expire(ctx, key, time.Hour).Result()
	if err != nil {
		fmt.Printf("i: %d, err: %s\n", i, err)
		return
	}
	fmt.Printf("i: %d, ret: %v\n", i, ret)
}

func main() {
	client := redis.NewClient(&redis.Options{
		Addr: "10.0.5.137:26379",
	})
	ctx := context.Background()
	client.PSetEx(ctx, "key", 100, 1000*time.Millisecond).Result()
	time.Sleep(2000 * time.Millisecond)
	wg := &sync.WaitGroup{}
	for i := 0; i < 2; i++ {
		wg.Add(1)
		go set("key", i, wg)
	}
	wg.Wait()
}
