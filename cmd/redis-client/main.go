package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"time"

	goredis "github.com/go-redis/redis/v8"
	"github.com/gomodule/redigo/redis"
	"github.com/nitishm/go-rejson/v4"
)

// Name - student name
type Name struct {
	First  string `json:"first,omitempty"`
	Middle string `json:"middle,omitempty"`
	Last   string `json:"last,omitempty"`
}

// Student - student object
type Student struct {
	Name Name `json:"name,omitempty"`
	Rank int  `json:"rank,omitempty"`
}

func Example_JSONSet(rh *rejson.Handler) {

	student := Student{
		Name: Name{
			"Mark",
			"S",
			"Pronto",
		},
		Rank: 1,
	}
	res, err := rh.JSONSet("student", ".", student)
	if err != nil {
		log.Fatalf("Failed to JSONSet %s", err)
		return
	}

	if res.(string) == "OK" {
		fmt.Printf("Success: %s\n", res)
	} else {
		fmt.Println("Failed to Set: ")
	}

	rank, err := redis.Int(rh.JSONGet("student", ".rank"))
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}
	fmt.Printf("rank read from redis : %d\n", rank)

	studentJSON, err := redis.Bytes(rh.JSONGet("student", "."))
	if err != nil {
		log.Fatalf("Failed to JSONGet")
		return
	}
	readStudent := Student{}
	err = json.Unmarshal(studentJSON, &readStudent)
	if err != nil {
		log.Fatalf("Failed to JSON Unmarshal")
		return
	}

	fmt.Printf("Student read from redis : %#v\n", readStudent)
}

func main1() {
	_ = goredis.NewFailoverClient(&goredis.FailoverOptions{
		MasterName:    "mymaster",
		SentinelAddrs: []string{"10.0.5.137:6379"},
		Username:      "root",
		Password:      "root",
	})
}

func main2() {
	client := goredis.NewClient(&goredis.Options{})
	ctx := context.Background()
	pubsub := client.Subscribe(ctx, "mychannel")
	defer pubsub.Close()

	{
		msgi, err := pubsub.ReceiveTimeout(ctx, time.Second)
		if err != nil {
			log.Fatalf("Failed to Receive: %s", err)
			return
		}
		subscr := msgi.(*goredis.Subscription)
		fmt.Printf("Received: %v\n", subscr)
	}

	ch := pubsub.Channel(
		goredis.WithChannelSize(10),
		goredis.WithChannelHealthCheckInterval(time.Second),
	)

	text := "w"
	err := client.Publish(ctx, "mychannel", text).Err()
	if err != nil {
		log.Fatalf("Failed to Publish: %s", err)
		return
	}

	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()
	for {
		select {
		case <-ticker.C:
			return
		case msg := <-ch:
			fmt.Printf("Message: %v\n", msg)
		}
	}
}

func main() {
	var addr = flag.String("Server", "10.0.5.137:6379", "Redis server address")

	rh := rejson.NewReJSONHandler()
	flag.Parse()

	// Redigo Client
	conn, err := redis.Dial("tcp", *addr, redis.DialPassword("root"))
	if err != nil {
		log.Fatalf("Failed to connect to redis-server @ %s", *addr)
	}
	defer func() {
		if err := conn.Close(); err != nil {
			log.Fatalf("redis - failed to communicate to redis-server: %v", err)
		}
	}()

	rh.SetRedigoClient(conn)
	fmt.Println("Executing Example_JSONSET for Redigo Client")
	Example_JSONSet(rh)

	// GoRedis Client
	cli := goredis.NewClient(&goredis.Options{Addr: *addr, Password: "root"})
	defer func() {
		if err := cli.Close(); err != nil {
			log.Fatalf("goredis - failed to communicate to redis-server: %v", err)
		}
	}()
	rh.SetGoRedisClient(cli)
	fmt.Println("\nExecuting Examplerh.Set_JSONSET for Redigo Client")
	Example_JSONSet(rh)
}
