package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"log"

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

func main() {
	var addr = flag.String("Server", "10.0.5.137:16379", "Redis server address")

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
	fmt.Println("\nExecuting Example_JSONSET for Redigo Client")
	Example_JSONSet(rh)
}
