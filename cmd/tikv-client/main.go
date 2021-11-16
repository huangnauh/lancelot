package main

import (
	"fmt"

	"github.com/tikv/client-go/v2/tikv"
)

func main() {
	store, err := tikv.NewTxnClient([]string{"10.0.5.89:2379"})
	if err != nil {
		panic(err)
	}

	txn, err := store.Begin()
	if err != nil {
		panic(err)
	}
	it, err := txn.Iter([]byte("t"), []byte("u"))
	if err != nil {
		panic(err)
	}

	for it.Valid() {
		fmt.Printf("key: %s, value:%s\n", []byte(it.Key()), []byte(it.Value()))
		it.Next()
	}
	it.Close()
}
