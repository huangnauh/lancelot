package main

import (
	"context"

	"github.com/pingcap/tidb/config"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/store/tikv"
)

func main() {
	cfg := config.GetGlobalConfig()
	cfg.Log.Level = "debug"
	config.StoreGlobalConfig(cfg)
	driver := tikv.Driver{}
	store, err := driver.Open("tikv://10.0.5.89:2379")
	if err != nil {
		panic(err)
	}

	txn1, err := store.Begin()
	if err != nil {
		panic(err)
	}
	err = txn1.LockKeys(context.Background(), new(kv.LockCtx), []byte("key1"))
	if err != nil {
		panic(err)
	}

	txn2, err := store.Begin()
	if err != nil {
		panic(err)
	}
	err = txn2.Set([]byte("key1"), []byte("value1"))
	if err != nil {
		panic(err)
	}
	err = txn2.Commit(context.Background())
	if err != nil {
		panic(err)
	}

	// err = txn1.Set([]byte("key2"), []byte("value2"))
	// if err != nil {
	// 	panic(err)
	// }
	err = txn1.Commit(context.Background())
	if err != nil {
		panic(err)
	}
	// txn, err := store.Begin()
	// if err != nil {
	// 	panic(err)
	// }
	// v, err := txn.Get(context.Background(), []byte("key1"))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(v)
	// v, err = txn.Get(context.Background(), []byte("key2"))
	// if err != nil {
	// 	panic(err)
	// }
	// fmt.Println(v)
}
