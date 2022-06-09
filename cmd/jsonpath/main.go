package main

import (
	"fmt"

	"github.com/spyzhov/ajson"
)

func main() {
	json := []byte(`{}`)

	root, err := ajson.Unmarshal(json)
	if err != nil {
		fmt.Println(err)
		return
	}
	nodes, err := root.JSONPath("$.price")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, node := range nodes {
		node.SetNumeric(node.MustNumeric() * 1.25)
		node.Parent().AppendObject("currency", ajson.StringNode("", "EUR"))
	}
	result, err := ajson.Marshal(root)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Printf("%s", result)
}
