package command_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetHandle(t *testing.T) {
	client := cmd.GetClient()
	txn := client.NewTxn()
	err := txn.Begin()
	assert.Nil(t, err, "txn begin")
	defer txn.Rollback()
	// cmd.SetHandle()
}
