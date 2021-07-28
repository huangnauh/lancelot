package command

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestSetHandle(t *testing.T) {
	txn := cmd.client.NewTxn()
	err := txn.Begin()
	assert.Nil(t, err, "txn begin")
	defer txn.Rollback()
	// cmd.SetHandle()
}
