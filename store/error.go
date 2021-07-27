package store

import "errors"

var (
	KeyNotFound = errors.New("key not found")
	ReachLimit  = errors.New("reach the limit")
)
