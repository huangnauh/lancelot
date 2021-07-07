package server

import "errors"

var (
	errWatchInsideMulti = "ERROR WATCH inside MULTI is not allowed"
	errMultiNested      = "ERROR MULTI calls can not be nested"
	errEXECErr          = "ERROR EXEC without MULTI"
	errDISCARDErr       = "ERROR DISCARD without MULTI"
	errMultiErr         = "ERROR without MULTI"
	errTransactionErr   = "ERROR Transaction discarded because of previous errors."

	missingTxn        = errors.New("missing transcation")
	invalidTxn        = errors.New("invalid transcation")
	wrongNumberOfArgs = errors.New("wrong number of arguments")
)

const (
	OK     = "OK"
	Queued = "QUEUED"
)
