package server

import "errors"

var (
	errWatchInsideMulti = "ERROR WATCH inside MULTI is not allowed"
	errMultiNested      = "ERROR MULTI calls can not be nested"
	errEXECErr          = "ERROR EXEC without MULTI"
	errDISCARDErr       = "ERROR DISCARD without MULTI"
	errMultiErr         = "ERROR without MULTI"
	errTransactionErr   = "ERROR Transaction discarded because of previous errors."
	errNotInteger       = errors.New("ERROR value is not an integer or out of range")
	errInvalidExpire    = errors.New("ERROR invalid expire time in set")
	errSyntax           = errors.New("ERROR syntax error")

	missingTxn        = errors.New("missing transcation")
	invalidTxn        = errors.New("invalid transcation")
	wrongNumberOfArgs = errors.New("wrong number of arguments")

	ErrValueTooShort = errors.New("ERR value is too short")
)

const (
	OK     = "OK"
	Queued = "QUEUED"
)
