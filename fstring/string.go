package fstring

import (
	"fmt"

	"github.com/dansbeer/go-forge/fstring/fcsv"
	"github.com/dansbeer/go-forge/fstring/fid"
)

var (
	GenerateID   = fid.GenerateID
	NewCSVReader = fcsv.NewCSVReader
)

func GetByAny(in any) (res string) {
	switch in := in.(type) {
	case string:
		res = in
	case *string:
		res = *in
	default:
		fmt.Printf("/* ----------------------------------- UNK[%T] ---------------------------------- */\n", in)
		res = fmt.Sprint(in)
	}
	return
}
