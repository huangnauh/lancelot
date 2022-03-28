package main

import (
	"fmt"
	"strconv"

	"github.com/axiomhq/hyperloglog"
)

func estimateError(got, exp uint64) float64 {
	var delta uint64
	if got > exp {
		delta = got - exp
	} else {
		delta = exp - got
	}
	return float64(delta) / float64(exp)
}

func main() {
	axiom := hyperloglog.NewNoSparse()
	step := 10
	unique := map[string]bool{}

	for i := 1; len(unique) <= 20000000; i++ {
		str := "stream-" + strconv.Itoa(i)
		axiom.Insert([]byte(str))
		unique[str] = true

		if len(unique)%step == 0 || len(unique) == 20000000 {
			step *= 5
			exact := uint64(len(unique))
			res := axiom.Estimate()
			ratio := 100 * estimateError(res, exact)
			data2, err := axiom.MarshalBinary()
			if err != nil {
				panic(err)
			}
			fmt.Printf("Exact %d, got:\n\t axiom HLL %d (%.4f%% off) total %d\n", exact, res, ratio, len(data2))
		}
	}

	data2, err := axiom.MarshalBinary()
	if err != nil {
		panic(err)
	}
	fmt.Println("AxiomHQ HLL total size:\t", len(data2))
}
