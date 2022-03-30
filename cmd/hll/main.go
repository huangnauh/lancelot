package main

import (
	"fmt"
	"strconv"

	"github.com/axiomhq/hyperloglog"
	"gitlab.s.upyun.com/platform/lancelot/hll"
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

type DefaultDense []uint8

func NewDefaultDense(m uint32) DefaultDense {
	return make(DefaultDense, m)
}

func (d DefaultDense) CheckAndSet(i uint64, rho uint8) (uint8, bool, error) {
	fmt.Printf("CheckAndSet %d %d\n", i, rho)
	origin := d[i]
	set := rho > origin
	if set {
		d[i] = rho
	}
	return origin, set, nil
}

func (d DefaultDense) Get(i uint64) (uint8, error) {
	fmt.Printf("Get %d\n", i)
	rho := d[i]
	return rho, nil
}

func (d DefaultDense) Set(i uint64, rho uint8) error {
	fmt.Printf("Set %d %d\n", i, rho)
	d[i] = rho
	return nil
}

func (d DefaultDense) List() ([]uint8, error) {
	return d, nil
}

func main() {
	dense := NewDefaultDense(1 << 16)
	h, err := hll.NewPlus(16, dense)
	if err != nil {
		panic(err)
	}
	h.Add([]byte("foo1"))
}

func main1() {
	axiom := hyperloglog.New16()
	influx, err := hll.NewPlus(16, nil)
	if err != nil {
		panic(err)
	}

	step := 10
	unique := map[string]bool{}

	for i := 1; len(unique) <= 10000000; i++ {
		str := "stream-" + strconv.Itoa(i)
		axiom.Insert([]byte(str))
		influx.Add([]byte(str))
		unique[str] = true

		if len(unique)%step == 0 || len(unique) == 10000000 {
			step *= 5
			exact := uint64(len(unique))
			res := axiom.Estimate()
			ratio := 100 * estimateError(res, exact)
			res2, err := influx.Count()
			if err != nil {
				panic(err)
			}
			ratio2 := 100 * estimateError(res2, exact)
			fmt.Printf("Exact %d, got:\n\t axiom HLL %d (%.4f%% off)\n\tinflux HLL++ %d (%.4f%% off)\n", exact, res, ratio, res2, ratio2)
			// data1, err := influx.MarshalBinary()
			// if err != nil {
			// 	panic(err)
			// }
			// data2, err := axiom.MarshalBinary()
			// if err != nil {
			// 	panic(err)
			// }
			// fmt.Println("AxiomHQ HLL total size:\t", len(data2))
			// fmt.Println("InfluxData HLL++ total size:\t", len(data1))
		}
	}

}
