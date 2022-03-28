package hll

type Dense interface {
	Get(i uint64) (uint8, error)
	Set(i uint64, rho uint8) error
	List() ([]uint8, error)
}

type DefaultDense []uint8

func NewDefaultDense(m uint32) DefaultDense {
	return make(DefaultDense, m)
}

func (d DefaultDense) Get(i uint64) (uint8, error) {
	rho := d[i]
	return rho, nil
}

func (d DefaultDense) Set(i uint64, rho uint8) error {
	d[i] = rho
	return nil
}

func (d DefaultDense) List() ([]uint8, error) {
	return d, nil
}
