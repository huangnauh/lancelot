package command

import (
	"math"
	"strconv"

	"github.com/mmcloughlin/geohash"
	"gitlab.s.upyun.com/platform/lancelot/geo"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
)

// GEORADIUS key longitude latitude radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func (c *Command) GeoRadiusHandle(txn *store.Txn, args [][]byte) interface{} {
	return nil
}

// GEOHASH key member [member ...]
func (c *Command) GeoHashHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(GEOHASH_COMMAND))
	}
	count := len(args) - 1
	rets := make([]interface{}, count)
	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return rets
	} else if err != nil {
		return txn.SetError(err)
	}

	for i := 1; i < len(args); i++ {
		zkey := object.GetValueBytes(EncodeMemberKey(args[i]))
		v, err := txn.Get(zkey)
		if err == store.KeyNotFound {
			rets[i-1] = nil
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		if v == nil {
			rets[i-1] = nil
			continue
		}
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return txn.SetError(err)
		}
		score := uint64(utils.DecodeFloat(zvalue.Value))
		lng, lat := geo.DecodeGeohash(score)
		rets[i-1] = geohash.EncodeWithPrecision(lat, lng, 11)
	}
	return rets
}

// GEOPOS key member [member ...]
func (c *Command) GeoPosHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetError(xerror.WrongArgsError(GEOPOS_COMMAND))
	}
	count := len(args) - 1
	rets := make([]interface{}, count)
	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return rets
	} else if err != nil {
		return txn.SetError(err)
	}

	for i := 1; i < len(args); i++ {
		zkey := object.GetValueBytes(EncodeMemberKey(args[i]))
		v, err := txn.Get(zkey)
		if err == store.KeyNotFound {
			rets[i-1] = nil
			continue
		} else if err != nil {
			return txn.SetError(err)
		}
		if v == nil {
			rets[i-1] = nil
			continue
		}
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return txn.SetError(err)
		}
		score := uint64(utils.DecodeFloat(zvalue.Value))
		lng, lat := geo.DecodeGeohash(score)
		rets[i-1] = []float64{lng, lat}
	}
	return rets
}

// GEODIST key member1 member2 [M|KM|FT|MI]
func (c *Command) GeoDistHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 3 {
		return txn.SetError(xerror.WrongArgsError(GEODIST_COMMAND))
	}
	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	scores := [2]uint64{}
	for i := 0; i < 2; i++ {
		zkey := object.GetValueBytes(EncodeMemberKey(args[i+1]))
		v, err := txn.Get(zkey)
		if err == store.KeyNotFound {
			return nil
		} else if err != nil {
			return txn.SetError(err)
		}
		if v == nil {
			return nil
		}
		zvalue := &Value{}
		err = DecodeValue(v, zvalue)
		if err != nil {
			return txn.SetError(err)
		}
		scores[i] = uint64(utils.DecodeFloat(zvalue.Value))
	}
	flng, flat := geo.DecodeGeohash(scores[0])
	tlng, tlat := geo.DecodeGeohash(scores[1])
	dist := geo.Distance(flat, flng, tlat, tlng)
	return dist
}

// GEOADD key [NX|XX] [CH] longitude latitude member [longitude latitude member ...]
func (c *Command) GeoAddHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetError(xerror.WrongArgsError(GEOADD_COMMAND))
	}
	opt, i, err := getCheckOption(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	if opt.Incr {
		return txn.SetError(xerror.ErrSyntax)
	}
	i = i + 1
	if len(args) < i+3 || (len(args)-i-3)%3 != 0 {
		return txn.SetError(xerror.ErrSyntax)
	}
	members := make(map[string]float64)
	for ; i < len(args); i += 3 {
		longitude, err := strconv.ParseFloat(utils.B2S(args[i]), 64)
		if err != nil || math.IsNaN(longitude) {
			return txn.SetError(xerror.ErrInvalidFloat)
		}
		latitude, err := strconv.ParseFloat(utils.B2S(args[i+1]), 64)
		if err != nil || math.IsNaN(longitude) {
			return txn.SetError(xerror.ErrInvalidFloat)
		}
		if latitude < -geo.ENC_LAT ||
			latitude > geo.ENC_LAT ||
			longitude < -geo.ENC_LONG ||
			longitude > geo.ENC_LONG {
			return txn.SetError(xerror.InvalidGEOError(latitude, longitude))
		}
		members[utils.B2S(args[i+2])] = float64(geo.EncodeGeohash(longitude, latitude))
	}
	return c.zaddMembers(txn, args[0], GeoType, members, opt)
}
