package command

import (
	"encoding/binary"
	"math"
	"sort"
	"strconv"
	"strings"

	"github.com/golang/geo/s2"
	"gitlab.s.upyun.com/platform/lancelot/geo"
	"gitlab.s.upyun.com/platform/lancelot/redcon"
	"gitlab.s.upyun.com/platform/lancelot/store"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"gitlab.s.upyun.com/platform/lancelot/xerror"
	"go.uber.org/zap"
)

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
		score := binary.BigEndian.Uint64(zvalue.Value)
		location := geo.DecodeCellID(score)
		rets[i-1] = location.EncodeGeohashString()
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
		score := binary.BigEndian.Uint64(zvalue.Value)
		location := geo.DecodeCellID(score)
		rets[i-1] = []float64{location.Lng, location.Lat}
	}
	return rets
}

func toMeter(arg []byte) float64 {
	switch strings.ToLower(string(arg)) {
	case "km":
		return 1000
	case "ft":
		return 0.3048
	case "mi":
		return 1609.344
	default:
		return 1
	}
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
		scores[i] = binary.BigEndian.Uint64(zvalue.Value)
	}
	fl := geo.DecodeCellID(scores[0])
	tl := geo.DecodeCellID(scores[1])
	dist := fl.Distance(tl)

	if len(args) > 3 {
		dist /= toMeter(args[3])
	}
	return dist
}

func checkGeoLocation(args [][]byte) (geo.Location, error) {
	l := geo.Location{}
	longitude, err := strconv.ParseFloat(utils.B2S(args[0]), 64)
	if err != nil || math.IsNaN(longitude) {
		return l, xerror.ErrInvalidFloat
	}
	latitude, err := strconv.ParseFloat(utils.B2S(args[1]), 64)
	if err != nil || math.IsNaN(longitude) {
		return l, xerror.ErrInvalidFloat
	}
	if latitude < -geo.ENC_LAT ||
		latitude > geo.ENC_LAT ||
		longitude < -geo.ENC_LONG ||
		longitude > geo.ENC_LONG {
		return l, xerror.InvalidGEOError(latitude, longitude)
	}
	l.Lat = latitude
	l.Lng = longitude
	return l, nil
}

// GEOREM key member [member ...]
func (c *Command) GeoRemHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 2 {
		return txn.SetWrongArgs(GEOREM_COMMAND)
	}
	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err := getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return redcon.SimpleInt(0)
	} else if err != nil {
		return txn.SetError(err)
	}

	count := 0
	for i := 1; i < len(args); i++ {
		ok, err := c.zrem(txn, object, args[i])
		if err != nil {
			return txn.SetError(err)
		}
		if ok {
			count++
		}
	}
	return redcon.SimpleInt(count)
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
	members := make(map[string]uint64)
	for ; i < len(args); i += 3 {
		l, err := checkGeoLocation(args[i:])
		if err != nil {
			return txn.SetError(err)
		}
		members[utils.B2S(args[i+2])] = l.EncodeCellID()
	}
	return c.uzaddMembers(txn, args[0], GeoType, members, opt)
}

const (
	UnSorted = 0
	Asc      = 1
	Desc     = 2
)

type ByRadius struct {
	Radius float64
}

type ByBox struct {
	Width  float64
	Height float64
}

type geoOption struct {
	Coord        bool
	Dist         bool
	Hash         bool
	CellID       bool
	FromMember   bool
	FromLonLat   bool
	Count        int
	Direction    int
	Center       geo.Location
	Unit         float64
	ByRadius     *ByRadius
	ByBox        *ByBox
	StoreKey     []byte
	StoreDistKey []byte
}

func checkGeoOption(args [][]byte, key []byte) (*geoOption, []byte, error) {
	opt := &geoOption{}
	var member []byte
	for i := 0; i < len(args); i++ {
		switch strings.ToLower(string(args[i])) {
		case "frommember":
			if opt.FromLonLat || i+1 >= len(args) {
				return nil, member, xerror.ErrSyntax
			}
			opt.FromMember = true
			member = args[i+1]
			i++
		case "fromlonlat":
			if opt.FromMember || i+2 >= len(args) {
				return nil, member, xerror.ErrSyntax
			}
			opt.FromLonLat = true
			l, err := checkGeoLocation(args[i+1:])
			if err != nil {
				return nil, member, err
			}
			opt.Center = l
			i += 2
		case "byradius":
			if opt.ByBox != nil || i+2 >= len(args) {
				return nil, nil, xerror.ErrSyntax
			}
			radius, err := strconv.ParseFloat(utils.B2S(args[i+1]), 64)
			if err != nil || math.IsNaN(radius) || math.IsInf(radius, 0) {
				return nil, member, xerror.ErrInvalidFloat
			}
			opt.Unit = toMeter(args[i+2])
			opt.ByRadius = &ByRadius{
				Radius: radius,
			}
			i += 2
		case "bybox":
			if false {
				return nil, nil, xerror.ErrNotSupport
			}
			if opt.ByRadius != nil || i+3 >= len(args) {
				return nil, member, xerror.ErrSyntax
			}
			width, err := strconv.ParseFloat(utils.B2S(args[i+1]), 64)
			if err != nil || math.IsNaN(width) || math.IsInf(width, 0) {
				return nil, member, xerror.ErrInvalidFloat
			}
			height, err := strconv.ParseFloat(utils.B2S(args[i+2]), 64)
			if err != nil || math.IsNaN(height) || math.IsInf(height, 0) {
				return nil, member, xerror.ErrInvalidFloat
			}
			opt.Unit = toMeter(args[i+3])
			opt.ByBox = &ByBox{
				Width:  width,
				Height: height,
			}
			i += 3
		case "withcoord":
			opt.Coord = true
		case "withdist":
			opt.Dist = true
		case "withcell":
			opt.CellID = true
		case "withhash":
			opt.Hash = true
		case "count":
			if i+1 >= len(args) {
				return nil, member, xerror.ErrSyntax
			}
			count, err := strconv.Atoi(utils.B2S(args[i+1]))
			if err != nil {
				return nil, member, xerror.ErrNotInteger
			}

			if count <= 0 {
				return nil, member, xerror.ErrCountNegative
			}
			i++
			opt.Count = count
		case "asc":
			opt.Direction = Asc
		case "desc":
			opt.Direction = Desc
		case "store":
			if i+1 >= len(args) {
				return nil, member, xerror.ErrSyntax
			}
			opt.StoreKey = args[i+1]
			i++
		case "storedist":
			if key != nil {
				opt.StoreDistKey = key
			} else {
				if i+1 >= len(args) {
					return nil, member, xerror.ErrSyntax
				}
				opt.StoreDistKey = args[i+1]
				i++
			}
		default:
			return nil, member, xerror.ErrSyntax
		}
	}
	if opt.StoreDistKey == nil && key != nil {
		opt.StoreKey = key
	}
	if (opt.StoreKey != nil || opt.StoreDistKey != nil) && (opt.Coord || opt.Dist || opt.Hash) {
		return nil, member, xerror.ErrStoreOption
	}
	return opt, member, nil
}

type GeoInfo struct {
	geo.Location
	Member   string
	Distance float64
	CellID   uint64
}

// GEORADIUSBYMEMBER key member radius M|KM|FT|MI [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func (c *Command) GeoRadiusByMemberHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 4 {
		return txn.SetError(xerror.WrongArgsError(GEORADIUSBYMEMBER_COMMAND))
	}
	radius, err := strconv.ParseFloat(utils.B2S(args[2]), 64)
	if err != nil || math.IsNaN(radius) || math.IsInf(radius, 0) {
		return txn.SetError(xerror.ErrInvalidFloat)
	}
	unit := toMeter(args[3])
	opt, _, err := checkGeoOption(args[4:], nil)
	if err != nil {
		return txn.SetError(err)
	}
	opt.Unit = unit
	object, zvalue, err := c.zget(txn, GeoType, args[0], args[1])
	if err != nil {
		return txn.SetError(err)
	}
	if object == nil || zvalue == nil {
		return nil
	}
	score := binary.BigEndian.Uint64(zvalue.Value)
	center := geo.DecodeCellID(score)
	opt.ByRadius = &ByRadius{
		Radius: radius,
	}
	opt.Center = center
	return c.geoSearchHandle(txn, object, opt, args)
}

// GEORADIUS key longitude latitude radius M|KM|FT|MI
// [WITHCOORD] [WITHDIST] [WITHHASH] [COUNT count [ANY]] [ASC|DESC] [STORE key] [STOREDIST key]
func (c *Command) GeoRadiusHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 5 {
		return txn.SetError(xerror.WrongArgsError(GEORADIUS_COMMAND))
	}

	location, err := checkGeoLocation(args[1:])
	if err != nil {
		return txn.SetError(err)
	}
	radius, err := strconv.ParseFloat(utils.B2S(args[3]), 64)
	if err != nil || math.IsNaN(radius) || math.IsInf(radius, 0) {
		return txn.SetError(xerror.ErrInvalidFloat)
	}
	unit := toMeter(args[4])
	opt, _, err := checkGeoOption(args[5:], nil)
	if err != nil {
		return txn.SetError(err)
	}
	opt.Unit = unit
	opt.Center = location
	opt.ByRadius = &ByRadius{
		Radius: radius,
	}

	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	return c.geoSearchHandle(txn, object, opt, args)
}

// GEOSEARCH key [FROMMEMBER member] [FROMLONLAT longitude latitude] [BYRADIUS radius M|KM|FT|MI] [BYBOX width height M|KM|FT|MI] [ASC|DESC] [COUNT count [ANY]] [WITHCOORD] [WITHDIST] [WITHHASH]
func (c *Command) GeoSearchHandle(txn *store.Txn, args [][]byte) interface{} {
	if len(args) < 1 {
		return txn.SetError(xerror.WrongArgsError(GEOSEARCH_COMMAND))
	}
	opt, member, err := checkGeoOption(args[1:], nil)
	if err != nil {
		return txn.SetError(err)
	}
	object := c.NewObject(txn, GeoType, args[0])
	key := object.GetKeyBytes()
	err = getTxnObject(txn, key, object, false)
	if err == store.KeyNotFound {
		return nil
	} else if err != nil {
		return txn.SetError(err)
	}
	if member != nil {
		zvalue, err := c.zgetMember(txn, object, member)
		if err != nil {
			return txn.SetError(err)
		}
		if zvalue == nil {
			return nil
		}
		score := binary.BigEndian.Uint64(zvalue.Value)
		center := geo.DecodeCellID(score)
		opt.Center = center
	}
	if !opt.FromLonLat && !opt.FromMember {
		return txn.SetError(xerror.ErrSyntax)
	}

	if opt.ByBox == nil && opt.ByRadius == nil {
		return txn.SetError(xerror.ErrSyntax)
	}

	return c.geoSearchHandle(txn, object, opt, args)
}

func (c *Command) geoSearchHandle(txn *store.Txn, object *Object, opt *geoOption, args [][]byte) interface{} {
	positions := make([]GeoInfo, 0)
	var region s2.Region
	if opt.ByRadius != nil {
		region = opt.Center.CapRegion(opt.ByRadius.Radius * opt.Unit)
	} else if opt.ByBox != nil {
		region = opt.Center.RectRegion(opt.ByBox.Width*opt.Unit, opt.ByBox.Height*opt.Unit)
	} else {
		return txn.SetError(xerror.ErrSyntax)
	}
	ranges := geo.RegionRange(region)
	for _, r := range ranges {
		utils.ZapLog.Debug("region", zap.Uint64("range-min", r.Min), zap.Uint64("range-max", r.Max))
		ret, err := c.objectZRangeByUint64Score(txn, object, args[0], r.Min, r.Max, true, true,
			&zRangeOption{
				limit:      int64(txn.Config.Redis.ScanMaxCount),
				withScores: true,
			})
		if err != nil {
			return txn.SetError(err)
		}
		for i := 0; i < len(ret); i += 2 {
			member := ret[i].([]byte)
			score := ret[i+1].(uint64)
			l := geo.DecodeCellID(score)
			if l.RegionContains(region) {
				positions = append(positions, GeoInfo{
					Location: l,
					Distance: l.Distance(opt.Center),
					CellID:   score,
					Member:   string(member),
				})
			}
		}
	}
	if opt.Direction != UnSorted {
		sort.Slice(positions, func(i, j int) bool {
			if opt.Direction == Asc {
				return positions[i].Distance < positions[j].Distance
			}
			return positions[i].Distance > positions[j].Distance
		})
	}
	ret := make([]interface{}, 0)
	for _, l := range positions {
		ret = append(ret, l.Member)
		if opt.Dist {
			ret = append(ret, l.Distance/opt.Unit)
		}
		if opt.CellID {
			ret = append(ret, l.CellID)
		}
		if opt.Hash {
			ret = append(ret, l.EncodeGeohash(geo.REDIS_GEO_MAX))
		}
		if opt.Coord {
			ret = append(ret, []float64{l.Lng, l.Lat})
		}
	}
	return ret
}
