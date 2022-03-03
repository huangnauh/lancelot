package geo

import (
	"math"

	"github.com/golang/geo/s1"
	"github.com/golang/geo/s2"
	"github.com/mmcloughlin/geohash"
	"gitlab.s.upyun.com/platform/lancelot/utils"
	"go.uber.org/zap"
)

const (
	ENC_LAT                = 90.0 //85.05112878
	ENC_LONG               = 180.0
	REDIS_GEO_MAX          = 64
	EARTH_RADIUS_IN_METERS = 6372797.560856
)

type Location struct {
	Lat float64
	Lng float64
}

func (l Location) EncodeGeohashString() string {
	return geohash.EncodeWithPrecision(l.Lat, l.Lng, 11)
}

func (l Location) EncodeGeohash(bits uint) uint64 {
	return geohash.EncodeIntWithPrecision(l.Lat, l.Lng, bits)
}

func DecodeGeohash(score uint64, bits uint) Location {
	lat, lng := geohash.DecodeIntWithPrecision(score, bits)
	return Location{Lat: lat, Lng: lng}
}

func (l Location) ToLatLng() s2.LatLng {
	return s2.LatLngFromDegrees(l.Lat, l.Lng)
}

func (l Location) Distance(other Location) float64 {
	return l.ToLatLng().Distance(other.ToLatLng()).Radians() * EARTH_RADIUS_IN_METERS
}

func (l Location) RegionContains(region s2.Region) bool {
	if r, ok := region.(s2.Cap); ok {
		return r.ContainsPoint(s2.PointFromLatLng(l.ToLatLng()))
	}
	if r, ok := region.(s2.Rect); ok {
		ll := l.ToLatLng()
		utils.ZapLog.Debug("rect contains",
			zap.Float64("lat lo", r.Lat.Lo), zap.Float64("lat hi", r.Lat.Hi),
			zap.Float64("lng lo", r.Lng.Lo), zap.Float64("lng hi", r.Lng.Hi),
			zap.Float64("lat", ll.Lat.Radians()), zap.Float64("lng", ll.Lng.Radians()))
		return r.ContainsLatLng(ll)
	}
	return false
}

func (l Location) EncodeCellID() uint64 {
	return uint64(s2.CellIDFromLatLng(l.ToLatLng()))
}

func DecodeCellID(score uint64) Location {
	cellID := s2.CellID(score)
	latLng := cellID.LatLng()
	return Location{Lat: latLng.Lat.Degrees(), Lng: latLng.Lng.Degrees()}
}

type Range struct {
	Min uint64
	Max uint64
}

func (l Location) RectRegion(width, height float64) s2.Region {
	lat_delta := width / 2 / EARTH_RADIUS_IN_METERS / float64(s1.Degree)
	if l.Lat < 0 {
		lng_bottom := height / 2 / EARTH_RADIUS_IN_METERS / float64(s1.Degree) / math.Cos((s1.Angle(l.Lat-lat_delta) * s1.Degree).Radians())
		utils.ZapLog.Debug("rect region", zap.Float64("width", width), zap.Float64("height", height),
			zap.Float64("lat_delta", lat_delta), zap.Float64("lng_delta", lng_bottom))
		return s2.RectFromLatLng(
			s2.LatLngFromDegrees(l.Lat-lat_delta, l.Lng-lng_bottom)).AddPoint(
			s2.LatLngFromDegrees(l.Lat+lat_delta, l.Lng+lng_bottom))
	} else {
		lng_top := height / 2 / EARTH_RADIUS_IN_METERS / float64(s1.Degree) / math.Cos((s1.Angle(l.Lat+lat_delta) * s1.Degree).Radians())
		utils.ZapLog.Debug("rect region", zap.Float64("width", width), zap.Float64("height", height),
			zap.Float64("lat_delta", lat_delta), zap.Float64("lng_delta", lng_top))
		return s2.RectFromLatLng(
			s2.LatLngFromDegrees(l.Lat-lat_delta, l.Lng-lng_top)).AddPoint(
			s2.LatLngFromDegrees(l.Lat+lat_delta, l.Lng+lng_top))
	}
}

func (l Location) CapRegion(meters float64) s2.Region {
	return s2.CapFromCenterAngle(s2.PointFromLatLng(l.ToLatLng()),
		s1.Angle(meters/EARTH_RADIUS_IN_METERS))
}

func RegionRange(region s2.Region) []Range {
	bound := region.CellUnionBound()
	ranges := make([]Range, len(bound))
	for i, cellID := range bound {
		ranges[i] = Range{
			Min: uint64(cellID.RangeMin()),
			Max: uint64(cellID.RangeMax()),
		}
	}
	return ranges
}
