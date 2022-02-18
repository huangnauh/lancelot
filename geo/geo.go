package geo

import (
	"math"
)

const (
	REDIS_GEO_MAX          = 52
	EARTH_RADIUS_IN_METERS = 6372797.560856
)

func EncodeGeohash(lng, lat float64) uint64 {
	return EncodeIntWithPrecision(lat, lng, REDIS_GEO_MAX)
}

func DecodeGeohash(score uint64) (float64, float64) {
	lat, lng := DecodeIntWithPrecision(score, REDIS_GEO_MAX)
	return lng, lat
}

// haversin(θ) function
func hsin(theta float64) float64 {
	return math.Pow(math.Sin(theta/2), 2)
}

// Distance function returns the distance (in meters) between two points of
//     a given longitude and latitude relatively accurately (using a spherical
//     approximation of the Earth) through the Haversin Distance Formula for
//     great arc distance on a sphere with accuracy for small distances
//
// point coordinates are supplied in degrees and converted into rad. in the func
//
// distance returned is METERS!!!!!!
// http://en.wikipedia.org/wiki/Haversine_formula
func Distance(lat1, lon1, lat2, lon2 float64) float64 {
	// convert to radians
	// must cast radius as float to multiply later
	var la1, lo1, la2, lo2 float64
	la1 = lat1 * math.Pi / 180
	lo1 = lon1 * math.Pi / 180
	la2 = lat2 * math.Pi / 180
	lo2 = lon2 * math.Pi / 180

	// calculate
	h := hsin(la2-la1) + math.Cos(la1)*math.Cos(la2)*hsin(lo2-lo1)

	return 2 * EARTH_RADIUS_IN_METERS * math.Asin(math.Sqrt(h))
}
