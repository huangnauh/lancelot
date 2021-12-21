package json

import (
	"time"
	"unsafe"

	jsoniter "github.com/json-iterator/go"
)

var (
	json = jsoniter.ConfigCompatibleWithStandardLibrary
	// Marshal is exported by gin/json package.
	Marshal = json.Marshal
	// Unmarshal is exported by gin/json package.
	Unmarshal = json.Unmarshal
	// MarshalIndent is exported by gin/json package.
	MarshalIndent = json.MarshalIndent
	// NewDecoder is exported by gin/json package.
	NewDecoder = json.NewDecoder
	// NewEncoder is exported by gin/json package.
	NewEncoder = json.NewEncoder
	Valid      = json.Valid
)

func init() {
	jsoniter.RegisterTypeEncoder("time.Duration", &durationAsStringCodec{})
	jsoniter.RegisterTypeDecoder("time.Duration", &durationAsStringCodec{})
}

type durationAsStringCodec struct {
}

func (codec *durationAsStringCodec) Decode(ptr unsafe.Pointer, iter *jsoniter.Iterator) {
	dur := iter.ReadString()
	d, err := time.ParseDuration(dur)
	if err != nil {
		iter.ReportError("time.Duration", err.Error())
		return
	}
	*((*time.Duration)(ptr)) = d
}

func (codec *durationAsStringCodec) IsEmpty(ptr unsafe.Pointer) bool {
	ts := *((*time.Duration)(ptr))
	return ts == 0
}

func (codec *durationAsStringCodec) Encode(ptr unsafe.Pointer, stream *jsoniter.Stream) {
	ts := *((*time.Duration)(ptr))
	stream.WriteString(ts.String())
}
