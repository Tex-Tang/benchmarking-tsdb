package shared

import (
	"math/rand"
	"time"
)

type Packet struct {
	Timestamp   time.Time
	AssetID     uint32
	AttributeID uint32
	Measurement string
	Value       float64
}

func GeneratePackets(timestamp time.Time, devicesCount int, attributePerDevice int) []Packet {
	packets := []Packet{}
	for i := 1; i <= devicesCount; i++ {
		for j := 1; j <= attributePerDevice; j++ {
			packets = append(packets, Packet{
				Timestamp:   timestamp,
				AssetID:     uint32(i),
				AttributeID: uint32(j),
				Measurement: "raw",
				Value:       rand.Float64() * 100,
			})
		}
	}
	return packets
}
