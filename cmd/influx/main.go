package main

import (
	"context"
	"fmt"
	"time"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/api/write"
	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
)

func Insert(conn influxdb2.Client, packets []shared.Packet) error {
	points := make([]*write.Point, 0)
	for _, packet := range packets {
		points = append(points, influxdb2.NewPoint(
			"_raw",
			map[string]string{
				"asset_id":     fmt.Sprintf("%d", packet.AssetID),
				"attribute_id": fmt.Sprintf("%d", packet.AttributeID),
			},
			map[string]interface{}{
				"value": packet.Value,
			},
			packet.Timestamp,
		))
	}

	writeAPI := conn.WriteAPIBlocking("resync", "raw")
	err := writeAPI.WritePoint(context.Background(), points...)
	return err
}

func main() {
	url := "https://<host>:<port>"
	token := "<username>:<password>"
	filename := "influx-<>vCPU-latency.csv"

	client := influxdb2.NewClientWithOptions(
		url,
		token,
		influxdb2.DefaultOptions().SetBatchSize(100000),
	)

	defer client.Close()

	recorder := shared.NewQueryLatencyRecorder(filename)
	defer recorder.Close()

	shared.RunTest(
		recorder,
		"influx",
		func(packets []shared.Packet) error {
			return Insert(client, packets)
		},
		func(start, end time.Time) error {
			return nil
		},
	)
}
