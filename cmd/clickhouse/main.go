// This script is made to benchmark the performance of ClickHouse
// It will insert 15000 packets per second and downsampling the raw
// data to 15 minutes resolution for an hour.
// Before running the script, make sure you have setup the ClickHouse
// database and table.
/*

Setup the `raw` table
DROP TABLE IF EXISTS raw;
CREATE TABLE raw
(
	"timestamp" DateTime64(3),
	"asset_id" UInt32,
	"attribute_id" UInt32,
	"measurement" String,
	"value" Float64
)
ENGINE = MergeTree
ORDER BY (timestamp, asset_id, attribute_id);

Setup the `resolution_15_min` table
DROP TABLE IF EXISTS resolution_1_min;
CREATE TABLE resolution_1_min
(
	"timestamp" DateTime64(3),
	"asset_id" UInt32,
	"attribute_id" UInt32,
	"measurement" String,
	"value" Float64
)
ENGINE = ReplacingMergeTree
ORDER BY (timestamp, asset_id, attribute_id)
*/
package main

import (
	"context"
	"crypto/tls"
	"log"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
)

func Insert(conn clickhouse.Conn, packets []shared.Packet) error {
	batch, err := conn.PrepareBatch(context.Background(), "INSERT INTO raw")
	if err != nil {
		return err
	}

	for _, packet := range packets {
		if err := batch.Append(
			packet.Timestamp,
			packet.AssetID,
			packet.AttributeID,
			packet.Measurement,
			packet.Value,
		); err != nil {
			panic(err)
		}
	}

	if err := batch.Send(); err != nil {
		return err
	}

	return nil
}

func DownSampling(conn clickhouse.Conn, start time.Time, end time.Time) (err error) {
	err = conn.Exec(context.Background(), `
			INSERT INTO resolution_15_min (timestamp, asset_id, attribute_id, measurement, value)
			SELECT 
				toStartOfInterval(raw.timestamp, INTERVAL 15 MINUTE) AS timestamp,
				asset_id,
				attribute_id,
				'mean' AS measurement,
				avg(raw.value) AS value
			FROM raw
			WHERE timestamp >= {start:DateTime}
			  AND timestamp <= {end:DateTime}
			GROUP BY timestamp, asset_id, attribute_id
			ORDER BY timestamp;
		`,
		clickhouse.Named("start", start.Format(time.DateTime)),
		clickhouse.Named("end", end.Format(time.DateTime)),
	)

	if err != nil {
		return
	}
	return
}

func main() {
	conn, err := clickhouse.Open(&clickhouse.Options{
		Addr: []string{
			"clickhouse-16dfb587-resynctech.aivencloud.com:22546",
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "avnadmin",
			Password: "AVNS_J6A2GgmTdC_cSXcPsEo",
		},
		TLS: &tls.Config{
			InsecureSkipVerify: true,
		},
	})

	if err != nil {
		log.Fatal(err)
	}

	recorder := shared.NewQueryLatencyRecorder("clickhouse-latency.csv")

	shared.RunTest(
		recorder,
		"clickhouse",
		func(packets []shared.Packet) error {
			return Insert(conn, packets)
		},
		func() error {
			return DownSampling(conn, time.Now().Add(-15*time.Minute), time.Now())
		},
	)
}
