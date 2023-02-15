// This script is made to benchmark the performance of ClickHouse
// It will insert 10000 packets per second and downsampling the raw
// data to 15 minutes resolution for an hour.
// Before running the script, make sure you have setup the ClickHouse
// database and table.
/*
DROP TABLE raw;
DROP TABLE resolution_15_min;

CREATE TABLE raw
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL
);
CREATE TABLE resolution_15_min
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL,
	UNIQUE (timestamp, asset_id, attribute_id, measurement)
);

SELECT create_hypertable(
  'raw',
  'timestamp'
);

SELECT create_hypertable(
  'resolution_15_min',
  'timestamp'
);
*/

package main

import (
	"context"
	"log"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
)

func Insert(conn *pgxpool.Pool, packets []shared.Packet) error {
	rows := [][]interface{}{}
	for _, packet := range packets {
		rows = append(rows, []interface{}{packet.Timestamp, packet.AssetID, packet.AttributeID, packet.Measurement, packet.Value})
	}

	_, err := conn.CopyFrom(
		context.Background(),
		pgx.Identifier{"raw"},
		[]string{"timestamp", "asset_id", "attribute_id", "measurement", "value"},
		pgx.CopyFromRows(rows),
	)

	return err
}

func DownSampling(conn *pgxpool.Pool, start time.Time, end time.Time) (err error) {
	_, err = conn.Exec(context.Background(), `
		INSERT INTO resolution_15_min (timestamp, asset_id, attribute_id, measurement, value) 
			SELECT 
				time_bucket('15min', timestamp) as timestamp, 
				asset_id, 
				attribute_id, 
				'mean' as measurement, 
				avg(value) as value
			FROM raw 
			WHERE timestamp >= $1
				AND timestamp <= $2
			GROUP BY 
				time_bucket('15min', timestamp), 
				asset_id, 
				attribute_id
			ORDER BY timestamp
		ON CONFLICT (timestamp, asset_id, attribute_id, measurement) DO UPDATE
			SET value = excluded.value;
	`, start, end)

	return
}

func main() {
	connStr := "<connection>"
	filename := "timescale-<>vCPU-latency.csv"
	config, _ := pgxpool.ParseConfig(connStr)
	config.MaxConns = 30
	pgxPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalln(err)
	}
	defer pgxPool.Close()

	recorder := shared.NewQueryLatencyRecorder(filename)
	shared.RunTest(
		recorder,
		"timescale",
		func(packets []shared.Packet) error {
			return Insert(pgxPool, packets)
		},
		func(start, end time.Time) error {
			return DownSampling(pgxPool, start, end)
		},
	)
}
