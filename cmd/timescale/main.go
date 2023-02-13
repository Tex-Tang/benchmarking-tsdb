// This script is made to benchmark the performance of ClickHouse
// It will insert 10000 packets per second and downsampling the raw
// data to 15 minutes resolution for an hour.
// Before running the script, make sure you have setup the ClickHouse
// database and table.
/*
DROP TABLE raw;
CREATE TABLE raw
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
  'raw',
  'timestamp'
);

DROP TABLE resolution_1_min;
CREATE TABLE resolution_1_min
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL
);
SELECT create_hypertable(
  'resolution_1_min',
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
			time_bucket('15min', timestamp), 
			asset_id, 
			attribute_id, 
			'mean' as measurement, 
			avg(value) 
		FROM raw 
		WHERE timestamp >= $1
			AND timestamp <= $2
		GROUP BY timestamp, asset_id, attribute_id
		ORDER BY timestamp;
	`, start, end)

	return
}

// connect to database using a single connection
func main() {
	connStr := "postgres://tsdbadmin:AVNS_opNgcCQ9mwcBpaOc_Ls@tsdb-31b2e3e8-resynctech-5265.a.timescaledb.io:25590/resync?sslmode=require"
	config, _ := pgxpool.ParseConfig(connStr)
	config.MaxConns = 30
	pgxPool, err := pgxpool.NewWithConfig(context.Background(), config)
	if err != nil {
		log.Fatalln(err)
	}
	defer pgxPool.Close()

	recorder := shared.NewQueryLatencyRecorder("timescale-latency.csv")
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

/*
DROP TABLE raw;
CREATE TABLE raw
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL
);

SELECT create_hypertable(
  'raw',
  'timestamp'
);

DROP TABLE resolution_1_min;
CREATE TABLE resolution_1_min
(
	"timestamp"     TIMESTAMPTZ			 NOT NULL,
	"asset_id"  	  BIGINT 					 NOT NULL,
	"attribute_id"  BIGINT 					 NOT NULL,
	"measurement"   TEXT        		 NOT NULL,
	"value"  				DOUBLE PRECISION NOT NULL
);
SELECT create_hypertable(
  'resolution_1_min',
  'timestamp'
);

		INSERT INTO resolution_1_min (timestamp, asset_id, attribute_id, measurement, value)
		SELECT
			time_bucket('1min', timestamp),
			asset_id,
			attribute_id,
			'mean' as measurement,
			avg(value)
		FROM raw
		WHERE timestamp >= '2023-02-09 06:54:00'
			AND timestamp <= '2023-02-09 06:56:00'
		GROUP BY timestamp, asset_id, attribute_id
		ORDER BY timestamp;


INSERT INTO resolution_1_min (timestamp, asset_id, attribute_id, measurement, value)
	SELECT
		toStartOfInterval(raw.timestamp, INTERVAL 1 MINUTE) AS timestamp,
		asset_id,
		attribute_id,
		'mean' AS measurement,
		avg(raw.value) AS value
	FROM raw
	WHERE timestamp >= {start:DateTime}
		AND timestamp <= {end:DateTime}
	GROUP BY timestamp, asset_id, attribute_id
	ORDER BY timestamp;


CREATE OR REPLACE PROCEDURE downsample_compress (job_id int, config jsonb)
LANGUAGE PLPGSQL
AS $$
DECLARE
  lag interval;
  chunk REGCLASS;
  tmp_name name;
BEGIN
  SELECT jsonb_object_field_text (config, 'lag')::interval INTO STRICT lag;

  IF lag IS NULL THEN
    RAISE EXCEPTION 'Config must have lag';
  END IF;

  FOR chunk IN
    SELECT show.oid
    FROM show_chunks('raw', older_than => lag) SHOW (oid)
      INNER JOIN pg_class pgc ON pgc.oid = show.oid
      INNER JOIN pg_namespace pgns ON pgc.relnamespace = pgns.oid
      INNER JOIN timescaledb_information.chunks chunk ON chunk.chunk_name = pgc.relname
        AND chunk.chunk_schema = pgns.nspname
    WHERE chunk.is_compressed::bool = FALSE
  LOOP
    RAISE NOTICE 'Processing chunk: %', chunk::text;

    -- build name for temp table
    SELECT '_tmp' || relname
    FROM pg_class
    WHERE oid = chunk INTO STRICT tmp_name;

    -- copy downsampled chunk data into temp table
    EXECUTE format($sql$ CREATE UNLOGGED TABLE %I AS
      SELECT time_bucket('1h', time), device_id, avg(value) FROM %s GROUP BY 1, 2;
    $sql$, tmp_name, chunk);

    -- clear original chunk
    EXECUTE format('TRUNCATE %s;', chunk);

    -- copy downsampled data back into chunk
    EXECUTE format('INSERT INTO %s(time, device_id, value) SELECT * FROM %I;', chunk, tmp_name);

    -- drop temp table
    EXECUTE format('DROP TABLE %I;', tmp_name);

    PERFORM compress_chunk (chunk);

    COMMIT;
  END LOOP;
END
$$;

INSERT INTO resolution_1_min (timestamp, asset_id, attribute_id, measurement, value)
SELECT
	time_bucket('1min', timestamp),
	asset_id,
	attribute_id,
	'mean' as measurement,
	avg(value)
FROM raw
GROUP BY timestamp, asset_id, attribute_id;
*/
