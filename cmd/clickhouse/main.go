package main

import (
	"context"
	"log"
	"os"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
)

func insert(conn clickhouse.Conn, packets []shared.Packet) error {
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

func downSampling(conn clickhouse.Conn, start time.Time, end time.Time) (err error) {
	err = conn.Exec(context.Background(), `
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
			"benchmarking-clickhouse.asia-southeast1-b.c.resync-cloud-dev.internal:9000",
		},
		Auth: clickhouse.Auth{
			Database: "default",
			Username: "default",
		},
	})

	f, err := os.OpenFile("perf.csv", os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0600)
	if err != nil {
		panic(err)
	}
	fmtx := &sync.Mutex{}

	defer f.Close()

	ticker1s := time.NewTicker(1 * time.Second)
	ticker2min := time.NewTicker(2 * time.Minute)
	exitTimer := time.NewTimer(30 * time.Minute)

	go func() {
		for {
			select {
			case <-ticker1s.C:
				go func() {
					packets := shared.GeneratePackets(time.Now(), 15000, 10)
					performanceStart := time.Now()
					err = insert(conn, packets)
					text := performanceStart.Format(time.RFC3339) + ",clickhouse.insert," + time.Since(performanceStart).String() + "\n"
					log.Print(text)
					if err != nil {
						panic(err)
					}
					fmtx.Lock()
					if _, err = f.WriteString(text); err != nil {
						panic(err)
					}
					fmtx.Unlock()
				}()
			case <-exitTimer.C:
				return
			}
		}
	}()

	for {
		select {
		case <-ticker2min.C:
			performanceStart := time.Now()
			err = downSampling(conn, time.Now().Add(-2*time.Minute), time.Now())
			text := time.Now().Format(time.RFC3339) + ",clickhouse.down-sampling," + time.Since(performanceStart).String() + "\n"
			log.Print(text)
			if err != nil {
				panic(err)
			}
			fmtx.Lock()
			if _, err = f.WriteString(text); err != nil {
				panic(err)
			}
			fmtx.Unlock()
		case <-exitTimer.C:
			return
		}
	}
}

/*
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

2023-02-08T14:42:46Z,clickhouse.insert,151.082527ms
*/
