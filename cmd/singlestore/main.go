/*
Setup the database and table.

CREATE TABLE raw (

	timestamp datetime,
	asset_id INT NOT NULL,
	attribute_id INT NOT NULL,
	measurement varchar(8) NOT NULL,
	value double NOT NULL,
	SHARD(attribute_id),
	KEY(timestamp)

);

CREATE TABLE resolution_15_min (

	timestamp datetime,
	asset_id INT NOT NULL,
	attribute_id INT NOT NULL,
	measurement varchar(8) NOT NULL,
	value double NOT NULL,
	SHARD(attribute_id),
	UNIQUE KEY(timestamp, asset_id, attribute_id, measurement),
	KEY(timestamp)

);
*/
package main

import (
	"database/sql"
	"log"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
)

func Insert(db *sql.DB, packets []shared.Packet) error {
	var wg sync.WaitGroup
	chunkSize := 10000
	for i := 0; i < len(packets); i += chunkSize {
		sqlStr := "INSERT INTO raw (timestamp, asset_id, attribute_id, measurement, value) VALUES "
		valueStrings := []string{}
		rows := []interface{}{}
		for j := i; j < i+chunkSize && j < len(packets); j++ {
			packet := packets[j]
			valueStrings = append(valueStrings, "(?, ?, ?, ?, ?)")
			rows = append(rows, packet.Timestamp.Format("2006-01-02 15:04:05"), packet.AssetID, packet.AttributeID, packet.Measurement, packet.Value)
		}
		sqlStr += strings.Join(valueStrings, ",")
		wg.Add(1)
		go func(wg *sync.WaitGroup, sqlStr string) {
			defer wg.Done()
			stmt, err := db.Prepare(sqlStr)
			defer stmt.Close()
			if err != nil {
				log.Fatal(err)
			}
			_, err = stmt.Exec(rows...)
			if err != nil {
				log.Fatal(err)
			}
		}(&wg, sqlStr)
	}
	wg.Wait()
	return nil
}

func DownSampling(db *sql.DB, start time.Time, end time.Time) (err error) {
	stmt, err := db.Prepare(`
		INSERT INTO resolution_15_min (timestamp, asset_id, attribute_id, measurement, value)
			SELECT 
					from_unixtime(unix_timestamp(timestamp) DIV (15*60) * (15*60)) as timestamp, 
					asset_id, 
					attribute_id, 
					'mean' as measurement, 
					avg(value) as value
			FROM raw
			WHERE timestamp >= ?
				AND timestamp <= ?
			GROUP BY 
					from_unixtime(unix_timestamp(timestamp) DIV (15*60) * (15*60)), 
					asset_id, 
					attribute_id
			ORDER BY timestamp
		ON DUPLICATE KEY UPDATE
				value = VALUES(value)
	`)

	if err != nil {
		return
	}

	_, err = stmt.Exec(
		start.Format("2006-01-02 15:04:05"),
		end.Format("2006-01-02 15:04:05"),
	)
	return err
}

func main() {

	HOSTNAME := ""
	PORT := "3306"
	USERNAME := "admin"
	PASSWORD := ""
	DATABASE := ""
	filename := "singlestore-<>vCPU-latency.csv"

	connection := USERNAME + ":" + PASSWORD + "@tcp(" + HOSTNAME + ":" + PORT + ")/" + DATABASE + "?parseTime=true"
	db, err := sql.Open("mysql", connection)
	db.SetMaxOpenConns(100)
	if err != nil {
		panic(err)
	}
	defer db.Close()

	recorder := shared.NewQueryLatencyRecorder(filename)
	shared.RunTest(
		recorder,
		"singlestore",
		func(p []shared.Packet) error {
			return Insert(db, p)
		}, func(start, end time.Time) error {
			return DownSampling(db, start, end)
		},
	)
}
