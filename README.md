# Benchmarking Script

This script is used to benchmark the performance of different time-series databases.

## How to run the test?

1. Setup the database you want to benchmark. For example, if you want to benchmark InfluxDB, you should setup InfluxDB first.
2. Setup the tables or buckets in the database.
3. Replace the configuration in your intended database's benchmarking script. For example, if you want to benchmark InfluxDB, you should replace `<host>` to `localhost` etc.
4. Configure the filename to store the benchmarking result. `filename := "influx-2vCPU-latency.csv"` or `filename := "influx-4vCPU-latency.csv"`
5. Run the benchmarking script. `go run ./cmd/influx/main.go`

## How to run the benchmarking script on GCP VM?

### Concepts

- Sync the benchmarking script to the VM instance using SFTP
- Run the benchmarking script on remote VM instance in background

These strategies are used to avoid back-and-forth debugging between local and remote VM instance.

### Steps

1. Install https://marketplace.visualstudio.com/items?itemName=Natizyskunk.sftp in vscode
2. Configure the sftp.json file

```json
{
  "name": "Benchmarking Client",
  "host": "<IP>",
  "protocol": "sftp",
  "port": 22,
  "username": "<username>",
  "remotePath": "/home/<username>/benchmarking",
  "privateKeyPath": "/Users/<username>/.ssh/google_compute_engine",
  "uploadOnSave": true,
  "useTempFile": false,
  "openSsh": false,
  "ignore": [".vscode", ".git", ".DS_Store", "node_modules", "results"]
}
```

3. SSH to the VM instance `gcloud compute ssh --zone "asia-southeast1-b" "benchmarking-client" --project "<project-id>"`
4. Run the benchmarking script in background

```bash
cd benchmarking
nohup go run ./cmd/clickhouse/. > clickhouse-2vCPU.out 2> clickhouse-2vCPU.err < /dev/null &
```
