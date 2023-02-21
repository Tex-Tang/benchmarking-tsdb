package main

import (
	"context"
	"time"

	//"fmt"

	"gitlab.com/resynctech/resync-cloud/benchmarking/shared"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

func DownSampling(client *mongo.Client, start time.Time, end time.Time) (err error) {
	stages := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "timestamp", Value: bson.D{
					{Key: "$gte", Value: start},
					{Key: "$lt", Value: end},
				}},
			}},
		},
		{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "asset_id", Value: "$metadata.asset_id"},
					{Key: "attribute_id", Value: "$metadata.attribute_id"},
					{Key: "timestamp", Value: bson.D{
						{Key: "$subtract", Value: bson.A{
							"$timestamp",
							bson.D{
								{Key: "$mod", Value: bson.A{
									bson.D{
										{Key: "$subtract", Value: bson.A{"$timestamp", start}},
									},
									bson.D{{Key: "$multiply", Value: []interface{}{60 * 15, 1000}}}},
								}},
						}},
					}},
				}},
				{Key: "value", Value: bson.D{{Key: "$avg", Value: "$value"}}},
			}},
		},
		{
			{Key: "$project", Value: bson.D{
				{Key: "_id", Value: false},
				{Key: "timestamp", Value: "$_id.timestamp"},
				{Key: "metadata", Value: bson.D{
					{Key: "asset_id", Value: "$_id.asset_id"},
					{Key: "attribute_id", Value: "$_id.attribute_id"},
					{Key: "measurement", Value: "mean"},
				}},
				{Key: "value", Value: true},
			}},
		},
	}

	cursor, err := client.Database("resync").Collection("raw").Aggregate(context.Background(), stages)
	if err != nil {
		return (err)
	}

	// display the results
	var results []interface{}
	if err = cursor.All(context.Background(), &results); err != nil {
		return (err)
	}

	for _, result := range results {
		timestamp := result.(bson.D).Map()["timestamp"]
		metadata := result.(bson.D).Map()["metadata"].(bson.D).Map()
		_, err = client.Database("resync").Collection("resolution_15_min").UpdateOne(
			context.Background(),
			bson.D{
				{Key: "timestamp", Value: timestamp},
				{Key: "metadata.asset_id", Value: metadata["asset_id"]},
				{Key: "metadata.attribute_id", Value: metadata["attribute_id"]},
				{Key: "metadata.measurement", Value: metadata["measurement"]},
			},
			bson.D{
				{Key: "$set", Value: result},
			},
			options.Update().SetUpsert(true),
		)
		if err != nil {
			return err
		}
	}

	return err
}

type MongoDBPacketMetadata struct {
	AssetID     uint32 `bson:"asset_id"`
	AttributeID uint32 `bson:"attribute_id"`
	Measurement string `bson:"measurement"`
}

type MongoDBPacket struct {
	Timestamp time.Time             `bson:"timestamp"`
	Metadata  MongoDBPacketMetadata `bson:"metadata"`
	Value     float64               `bson:"value"`
}

func Insert(client *mongo.Client, packets []shared.Packet) (err error) {
	packetsToInsert := []mongo.WriteModel{}
	for _, v := range packets {
		packetsToInsert = append(packetsToInsert, mongo.NewInsertOneModel().SetDocument(
			MongoDBPacket{
				Timestamp: v.Timestamp,
				Metadata: MongoDBPacketMetadata{
					AssetID:     v.AssetID,
					AttributeID: v.AttributeID,
					Measurement: v.Measurement,
				},
				Value: v.Value,
			},
		))
	}
	_, err = client.Database("resync").Collection("raw").BulkWrite(context.Background(), packetsToInsert)
	return err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	connStr := "mongodb+srv://doadmin:8j06oJ4cKN2b9G71@db-mongodb-sgp1-91395-c2ae6d49.mongo.ondigitalocean.com/admin?tls=true&authSource=admin&replicaSet=db-mongodb-sgp1-91395"
	filename := "mongodb-8vCPU-latency.csv"
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(connStr))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		panic(err)
	}

	recorder := shared.NewQueryLatencyRecorder(filename)
	shared.RunTest(
		recorder,
		"mongodb",
		func(p []shared.Packet) error {
			return Insert(client, p)
		},
		func(start time.Time, end time.Time) error {
			return DownSampling(client, start, end)
		},
	)
}

/*
db.createCollection(
	"raw",
	{
		timeseries: {
			timeField: "timestamp",
			metaField: "metadata",
			granularity: "seconds"
		}
	}
)

db.createCollection(
	"resolution_15_min",
	{
		timeseries: {
			timeField: "timestamp",
			metaField: "metadata",
			granularity: "minutes"
		}
	}
)
var low = ISODate("2023-02-01T00:00:00.000+02:00")
var high = ISODate("2023-02-31T00:00:00.000+02:00")
db.raw.aggregate([
  {$match: {timestamp:{$gte: low, $lt: high}}},
	{$group: {
     _id:{
			 asset_id: "$metadata.asset_id",
			 attribute_id: "$metadata.attribute_id",
       timestamp: {
				$subtract: [
					"$timestamp",
					{ $mod: [{ $subtract: ["$timestamp", low] }, 60*1000]}
				]
			}
     },
     value: {$avg: "$value"},
  }},
	{
		$project:{
			_id: false,
			timestamp: "$_id.timestamp",
			metadata: {
				asset_id: "$_id.asset_id",
				attribute_id: "$_id.attribute_id",
				measurement: "mean"
			},
			value: true
		}
	},
])
*/
