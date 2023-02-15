package main

import (
	"context"
	"time"

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
				{Key: "1", Value: bson.D{{Key: "$avg", Value: "$1"}}},
				{Key: "2", Value: bson.D{{Key: "$avg", Value: "$2"}}},
				{Key: "3", Value: bson.D{{Key: "$avg", Value: "$3"}}},
				{Key: "4", Value: bson.D{{Key: "$avg", Value: "$4"}}},
				{Key: "5", Value: bson.D{{Key: "$avg", Value: "$5"}}},
				{Key: "6", Value: bson.D{{Key: "$avg", Value: "$6"}}},
				{Key: "7", Value: bson.D{{Key: "$avg", Value: "$7"}}},
				{Key: "8", Value: bson.D{{Key: "$avg", Value: "$8"}}},
				{Key: "9", Value: bson.D{{Key: "$avg", Value: "$9"}}},
				{Key: "10", Value: bson.D{{Key: "$avg", Value: "$10"}}},
			}},
		},
		{
			{Key: "$project", Value: bson.D{
				{Key: "_id", Value: false},
				{Key: "timestamp", Value: "$_id.timestamp"},
				{Key: "metadata", Value: bson.D{
					{Key: "asset_id", Value: "$_id.asset_id"},
				}},
				{Key: "1", Value: true},
				{Key: "2", Value: true},
				{Key: "3", Value: true},
				{Key: "4", Value: true},
				{Key: "5", Value: true},
				{Key: "6", Value: true},
				{Key: "7", Value: true},
				{Key: "8", Value: true},
				{Key: "9", Value: true},
				{Key: "10", Value: true},
			}},
		},
	}

	cursor, err := client.Database("test").Collection("raw").Aggregate(context.Background(), stages)
	if err != nil {
		return (err)
	}

	// display the results
	var results []interface{}
	if err = cursor.All(context.Background(), &results); err != nil {
		return (err)
	}

	for _, result := range results {
		_, err = client.Database("test").Collection("resolution_15_min").UpdateOne(
			context.Background(),
			bson.D{
				{Key: "timestamp", Value: result.(bson.M)["timestamp"]},
				{Key: "metadata.asset_id", Value: result.(bson.M)["metadata"].(bson.M)["asset_id"]},
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
	packetsToInsert := make([]interface{}, len(packets))
	for i, v := range packets {
		packetsToInsert[i] = MongoDBPacket{
			Timestamp: v.Timestamp,
			Metadata: MongoDBPacketMetadata{
				AssetID:     v.AssetID,
				AttributeID: v.AttributeID,
				Measurement: v.Measurement,
			},
			Value: v.Value,
		}
	}
	_, err = client.Database("resync").Collection("raw").InsertMany(context.Background(), packetsToInsert)
	return err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(context.Background(), readpref.Primary())
	if err != nil {
		panic(err)
	}

	recorder := shared.NewQueryLatencyRecorder("mongodb-<>vCPU-latency.csv")
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
       timestamp: {
				$subtract: ["$timestamp", {
         $mod: [{
           $subtract: ["$timestamp", low]
         }, 60*1000]
       }]
				 }
     },
     1: {$avg: "$1"},
     2: {$avg: "$2"},
     3: {$avg: "$3"},
		 4: {$avg: "$4"},
		 5: {$avg: "$5"},
		 6: {$avg: "$6"},
		 7: {$avg: "$7"},
		 8: {$avg: "$8"},
		 9: {$avg: "$9"},
		 10: {$avg: "$10"},
    }
  },
	{
		$project:{
			_id: false,
			timestamp: "$_id.timestamp",
			metadata: {
				asset_id: "$_id.asset_id"
			},
			"1" :true,
			"2" :true,
			"3" :true,
			"4" :true,
			"5" :true,
			"6" :true,
			"7" :true,
			"8" :true,
			"9" :true,
			"10" :true,
		}
	},
]).forEach(function(doc){
   db.resolution_1_min.insert(doc);
});
*/
