package main

import (
	"context"
	"log"
	"math/rand"
	"os"
	"sync"
	"time"

	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"go.mongodb.org/mongo-driver/mongo/readpref"
)

type Metadata struct {
	AssetID int `bson:"asset_id"`
}

type MongoDBPacket struct {
	Timestamp time.Time `bson:"timestamp"`
	Metadata  Metadata  `bson:"metadata"`
	Value1    float64   `bson:"1"`
	Value2    float64   `bson:"2"`
	Value3    float64   `bson:"3"`
	Value4    float64   `bson:"4"`
	Value5    float64   `bson:"5"`
	Value6    float64   `bson:"6"`
	Value7    float64   `bson:"7"`
	Value8    float64   `bson:"8"`
	Value9    float64   `bson:"9"`
	Value10   float64   `bson:"10"`
}

func GeneratePackets(start time.Time, assetCount int, attributeCount int) (packets []MongoDBPacket) {
	for i := 0; i < assetCount; i++ {
		packets = append(packets, MongoDBPacket{
			Timestamp: start,
			Metadata: Metadata{
				AssetID: (i),
			},
			Value1:  rand.Float64() * 100,
			Value2:  rand.Float64() * 100,
			Value3:  rand.Float64() * 100,
			Value4:  rand.Float64() * 100,
			Value5:  rand.Float64() * 100,
			Value6:  rand.Float64() * 100,
			Value7:  rand.Float64() * 100,
			Value8:  rand.Float64() * 100,
			Value9:  rand.Float64() * 100,
			Value10: rand.Float64() * 100,
		})
	}

	return
}

func DownSampling(client *mongo.Client, start time.Time, end time.Time) (err error) {
	stages := []bson.D{
		{
			{Key: "$match", Value: bson.D{
				{Key: "timestamp", Value: bson.D{
					{"$gte", start},
					{"$lt", end},
				}},
			}},
		},
		{
			{Key: "$group", Value: bson.D{
				{Key: "_id", Value: bson.D{
					{Key: "asset_id", Value: "$metadata.asset_id"},
					{Key: "timestamp", Value: bson.D{
						{"$subtract", bson.A{
							"$timestamp",
							bson.D{
								{"$mod", bson.A{
									bson.D{
										{"$subtract", bson.A{"$timestamp", start}},
									},
									bson.D{{"$multiply", []interface{}{60, 1000}}}},
								}},
						}},
					}},
				}},
				{"1", bson.D{{"$avg", "$1"}}},
				{"2", bson.D{{"$avg", "$2"}}},
				{"3", bson.D{{"$avg", "$3"}}},
				{"4", bson.D{{"$avg", "$4"}}},
				{"5", bson.D{{"$avg", "$5"}}},
				{"6", bson.D{{"$avg", "$6"}}},
				{"7", bson.D{{"$avg", "$7"}}},
				{"8", bson.D{{"$avg", "$8"}}},
				{"9", bson.D{{"$avg", "$9"}}},
				{"10", bson.D{{"$avg", "$10"}}},
			}},
		},
		{
			{"$project", bson.D{
				{"_id", false},
				{"timestamp", "$_id.timestamp"},
				{"metadata", bson.D{
					{"asset_id", "$_id.asset_id"},
				}},
				{"1", true},
				{"2", true},
				{"3", true},
				{"4", true},
				{"5", true},
				{"6", true},
				{"7", true},
				{"8", true},
				{"9", true},
				{"10", true},
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

	_, err = client.Database("test").Collection("resolution_1_min").InsertMany(context.Background(), results)
	if err != nil {
		return (err)
	}

	return err
}

func Insert(client *mongo.Client, packets []interface{}) (err error) {
	_, err = client.Database("test").Collection("raw").InsertMany(context.Background(), packets)
	return err
}

func main() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://benchmarking-mongodb.asia-southeast1-b.c.resync-cloud-dev.internal:27017"))
	defer func() {
		if err = client.Disconnect(ctx); err != nil {
			panic(err)
		}
	}()

	err = client.Ping(ctx, readpref.Primary())
	if err != nil {
		panic(err)
	}

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
					packets := GeneratePackets(time.Now(), 15000, 10)
					s := make([]interface{}, len(packets))
					for i, v := range packets {
						s[i] = v
					}
					performanceStart := time.Now()
					err = Insert(client, s)
					text := time.Now().Format(time.RFC3339) + ",mongodb.insert," + string(time.Since(performanceStart).Milliseconds()) + "\n"
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
			err = DownSampling(client, time.Now().Add(-2*time.Minute), time.Now())
			text := time.Now().Format(time.RFC3339) + ",mongodb.down-sampling," + string(time.Since(performanceStart).Milliseconds()) + "\n"
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
	"resolution_1_min",
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
