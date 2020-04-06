using System;
using Newtonsoft.Json;
using System.IO;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;

namespace Work
{
    class Program
    {
        
        private static  List<string> husnummers = new List<string>();

        public static void Main(string[] args)
        {
            //await mongoDBWriter();
            //await mongoDbReader();
             //adressKafkaProducer("/home/mehigh/Addresses2/Addresses2.json");
             ProcessDirectory("/home/mehigh/GeoData","geodata-topic");
        }

        private async static Task HandleInvalidBatch(List<BsonDocument> invalidDocuments, IMongoCollection<BsonDocument> collection)
        {
            foreach (var item in invalidDocuments)
            {
                try
                {
                    await collection.InsertOneAsync(item);
                }
                catch (System.Exception)
                {
                    Console.WriteLine(item.ToJson());
                    break;
                }
            }
        }


        public async static Task mongoDBWriter()
        {
            string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";
            const string connectionString = "mongodb://localhost:27017";

            // Create a MongoClient object by using the connection string
            var client = new MongoClient(connectionString);

            //Use the MongoClient to access the server
            var database = client.GetDatabase("test");
            BsonDocument bsonDoc = new BsonDocument();
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("adresses"); // initialize to the collection to write to.
            List<BsonDocument> batch = new List<BsonDocument>();
            var options = new CreateIndexOptions() { Unique = false };
            var insertOptions = new InsertManyOptions { IsOrdered = false };

            var registreringFraKey = Builders<BsonDocument>.IndexKeys.Descending("registreringFra");
            var idLokalId = Builders<BsonDocument>.IndexKeys.Descending("id_lokalId");

            // var keys = Builders<BsonDocument>.IndexKeys.Combine("registreringFra", "id_lokalId");

            var keys = Builders<BsonDocument>.IndexKeys.Combine(registreringFraKey, idLokalId);

            var indexModel = new CreateIndexModel<BsonDocument>(keys, options);
            // await collection.Indexes.CreateOneAsync(indexModel);

            using (var streamReader = new StreamReader(inputFileName))
            {
                int i = 0;
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (reader.Read())
                    {
                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                        {
                            Console.WriteLine(reader.TokenType);
                            while (reader.Read())
                            {
                                if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                                {
                                    object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                    var jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
                                    bsonDoc = MongoDB.Bson.Serialization.BsonSerializer.Deserialize<BsonDocument>(jsonDoc);
                                }

                                batch.Add(bsonDoc);
                                if (batch.Count >= 10000)
                                {
                                    i += 10000;
                                    try
                                    {
                                        await collection.InsertManyAsync(batch, insertOptions);
                                        batch.Clear();
                                        Console.WriteLine("Inserted 100000 records");
                                        Console.WriteLine(i);
                                    }
                                    catch (System.Exception)
                                    {
                                        Console.WriteLine("Exception");
                                        await HandleInvalidBatch(batch, collection);
                                        batch.Clear();
                                        Console.WriteLine("Inserted 100000 records");
                                        Console.WriteLine(i);
                                    }
                                }
                            }
                        }
                    }
                }
            }

        }

        public  static void KafkaProducer(String filename, String topicname)
        {
            //string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";

            var config = new ProducerConfig {BootstrapServers = "localhost:9092",LingerMs = 30, BatchNumMessages = 10000, QueueBufferingMaxMessages = 100000 };
            String jsonDoc = "";
            List<String> batch = new List<string>();
            int i = 0;

/*
            Action<DeliveryReport<Null, string>> handler = r => 
            Console.WriteLine(!r.Error.IsError
                ? $"Delivered message to {r.TopicPartitionOffset}"
                : $"Delivery Error: {r.Error.Reason}");
*/

            using (var streamReader = new StreamReader(filename))
            {
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (reader.Read())
                    {
                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                        {
                            //Console.WriteLine(reader.TokenType);
                            while (reader.Read())
                            {
                                if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                                {
                                    Object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                    jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj);
                                }

                                batch.Add(jsonDoc);
                                //Console.WriteLine(batch.Count);
                                if(batch.Count >= 10000)
                                {
                

                                using (var p = new ProducerBuilder<Null, String>(config).Build())
                                {
                                    i+= 10000;
                                    foreach(var document in batch)
                                    {
                                    
                                    try
                                    { 
                                        p.Produce(topicname, new Message<Null, string> { Value = document });
                                    }
                                    catch (ProduceException<Null, string> e)
                                    {
                                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                                    }

                                    }
                                     p.Flush(TimeSpan.FromSeconds(10));
                                     batch.Clear();
                                     Console.WriteLine(i);          
                                }
                                    }

                                    
                            }
                        }
                    }
                    }
                }
        }

     

        public static void ProcessDirectory(string targetDirectory,string topicname)
        {
            string [] fileEntries = Directory.GetFiles(targetDirectory);
            foreach(string filenName in fileEntries)
            {
                Console.WriteLine(filenName);
                KafkaProducer(filenName,topicname);
            }
        }       

        public async static Task mongoDbReader()
        {
            const string connectionString = "mongodb://localhost:27017";

            // Create a MongoClient object by using the connection string
            var client = new MongoClient(connectionString);

            //Use the MongoClient to access the server
            var database = client.GetDatabase("test");

            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("adresses"); // initialize to the collection to write to
            List<BsonDocument> HorsensHusnnumer = new List<BsonDocument>();
            var filter = new BsonDocument("postnummer", "f5c4d790-04ce-4a37-8b02-feaefa353c6e");
            using(IAsyncCursor<BsonDocument> cursor = await collection.FindAsync(filter))
            {
                while(await cursor.MoveNextAsync())
                {
                    IEnumerable<BsonDocument> batch  = cursor.Current;
                    foreach(BsonDocument document in batch)
                    {
                        var newDoc = Newtonsoft.Json.JsonConvert.SerializeObject(document);

                        husnummers.Add(newDoc);

                    }

                    Console.WriteLine("The number of house numbers are " + HorsensHusnnumer.Count);

                }

            }


        }


    }

}
