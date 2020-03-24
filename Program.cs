using System;
using Newtonsoft.Json;
using System.IO;
using MongoDB.Bson;
using MongoDB.Bson.Serialization;
using MongoDB.Driver;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;

namespace Work
{
    class Program
    {

        public async static Task Main(string[] args)
        {
            string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";
            const string connectionString = "mongodb://localhost:27017";

            // Create a MongoClient object by using the connection string
            var client = new MongoClient(connectionString);

            //Use the MongoClient to access the server
            var database = client.GetDatabase("test");
            BsonDocument bsonDoc = new BsonDocument();
            IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("addresses"); // initialize to the collection to write to.
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
                    BsonValue value = null;
                    var filter = Builders<BsonDocument>.Filter.Eq("id_lokalId", item.GetValue("id_lokalId"));
                    var filter2 = Builders<BsonDocument>.Filter.Eq("virkningTil",value);
                    var results = collection.Find(filter).ToList();
                    var sortedResults = results.OrderBy(e => e.GetValue("registreringFra"));
                    //var list =  Newtonsoft.Json.JsonConvert.SerializeObject(results);
                    Console.WriteLine(sortedResults.Count());
                  
                    foreach(var result in sortedResults)
                    {
                        Console.WriteLine("This is the same object " + result);
                    }
                    //Console.WriteLine("This is the last element in list " + sortedResults.Last() );
                    //await collection.FindOneAndUpdateAsync()
                    break;

                    // Find a address in mongodb with the same id lokalid
                    
                    // Compare the registreringFra and take the newest one

                    // Update the document based on the id, with the newest data
                }
            }
        }

        /*
                public async static void mongoDBWriter()
                {
                    string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";
                    const string connectionString = "mongodb://localhost:27017";

                    // Create a MongoClient object by using the connection string
                    var client = new MongoClient(connectionString);

                    //Use the MongoClient to access the server
                    var database = client.GetDatabase("test");
                    BsonDocument bsonDoc = new BsonDocument();
                    IMongoCollection<BsonDocument> collection = database.GetCollection<BsonDocument>("addresses"); // initialize to the collection to write to.
                    List<BsonDocument> batch = new List<BsonDocument>();
                    var options = new CreateIndexOptions() { Unique = true };
                    var insertOptions = new InsertManyOptions { IsOrdered = false };
                    var keys = Builders<BsonDocument>.IndexKeys.Combine("registreringFra", "id_lokalId");
                    var indexModel = new CreateIndexModel<BsonDocument>(keys, options);

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
                                        if (batch.Count >= 100000)
                                        {
                                            i += 100000;
                                            await collection.InsertManyAsync(batch, insertOptions);
                                            batch.Clear();
                                            Console.WriteLine("Inserted 100000 records");
                                            Console.WriteLine(i);
                                            //await collection.Indexes.CreateOneAsync(indexModel);

                                        }





                                    }

                                }
                            }
                        }
                    }
                }
                */
    }

}
