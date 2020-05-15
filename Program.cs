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
using Newtonsoft.Json.Linq;
using System.Text.RegularExpressions;


namespace Work
{
    class Program
    {

        private static List<string> husnummers = new List<string>();

        public static async Task Main(string[] args)
        {

            //ProcessGeoDirectory("/home/mehigh/Adresses_kafka");
            await AdressToKafka();
        }

        public static void  ProcessGeoDirectory(string targetDirectory)
        {
            string[] fileEntries = Directory.GetFiles(targetDirectory);
            foreach (string filenName in fileEntries)
            {
                //Console.WriteLine(filenName);
                JsonToKafka(filenName);
            }
        }

        public static async Task  AdressToKafka()
        {
            using (FileStream fs = new FileStream("/home/mehigh/AddressData/AddressAgain.json", FileMode.Open, FileAccess.Read))
            using (StreamReader sr = new StreamReader(fs))
            using (JsonTextReader reader = new JsonTextReader(sr))
            {
                while (await reader.ReadAsync())
                {
                    // Advance the reader to start of first array, 
                    // which should be value of the "Stores" property
                    var listName = string.Empty;
                    while (reader.TokenType != JsonToken.StartArray)
                    {
                        if (reader.TokenType == JsonToken.PropertyName)
                        {
                            listName = reader?.Value.ToString();
                            Console.WriteLine(listName);
                        }

                        await reader.ReadAsync();
                    }

                    var jsonText = new List<string>();
                    // Now process each store individually
                    while (await reader.ReadAsync())
                    {
                        if (reader.TokenType == JsonToken.EndArray)
                            break;

                        if (reader.TokenType == JsonToken.StartObject)
                        {
                            dynamic obj = await JObject.LoadAsync(reader);
                            //Console.WriteLine(obj);
                            //jsonText.Add(JsonConvert.SerializeObject(obj, Formatting.Indented));
                            jsonText.Add(ChangeNumericalPropertyNames(obj));

                            if (jsonText.Count >= 100000)
                            {

                                KafkaProducer(listName, jsonText);
                                jsonText.Clear();
                                Console.WriteLine("Wrote 100000 objects into topic");
                            }
                        }
                    }

                    KafkaProducer(listName, jsonText);
                    jsonText.Clear();
                    System.Console.WriteLine("The end of the world!");
                }
            }
        }

        public static void  JsonToKafka(String filename)
        {
            //string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";

            String jsonDoc = "";
            List<String> batch = new List<string>();

            using (FileStream s = File.Open(filename, FileMode.Open))
            using (var streamReader = new StreamReader(s))
            {
                Console.WriteLine(filename);
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                  
                        
                   
                        while (reader.Read())
                        {

                            Console.WriteLine(reader.TokenType);
                            Console.WriteLine(reader.Value);
                            if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                            {
                                //Console.WriteLine(reader.TokenType);

                                Object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj,Formatting.Indented);
                                Console.WriteLine(jsonDoc);
                                //var obj = JObject.Load(reader);
                                //jsonDoc = ChangeNumericalPropertyNames(obj);
                            }
                            //batch.Add(jsonDoc);

                            //Console.WriteLine(batch.Count());
                            //if (batch.Count >= 10000)
                           // {
                             //   KafkaProducer(filename, batch);
                               // batch.Clear();
                            //}
                        }
                }
            }
        }

        public static void KafkaProducer(String topicname, List<String> batch)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            int i = 0;
            using (var p = new ProducerBuilder<Null, String>(config).Build())
            {
                Console.WriteLine(batch.Count());
                i += 10000;
             
                
                foreach (var document in batch)
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
                Console.WriteLine(i);
                //Console.WriteLine(topicname);
            }
        }

        public static string ChangeNumericalPropertyNames(JObject jo)
        {
            //Console.WriteLine("inside function");

            foreach (JProperty jp in jo.Properties().ToList())
            {
                switch(jp.Name)
                {
                    case "forretningshændelse":
                        jp.Replace(new JProperty("businessEvent", jp.Value));
                        break;
                    case "forretningsområde":
                        jp.Replace(new JProperty("businessArea", jp.Value));
                        break;
                    case "forretningsproces":
                        jp.Replace(new JProperty("businessProces", jp.Value));
                        break;
                    case "registreringFra":
                        jp.Replace(new JProperty("registrationFrom", jp.Value));
                        break;
                    case "registreringsaktør":
                        jp.Replace(new JProperty("registrationActor", jp.Value));
                        break;
                    case "registreringTil":
                        jp.Replace(new JProperty("registrationTo", jp.Value));
                        break;
                    case "virkningFra":
                        jp.Replace(new JProperty("effectFrom", jp.Value));
                        break;
                    case "virkningsaktør":
                        jp.Replace(new JProperty("effectActor", jp.Value));
                        break;
                    case "virkningTil":
                        jp.Replace(new JProperty("effectTo", jp.Value));
                        break;
                    case "adressebetegnelse":
                        jp.Replace(new JProperty("unitAddressDescription", jp.Value));
                        break;
                    case "dørbetegnelse":
                        jp.Replace(new JProperty("door", jp.Value));
                        break;
                    case "dørpunkt":
                        jp.Replace(new JProperty("doorPoint", jp.Value));
                        break;
                    case "etagebetegnelse":
                        jp.Replace(new JProperty("floor", jp.Value));
                        break;
                    case "bygning":
                        jp.Replace(new JProperty("building", jp.Value));
                        break;
                    case "husnummer":
                        jp.Replace(new JProperty("houseNumber", jp.Value));
                        break;
                    case "oprindelse_kilde":
                        jp.Replace(new JProperty("registrationSource", jp.Value));
                        break;
                    case "oprindelse_nøjagtighedsklasse":
                        jp.Replace(new JProperty("registrationPrecision", jp.Value));
                        break;
                    case "oprindelse_registrering":
                        jp.Replace(new JProperty("registration", jp.Value));
                        break;
                    case "oprindelse_tekniskStandard":
                        jp.Replace(new JProperty("registrationTechicalStandard", jp.Value));
                        break;
                    case "adgangsadressebetegnelse":
                        jp.Replace(new JProperty("accessAddressDescription", jp.Value));
                        break;
                    case "adgangspunkt":
                        jp.Replace(new JProperty("addressPoint", jp.Value));
                        break;
                    case "husnummerretning":
                        jp.Replace(new JProperty("houseNumberDirection", jp.Value));
                        break;
                    case "husnummertekst":
                        jp.Replace(new JProperty("houseNumberText", jp.Value));
                        break;
                    case "vejpunkt":
                        jp.Replace(new JProperty("roadPoint", jp.Value));
                        break;
                    case "jordstykke":
                        jp.Replace(new JProperty("plot", jp.Value));
                        break;
                    case "placeretPåForeløbigtJordstykke":
                        jp.Replace(new JProperty("placedOnTemporaryPlot", jp.Value));
                        break;
                    case "geoDanmarkBygning":
                        jp.Replace(new JProperty("geoDenmarkBuilding", jp.Value));
                        break;
                    case "adgangTilBygning":
                        jp.Replace(new JProperty("buildingAccess", jp.Value));
                        break;
                    case "adgangTilTekniskAnlæg":
                        jp.Replace(new JProperty("technicalStructureAccess", jp.Value));
                        break;
                    case "vejmidte":
                        jp.Replace(new JProperty("roadMid", jp.Value));
                        break;
                    case "afstemningsområde":
                        jp.Replace(new JProperty("electionDistrict", jp.Value));
                        break;
                    case "kommuneinddeling":
                        jp.Replace(new JProperty("municipalityDistrict", jp.Value));
                        break;
                    case "menighedsrådsafstemningsområde":
                        jp.Replace(new JProperty("churchVotingDistrict", jp.Value));
                        break;
                    case "sogneinddeling":
                        jp.Replace(new JProperty("churchDistrict", jp.Value));
                        break;
                    case "supplerendeBynavn":
                        jp.Replace(new JProperty("additionalCityName", jp.Value));
                        break;
                    case "navngivenVej":
                        jp.Replace(new JProperty("namedRoad", jp.Value));
                        break;
                    case "postnummer":
                        jp.Replace(new JProperty("postalCode", jp.Value));
                        break;
                    case "administreresAfKommune":
                        jp.Replace(new JProperty("municipalityAdministration", jp.Value));
                        break;
                    case "beskrivelse":
                        jp.Replace(new JProperty("description", jp.Value));
                        break;
                    case "udtaltVejnavn":
                        jp.Replace(new JProperty("pronouncedRoadName", jp.Value));
                        break;
                    case "vejadresseringsnavn":
                        jp.Replace(new JProperty("shortRoadName", jp.Value));
                        break;
                    case "vejnavn":
                        jp.Replace(new JProperty("roadName", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_oprindelse_kilde":
                        jp.Replace(new JProperty("roadRegistrationSource", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_oprindelse_nøjagtighedsklasse":
                        jp.Replace(new JProperty("roadRegistrationPrecision", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_oprindelse_registrering":
                        jp.Replace(new JProperty("roadRegistration", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_oprindelse_tekniskStandard":
                        jp.Replace(new JProperty("roadRegistrationTechicalStandard", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_vejnavnelinje":
                        jp.Replace(new JProperty("roadRegistrationRoadLine", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_vejnavneområde":
                        jp.Replace(new JProperty("roadRegistrationRoadAread", jp.Value));
                        break;
                    case "vejnavnebeliggenhed_vejtilslutningspunkter":
                        jp.Replace(new JProperty("roadRegistrationRoadConnectionPoints", jp.Value));
                        break;
                    case "kommune":
                        jp.Replace(new JProperty("municipality", jp.Value));
                        break;
                    case "vejkode":
                        jp.Replace(new JProperty("roadCode", jp.Value));
                        break;
                    case "postnummerinddeling":
                        jp.Replace(new JProperty("postalCodeDistrict", jp.Value));
                        break;         
                    default:
                        //Console.WriteLine("wrong input");
                        break;
                }
                //Console.WriteLine(jp.Name);
                //if (jp.Name == "forretningshændelse")
                //{
                  //  string name = "Events";
                    //jp.Replace(new JProperty(name, jp.Value));
                //}
                //Console.WriteLine(jo.ToString());
            }
            var obj = JsonConvert.SerializeObject(jo,Formatting.Indented);
            return obj;

        }


    }

}
