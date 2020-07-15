using System;
using Newtonsoft.Json;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using System.Diagnostics;




namespace Work
{
    class Program
    {
         static FTPClient  client = new FTPClient();


        public static async Task Main(string[] args)
        {
            //await getinitialAdressData();
            //await getLatestGeoData();
            //await getLatestAdressData();
            IsWithin(25,21,27);

        }

        public static async Task getinitialAdressData()
        {
            client.getAdressInitialLoad("https://selfservice.datafordeler.dk/filedownloads/626/334",@"/home/mehigh/ftptrials/adress.zip");
            client.UnzipFile(@"/home/mehigh/ftptrials/",@"/home/mehigh/ftptrials/");
            await ProcessLatestAdresses("/home/mehigh/ftptrials/","/home/mehigh/newftp");

        }

        public static async Task getLatestAdressData()
        {
            await client.getFileFtp("ftp3.datafordeler.dk","JFOWRLSDKM","sWRbn2M8y2tH!","/home/mehigh/ftptrials/");
            client.UnzipFile(@"/home/mehigh/ftptrials/",@"/home/mehigh/ftptrials/");
            await ProcessLatestAdresses("/home/mehigh/ftptrials/","/home/mehigh/newftp");
        }

        public static async Task getLatestGeoData()
        {
            //await client.getFileFtp("ftp3.datafordeler.dk","PCVZLGPTJE","sWRbn2M8y2tH!","/home/mehigh/geo/");
            //client.UnzipFile(@"/home/mehigh/geo/",@"/home/mehigh/geo/geogml");
            //convertToGeojson();
            ProcessGeoDirectory(@"/home/mehigh/geo/",@"/home/mehigh/NewGeo/", new List<string>(){"trae","bygning","chikane","bygvaerk","erhverv","systemlinje","vejkant","vejmidte"});
        }

        public static void ProcessGeoDirectory(string sourceDirectory, string destinationDirectory,List<String> geoFilter)
        {
            List<String> fileEntries = Directory.GetFiles(sourceDirectory).ToList();
            List<String> filtered = new List<String>();
            var result = fileEntries.Where(a => geoFilter.Any(b => a.Contains(b))).ToList();
            
            foreach (string fileName in result)
            {
                Console.WriteLine(fileName);
                var fileNoExtension = Path.GetFileNameWithoutExtension(fileName);
                var dest = Path.Combine(destinationDirectory,fileNoExtension);
                JsonToKafka(fileName);
                File.Move(fileName,dest);
            }
            
        }

        public static void convertToGeojson()
        {
            ProcessStartInfo startInfo = new ProcessStartInfo()
            {
                FileName = @"/home/mehigh/confluentKafka/convert_script.sh",
            };
            Process proc = new Process()
            {
                StartInfo = startInfo,
            };
            proc.Start();
        }

        public static async Task  ProcessLatestAdresses(string sourceDirectory, string destinationDirectory)
        {
            DirectoryInfo destinfo = new DirectoryInfo(destinationDirectory);
            if(destinfo.Exists == false)
            {
                Directory.CreateDirectory(destinationDirectory);
            }
            DirectoryInfo sourceinfo = new DirectoryInfo(sourceDirectory);
            DirectoryInfo [] dirs = sourceinfo.GetDirectories();

            List<String> fileEntries = Directory.GetFiles(sourceDirectory).ToList();
            foreach (string filename in fileEntries)
            {
                if (!filename.Contains("Metadata"))
                {
                    await AdressToKafka(filename);
                    string file = Path.GetFileName(filename);
                    string destFile = Path.Combine(destinationDirectory,file);
                    File.Move(filename, destFile);
                    Console.WriteLine("File moved in new directory");
                }
                else
                {
                    string file = Path.GetFileName(filename);
                    string destFile = Path.Combine(destinationDirectory,file);
                    File.Move(filename, destFile);
                }
            }
            //List<String> fileEntries = Directory.GetFiles(rootDirectory).ToList();

        }

        public static string ChangeDateFormat(string dateString)
        {
            DateTime result;
            if (dateString == null)
            {
                //Do nothing
                dateString = "null";
                return dateString;
            }
            else
            {
                try
                {
                    result = DateTime.Parse(dateString, null, System.Globalization.DateTimeStyles.RoundtripKind);
                    //Console.WriteLine("{0} converts to {1}.", dateString, result.ToString("yyyy-MM-dd HH:mm:ss"));
                    return result.ToString("yyyy-MM-dd HH:mm:ss");
                }
                catch (ArgumentNullException)
                {
                    Console.WriteLine("{0} is not in the correct format.", dateString);
                }
            }
            return String.Empty;
        }

         public static async Task AdressToKafka(string filename)
        {
            using (FileStream fs = new FileStream(filename, FileMode.Open, FileAccess.Read))
            using (StreamReader sr = new StreamReader(fs))
            using (JsonTextReader reader = new JsonTextReader(sr))
            {
                while (await reader.ReadAsync())
                {
                    // Advance the reader to start of first array, 
                    // which should be value of the "Stores" property
                    var listName = "AdresseList";
                    if (reader.TokenType != JsonToken.StartArray)
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
                            jsonText.Add(ChangeAdressNames(obj));

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


        public static void JsonToKafka(String filename)
        {
            //string inputFileName = "/home/mehigh/Addresses2/Addresses2.json";

            String jsonDoc = "";
            List<String> batch = new List<string>();

            using (FileStream s = File.Open(filename, FileMode.Open))

            using (var streamReader = new StreamReader(s))
            {
              //Console.WriteLine(filename);
             var file = Path.GetFileNameWithoutExtension(filename).Split("."); 
             var topicname = file[1];
             
                using (var reader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (reader.Read())
                    {
                        //Console.WriteLine(reader.TokenType);
                        //Console.WriteLine(reader.Value);
                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                        {
                            while (reader.Read())
                            {
                                if (reader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                                {
                                    while (reader.Read())
                                    {
                                        if (reader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                                        {
                                            Object obj = new Newtonsoft.Json.JsonSerializer().Deserialize(reader);
                                            jsonDoc = Newtonsoft.Json.JsonConvert.SerializeObject(obj, Formatting.Indented);
                                            //Console.WriteLine("THis is the object" + jsonDoc);


                                        }
                                        batch.Add(jsonDoc);
                                        //Console.WriteLine(batch.Count());
                                        if (batch.Count >= 10000)
                                        {
                                            KafkaProducerGeo(topicname, batch);
                                            batch.Clear();
                                        }
                                    }

                                    //Console.WriteLine(batch.Count());
                                    

                                }

                            }

                        }
                        KafkaProducerGeo(topicname, batch);
                        batch.Clear();

                    }
                }
            }
        }

        public static void KafkaProducer(String topicname, List<String> batch)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            int i = 0;
            using (var p = new ProducerBuilder<String, String>(config).Build())
            {
                Console.WriteLine(batch.Count());
                i += 10000;
                foreach (var document in batch)
                {
                    String id = String.Empty;

                    JObject o = JObject.Parse(document);
                    foreach (JProperty jp in o.Properties().ToList())
                    {
                        if (jp.Name == "id_lokalId")
                        {
                            id = (String)jp.Value;
                        }
                    }

                    try
                    {
                        p.Produce(topicname, new Message<String, string> { Value = document, Key = id });
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


        public static void KafkaProducerGeo(String topicname, List<String> batch)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            int i = 0;
            using (var p = new ProducerBuilder<String, String>(config).Build())
            {
                Console.WriteLine(topicname);
                Console.WriteLine(batch.Count());
                i += 10000;


                foreach (var document in batch)
                {
                    String id = String.Empty;

                    JObject o = JObject.Parse(document);
                    //Console.Write(o);
                    id = o["properties"]["gml_id"].ToString();
                    var ar = o["geometry"]["coordinates"].ToList();
                    foreach(var a in ar)
                    {
                        foreach(var b in a)
                        {
                            foreach(var c in b)
                            {
                                //Console.WriteLine(c);
                                //Console.WriteLine("one object");
                                int xvalue = (int)c[0];
                                int yvalue = (int)c[1];
                                if(IsWithin(xvalue,538913,568605) && IsWithin(yvalue,6182387,6199152))
                                {
                                    Console.WriteLine("hey");
                                }
                            }
                        }
                        
                    }
                    /*
                    try
                    {
                        p.Produce(topicname, new Message<String, string> { Value = document, Key = id });
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }
                    */

                }
                p.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine(i);
                //Console.WriteLine(topicname);
            }
        }

        public static bool IsWithin(int value, int minimum, int maximum)
        {
               Console.WriteLine(value >= minimum && value <= maximum);
            return value >= minimum && value <= maximum;
         
        }
        public static string ChangeAdressNames(JObject jo)
        {
            //Console.WriteLine("inside function");

            foreach (JProperty jp in jo.Properties().ToList())
            {
                switch (jp.Name)
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
                        string dateValue = (string)jp.Value;
                        jp.Value = ChangeDateFormat(dateValue);
                        jp.Replace(new JProperty("registrationFrom", jp.Value));
                        break;
                    case "registreringsaktør":
                        jp.Replace(new JProperty("registrationActor", jp.Value));
                        break;
                    case "registreringTil":
                        dateValue = (string)jp.Value;
                        jp.Value = ChangeDateFormat(dateValue);
                        jp.Replace(new JProperty("registrationTo", jp.Value));
                        break;
                    case "virkningFra":
                        dateValue = (string)jp.Value;
                        jp.Value = ChangeDateFormat(dateValue);
                        jp.Replace(new JProperty("effectFrom", jp.Value));
                        break;
                    case "virkningsaktør":
                        jp.Replace(new JProperty("effectActor", jp.Value));
                        break;
                    case "virkningTil":
                        dateValue = (string)jp.Value;
                        jp.Value = ChangeDateFormat(dateValue);
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
            }
            var obj = JsonConvert.SerializeObject(jo, Formatting.Indented);
            return obj;

        }


    }

}
