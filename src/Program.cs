using System;
using Newtonsoft.Json;
using System.IO;
using System.Threading.Tasks;
using System.Collections.Generic;
using System.Linq;
using Confluent.Kafka;
using Newtonsoft.Json.Linq;
using System.Diagnostics;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;
using Microsoft.Extensions.Configuration;
using Datafordelen.Config;

namespace Datafordelen
{
    class Program
    {
        private static FTPClient client = new FTPClient();
        private static AppSettings _appSettings = new AppSettings();

        public static async Task Main(string[] args)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(Directory.GetCurrentDirectory())
                .AddJsonFile("appsettings.json", optional: true, reloadOnChange: true);

            var configuration = builder.Build();
            ConfigurationBinder.Bind(configuration.GetSection("appSettings"), _appSettings);

            await getLatestAdressData();
        }

        public static async Task getinitialAdressData()
        {
            Console.WriteLine("this is the uri" + _appSettings.InitialAddressDataUrl);
            client.getAdressInitialLoad(_appSettings.InitialAddressDataUrl, _appSettings.InitialAddressDataZipFilePath);
            client.UnzipFile(_appSettings.InitialAddressDataUnzipPath, _appSettings.InitialAddressDataUnzipPath);
            await ProcessLatestAdresses(
                _appSettings.InitialAddressDataUnzipPath,
                _appSettings.InitialAddressDataProcessedPath,
                _appSettings.MinX,
                _appSettings.MinY,
                _appSettings.MaxX,
                _appSettings.MaxY);
        }

        public static async Task getLatestAdressData()
        {
            await client.getFileFtp(_appSettings.ftpServer, _appSettings.adressUserName, _appSettings.adressPassword, _appSettings.InitialAddressDataUnzipPath);
            client.UnzipFile(_appSettings.InitialAddressDataUnzipPath, _appSettings.InitialAddressDataUnzipPath);
            await ProcessLatestAdresses(
                _appSettings.InitialAddressDataUnzipPath,
                _appSettings.InitialAddressDataProcessedPath,
                _appSettings.MinX,
                _appSettings.MinY,
                _appSettings.MaxX,
                _appSettings.MaxY);
        }

        public static async Task getLatestGeoData()
        {
            await client.getFileFtp(_appSettings.ftpServer, _appSettings.geoUserName, _appSettings.geoPassword, _appSettings.geoUnzipPath);
            client.UnzipFile(_appSettings.geoUnzipPath, _appSettings.geoGmlPath);
            convertToGeojson(_appSettings.geoFieldList);
            ProcessGeoDirectory(_appSettings.geoUnzipPath,
             _appSettings.geoProcessedPath,
             _appSettings.geoFieldList,
             _appSettings.MinX,
             _appSettings.MinY,
             _appSettings.MaxX,
             _appSettings.MaxY);
        }

        public static void ProcessGeoDirectory(string sourceDirectory, string destinationDirectory, List<String> geoFilter, double minX, double minY, double maxX, double maxY)
        {
            List<String> fileEntries = Directory.GetFiles(sourceDirectory).ToList();
            List<String> filtered = new List<String>();
            var result = fileEntries.Where(a => geoFilter.Any(b => a.Contains(b))).ToList();

            foreach (string fileName in result)
            {
                Console.WriteLine(fileName);
                var fileNoExtension = Path.GetFileNameWithoutExtension(fileName);
                var dest = Path.Combine(destinationDirectory, fileNoExtension + ".json");
                filterGeoPosition(fileName, minX, maxX, minY, maxY);
                File.Move(fileName, dest);
            }
        }

        public static void convertToGeojson(List<string> list)
        {
            foreach (var item in list)
            {
                ProcessStartInfo startInfo = new ProcessStartInfo()
                {
                    FileName = @"/home/mehigh/confluentKafka/convert_script.sh",

                    Arguments = item
                };
                Process proc = new Process()
                {
                    StartInfo = startInfo,
                };
                proc.Start();
                proc.WaitForExit();
            }
        }

        public static async Task ProcessLatestAdresses(string sourceDirectory, string destinationDirectory, double minX, double minY, double maxX, double maxY)
        {
            DirectoryInfo destinfo = new DirectoryInfo(destinationDirectory);
            if (destinfo.Exists == false)
            {
                Directory.CreateDirectory(destinationDirectory);
            }
            DirectoryInfo sourceinfo = new DirectoryInfo(sourceDirectory);
            DirectoryInfo[] dirs = sourceinfo.GetDirectories();

            List<String> fileEntries = Directory.GetFiles(sourceDirectory).ToList();
            foreach (string filename in fileEntries)
            {
                if (!filename.Contains("Metadata"))
                {
                    await AdressToKafka(filename, minX, minY, maxX, maxY);
                    string file = Path.GetFileName(filename);
                    string destFile = Path.Combine(destinationDirectory, file);
                    File.Move(filename, destFile);
                    Console.WriteLine("File moved in new directory");
                }
                else
                {
                    string file = Path.GetFileName(filename);
                    string destFile = Path.Combine(destinationDirectory, file);
                    File.Move(filename, destFile);
                }
            }
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
                    return result.ToString("yyyy-MM-dd HH:mm:ss");
                }
                catch (ArgumentNullException)
                {
                    Console.WriteLine("{0} is not in the correct format.", dateString);
                }
            }
            return String.Empty;
        }

        public static async Task AdressToKafka(string filename, double minX, double minY, double maxX, double maxY)
        {
            var hussnummerBatch = new List<string>();
            var adresspunktBatch = new List<string>();
            var newHussnummerBatch = new List<string>();
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

                            jsonText.Add(ChangeAdressNames(obj));

                            if (jsonText.Count >= 100000)
                            {
                                if (listName.Equals("AdressepunktList"))
                                {
                                    var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                                    foreach (var b in boundingBatch)
                                    {
                                        adresspunktBatch.Add(b);
                                    }

                                    jsonText.Clear();
                                }
                                else if (listName.Equals("HusnummerList"))
                                {

                                    foreach (var ob in jsonText)
                                    {
                                        hussnummerBatch.Add(ob);
                                    }
                                    jsonText.Clear();
                                    foreach (var hus in hussnummerBatch)
                                    {
                                        foreach (var adress in adresspunktBatch)
                                        {
                                            JObject ohus = JObject.Parse(hus);
                                            JObject oadress = JObject.Parse(adress);
                                            if (ohus["addressPoint"].Equals(oadress["id_lokalId"]))
                                            {
                                                ohus["position"] = oadress["position"];
                                                //Console.WriteLine(o1.ToString());
                                                var newhus = JsonConvert.SerializeObject(ohus, Formatting.Indented);
                                                newHussnummerBatch.Add(newhus);
                                            }

                                        }
                                    }
                                    KafkaProducer(listName, newHussnummerBatch);
                                    adresspunktBatch.Clear();
                                    hussnummerBatch.Clear();
                                    newHussnummerBatch.Clear();
                                }
                                else if (listName.Equals("NavngivenVejList"))
                                {
                                    var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                                    KafkaProducer(listName, boundingBatch);
                                    boundingBatch.Clear();
                                    jsonText.Clear();
                                    Console.WriteLine("Wrote 100000 objects into topic");
                                }
                                else
                                {
                                    KafkaProducer(listName, jsonText);
                                    jsonText.Clear();
                                    Console.WriteLine("Wrote 100000 objects into topic");
                                }
                            }
                        }
                    }

                    if (listName.Equals("AdressepunktList"))
                    {
                        var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                        foreach (var b in boundingBatch)
                        {
                            adresspunktBatch.Add(b);
                        }

                        KafkaProducer(listName, boundingBatch);
                        boundingBatch.Clear();
                        jsonText.Clear();
                        Console.WriteLine("This is the adresspunkt inside loop " + adresspunktBatch.Count);
                    }
                    else if (listName.Equals("HusnummerList"))
                    {
                        foreach (var ob in jsonText)
                        {
                            hussnummerBatch.Add(ob);
                        }
                        jsonText.Clear();
                        Console.WriteLine("This is the hussnummer inside loop " + hussnummerBatch.Count);
                        foreach (var hus in hussnummerBatch)
                        {
                            foreach (var adress in adresspunktBatch)
                            {
                                JObject ohus = JObject.Parse(hus);
                                JObject oadress = JObject.Parse(adress);
                                if (ohus["addressPoint"].Equals(oadress["id_lokalId"]))
                                {
                                    ohus["position"] = oadress["position"];
                                    //Console.WriteLine(o1.ToString());
                                    var newhus = JsonConvert.SerializeObject(ohus, Formatting.Indented);
                                    newHussnummerBatch.Add(newhus);
                                }
                            }
                        }

                        KafkaProducer(listName, newHussnummerBatch);
                        adresspunktBatch.Clear();
                        hussnummerBatch.Clear();
                        newHussnummerBatch.Clear();
                    }
                    else if (listName.Equals("NavngivenVejList"))
                    {
                        var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                        KafkaProducer(listName, boundingBatch);

                        boundingBatch.Clear();
                        jsonText.Clear();
                        Console.WriteLine("Wrote 100000 objects into topic");
                    }
                    else
                    {
                        KafkaProducer(listName, jsonText);

                        jsonText.Clear();
                        Console.WriteLine("Wrote 100000 objects into topic");
                    }
                }
            }
        }


        public static void filterGeoPosition(String fileName, double minX, double maxX, double minY, double maxY)
        {
            String jsonDoc = "";
            List<string> batch = new List<string>();
            var boundingBox = new NetTopologySuite.Geometries.Envelope(minX, maxX, minY, maxY);
            NetTopologySuite.Features.Feature feature = new NetTopologySuite.Features.Feature();

            using (FileStream s = File.Open(fileName, FileMode.Open))
            using (var streamReader = new StreamReader(s))
            {
                var file = Path.GetFileNameWithoutExtension(fileName).Split(".");
                var topicname = file[1];

                using (var jsonreader = new Newtonsoft.Json.JsonTextReader(streamReader))
                {
                    while (jsonreader.Read())
                    {
                        var reader = new NetTopologySuite.IO.GeoJsonReader();
                        //var feature = reader.Read<GeoJSON.Net.Feature.FeatureCollection>(jsonreader);
                        //var featurecollection = reader.Read<NetTopologySuite.Features.FeatureCollection>(jsonreader);
                        if (jsonreader.TokenType == Newtonsoft.Json.JsonToken.StartObject)
                        {
                            while (jsonreader.Read())
                            {
                                if (jsonreader.TokenType == Newtonsoft.Json.JsonToken.StartArray)
                                {
                                    while (jsonreader.Read())
                                    {
                                        try
                                        {
                                            if (jsonreader != null)
                                            {
                                                feature = reader.Read<NetTopologySuite.Features.Feature>(jsonreader);
                                            }

                                            var geo = feature.Geometry;
                                            var atr = feature.Attributes;
                                            if (boundingBox.Intersects(geo.EnvelopeInternal))
                                            {
                                                //Console.WriteLine("This is one object " + be.ToString());
                                                var jsonObj = new
                                                {
                                                    gml_id = atr.GetOptionalValue("gml_id"),
                                                    id_lokalId = atr.GetOptionalValue("id_lokalid"),
                                                    geo = geo.ToString()
                                                };
                                                jsonDoc = JsonConvert.SerializeObject(jsonObj);
                                                //Console.WriteLine(jsonDoc);
                                                //Console.WriteLine(jsonDoc);
                                                batch.Add(jsonDoc);
                                                if (batch.Count >= 5000)
                                                {
                                                    KafkaProducerGeo(topicname, batch);
                                                    batch.Clear();
                                                }
                                            }
                                        }
                                        //Loop gives reader exception when it reaches the last element from the file
                                        catch (Newtonsoft.Json.JsonReaderException e)
                                        {
                                            Console.WriteLine("Error writing data: {0}.", e.GetType().Name);
                                            var geo = feature.Geometry;
                                            var atr = feature.Attributes;
                                            var jsonObj = new
                                            {
                                                gml_id = atr.GetOptionalValue("gml_id"),
                                                id_lokalId = atr.GetOptionalValue("id_lokalid"),
                                                geo = geo.ToString()
                                            };
                                            jsonDoc = JsonConvert.SerializeObject(jsonObj);
                                            batch.Add(jsonDoc);
                                            KafkaProducerGeo(topicname, batch);
                                            batch.Clear();
                                            Console.WriteLine("Document has been added");
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        if (batch != null)
                        {
                            KafkaProducerGeo(topicname, batch);
                            batch.Clear();
                        }
                    }
                }
            }
        }

        public static void KafkaProducer(String topicname, List<string> batch)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            int i = 0;
            using (var p = new ProducerBuilder<string, string>(config).Build())
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
                            id = (string)jp.Value;
                        }
                    }

                    try
                    {
                        p.Produce(topicname, new Message<string, string> { Value = document, Key = id });
                    }
                    catch (ProduceException<Null, string> e)
                    {
                        Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                    }

                }
                p.Flush(TimeSpan.FromSeconds(10));
                Console.WriteLine(i);
            }
        }


        public static void KafkaProducerGeo(String topicname, List<string> batch)
        {
            var config = new ProducerConfig { BootstrapServers = "localhost:9092", LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            int i = 0;
            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine(topicname);
                Console.WriteLine(batch.Count());
                i += 10000;

                if (batch.Count > 0)
                {
                    foreach (var document in batch)
                    {
                        String id = String.Empty;

                        JObject o = JObject.Parse(document);
                        //Console.Write(o);
                        foreach (JProperty jp in o.Properties().ToList())
                        {
                            if (jp.Name == "id_lokalId")
                            {
                                id = (String)jp.Value;
                            }
                        }
                        try
                        {
                            p.Produce(topicname, new Message<string, string> { Value = document, Key = id });
                        }
                        catch (ProduceException<Null, string> e)
                        {
                            Console.WriteLine($"Delivery failed: {e.Error.Reason}");
                        }

                        p.Flush(TimeSpan.FromSeconds(10));
                    }
                }
                Console.WriteLine(i);
            }
        }

        public static List<string> newfilterAdressPosition(List<string> batch, double minX, double minY, double maxX, double maxY)
        {

            List<string> filteredBatch = new List<string>();
            GeometryFactory geometryFactory = new GeometryFactory();
            Geometry line;
            Geometry point;
            Geometry polygon;
            Geometry multipoint;
            string name;
            string value = "";
            WKTReader rdr = new WKTReader(geometryFactory);
            var boundingBox = new NetTopologySuite.Geometries.Envelope(minX, maxX, minY, maxY);
            foreach (var document in batch)
            {
                JObject o = JObject.Parse(document);
                foreach (JProperty jp in o.Properties().ToList())
                {
                    if (jp.Name == "position")
                    {
                        point = rdr.Read(jp.Value.ToString());
                        if (boundingBox.Intersects(point.EnvelopeInternal))
                        {
                            filteredBatch.Add(document);
                        }
                    }
                    else if (jp.Name == "roadRegistrationRoadLine")
                    {
                        try
                        {
                            if (jp.Value != null)
                            {
                                line = rdr.Read(jp.Value.ToString());
                                if (boundingBox.Intersects(line.EnvelopeInternal))
                                {
                                    filteredBatch.Add(document);
                                }
                            }
                            else
                            {
                                if (jp.Name == "roadRegistrationRoadArea")
                                {
                                    name = jp.Name;
                                    value = (string)jp.Value;
                                    polygon = rdr.Read(jp.Value.ToString());

                                    if (boundingBox.Intersects(polygon.EnvelopeInternal))
                                    {
                                        filteredBatch.Add(document);
                                    }
                                }

                                else if (jp.Name == "roadRegistrationRoadConnectionPoints")
                                {
                                    multipoint = rdr.Read(jp.Value.ToString());

                                    if (boundingBox.Intersects(multipoint.EnvelopeInternal))
                                    {
                                        filteredBatch.Add(document);
                                    }
                                }
                            }
                        }

                        /* Gets parse exception when they are values in both roadRegistrationRoadConnectionPoints and roadRegistrationArea,
                           also when they are null values in those fields
                        */
                        catch (NetTopologySuite.IO.ParseException e)
                        {
                            Console.WriteLine("Error writing data: {0}.", e.GetType().Name);
                            Console.WriteLine(document);
                            JObject obj = JObject.Parse(document);
                            if (String.IsNullOrEmpty(obj["roadRegistrationRoadArea"].ToString()) == false & (String.IsNullOrEmpty(obj["roadRegistrationRoadConnectionPoints"].ToString()) == false))
                            {
                                polygon = rdr.Read((string)obj["roadRegistrationRoadArea"]);
                                if (boundingBox.Intersects(polygon.EnvelopeInternal))
                                {
                                    filteredBatch.Add(document);
                                    Console.WriteLine("document added");
                                }
                            }
                            break;
                        }
                    }
                }
            }
            Console.WriteLine(filteredBatch.Count);
            return filteredBatch;
        }

        public static string ChangeAdressNames(JObject jo)
        {
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
                        jp.Replace(new JProperty("roadRegistrationRoadArea", jp.Value));
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
                        break;
                }
            }

            var obj = JsonConvert.SerializeObject(jo, Formatting.Indented);
            return obj;
        }
    }
}
