using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Datafordelen.Config;
using Datafordelen.Kafka;
using Datafordelen.Ftp;
using NetTopologySuite.Geometries;
using NetTopologySuite.IO;

namespace Datafordelen.Address
{
    public class AddressService
    {
        private readonly AppSettings _appSettings;
        private FTPClient _client;
        private KafkaProducer _kafkaProducer;

        public AddressService(AppSettings appSettings)
        {
            _appSettings = appSettings;
            _client = new FTPClient();
            _kafkaProducer = new KafkaProducer();
        }

        public async Task GetinitialAddressData()
        {
            Console.WriteLine("this is the uri" + _appSettings.InitialAddressDataUrl);
            _client.GetAddressInitialLoad(_appSettings.InitialAddressDataUrl, _appSettings.InitialAddressDataZipFilePath);
            _client.UnzipFile(_appSettings.InitialAddressDataUnzipPath, _appSettings.InitialAddressDataUnzipPath);
            await ProcessLatestAdresses(
                _appSettings.InitialAddressDataUnzipPath,
                _appSettings.InitialAddressDataProcessedPath,
                _appSettings.MinX,
                _appSettings.MinY,
                _appSettings.MaxX,
                _appSettings.MaxY);
        }

        public async Task GetLatestAddressData()
        {
            await _client.GetFileFtp(_appSettings.ftpServer, _appSettings.adressUserName, _appSettings.adressPassword, _appSettings.InitialAddressDataUnzipPath);
            _client.UnzipFile(_appSettings.InitialAddressDataUnzipPath, _appSettings.InitialAddressDataUnzipPath);
            await ProcessLatestAdresses(
                _appSettings.InitialAddressDataUnzipPath,
                _appSettings.InitialAddressDataProcessedPath,
                _appSettings.MinX,
                _appSettings.MinY,
                _appSettings.MaxX,
                _appSettings.MaxY);
        }

        private async Task ProcessLatestAdresses(string sourceDirectory, string destinationDirectory, double minX, double minY, double maxX, double maxY)
        {
            var destinfo = new DirectoryInfo(destinationDirectory);
            if (destinfo.Exists == false)
            {
                Directory.CreateDirectory(destinationDirectory);
            }
            var sourceinfo = new DirectoryInfo(sourceDirectory);
            var dirs = sourceinfo.GetDirectories();

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

        private async Task AdressToKafka(string filename, double minX, double minY, double maxX, double maxY)
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
                                    _kafkaProducer.Produce(listName, newHussnummerBatch);
                                    adresspunktBatch.Clear();
                                    hussnummerBatch.Clear();
                                    newHussnummerBatch.Clear();
                                }
                                else if (listName.Equals("NavngivenVejList"))
                                {
                                    var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                                    _kafkaProducer.Produce(listName, boundingBatch);
                                    boundingBatch.Clear();
                                    jsonText.Clear();
                                    Console.WriteLine("Wrote 100000 objects into topic");
                                }
                                else
                                {
                                    _kafkaProducer.Produce(listName, jsonText);
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

                        _kafkaProducer.Produce(listName, boundingBatch);
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

                        _kafkaProducer.Produce(listName, newHussnummerBatch);
                        adresspunktBatch.Clear();
                        hussnummerBatch.Clear();
                        newHussnummerBatch.Clear();
                    }
                    else if (listName.Equals("NavngivenVejList"))
                    {
                        var boundingBatch = newfilterAdressPosition(jsonText, minX, minY, maxX, maxY);
                        _kafkaProducer.Produce(listName, boundingBatch);

                        boundingBatch.Clear();
                        jsonText.Clear();
                        Console.WriteLine("Wrote 100000 objects into topic");
                    }
                    else
                    {
                        _kafkaProducer.Produce(listName, jsonText);

                        jsonText.Clear();
                        Console.WriteLine("Wrote 100000 objects into topic");
                    }
                }
            }
        }

        private List<string> newfilterAdressPosition(List<string> batch, double minX, double minY, double maxX, double maxY)
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

        private string ChangeAdressNames(JObject jo)
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
    }
}
