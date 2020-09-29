using System;
using System.IO;
using System.Linq;
using System.Threading.Tasks;
using System.Collections.Generic;
using Newtonsoft.Json;
using Datafordelen.Config;
using Datafordelen.Kafka;
using System.Diagnostics;

namespace Datafordelen.GeoData
{
    public class GeoDataService
    {
        private readonly AppSettings _appSettings;
        private readonly FTPClient _client;
        private readonly KafkaProducer _producer;

        public GeoDataService(AppSettings appSettings)
        {
            _appSettings = appSettings;
            _client = new FTPClient();
            _producer = new KafkaProducer();
        }

        public async Task getLatestGeoData()
        {
            await _client.getFileFtp(_appSettings.ftpServer, _appSettings.geoUserName, _appSettings.geoPassword, _appSettings.geoUnzipPath);
            _client.UnzipFile(_appSettings.geoUnzipPath, _appSettings.geoGmlPath);
            convertToGeojson(_appSettings.geoFieldList);
            Console.WriteLine(_appSettings.geoUnzipPath);
            Console.WriteLine(_appSettings.geoFieldList);
            ProcessGeoDirectory(_appSettings.geoUnzipPath,
             _appSettings.geoProcessedPath,
             _appSettings.geoFieldList,
             _appSettings.MinX,
             _appSettings.MinY,
             _appSettings.MaxX,
             _appSettings.MaxY);
        }

        private void ProcessGeoDirectory(string sourceDirectory, string destinationDirectory, List<String> geoFilter, double minX, double minY, double maxX, double maxY)
        {
            var fileEntries = Directory.GetFiles(sourceDirectory).ToList();
            var filtered = new List<String>();
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

        private void convertToGeojson(List<string> list)
        {
            foreach (var item in list)
            {
                var startInfo = new ProcessStartInfo()
                {
                    FileName = @"/home/mehigh/confluentKafka/convert_script.sh",

                    Arguments = item
                };

                var proc = new Process()
                {
                    StartInfo = startInfo,
                };

                proc.Start();
                proc.WaitForExit();
            }
        }

        private void filterGeoPosition(String fileName, double minX, double maxX, double minY, double maxY)
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
                                                    _producer.Produce(topicname, batch);
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
                                            _producer.Produce(topicname, batch);
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
                            _producer.Produce(topicname, batch);
                            batch.Clear();
                        }
                    }
                }
            }
        }
    }
}
