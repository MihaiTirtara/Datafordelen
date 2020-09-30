using Confluent.Kafka;
using System;
using System.Linq;
using System.Collections.Generic;
using Newtonsoft.Json.Linq;
using Datafordelen.Config;

namespace Datafordelen.Kafka
{
    public class KafkaProducer:IKakfkaProducer
    {
        private readonly AppSettings _appSettings;

        public KafkaProducer(AppSettings appSettings)
        {
            _appSettings = appSettings;
        }

        public void Produce(string topicname, List<string> batch)
        {
            var config = new ProducerConfig { BootstrapServers = _appSettings.KafkaBootstrapServer, LingerMs = 5, BatchNumMessages = 100000, QueueBufferingMaxMessages = 100000 };
            var i = 0;

            using (var p = new ProducerBuilder<string, string>(config).Build())
            {
                Console.WriteLine(batch.Count());
                i += 10000;
                foreach (var document in batch)
                {
                    var id = String.Empty;
                    var o = JObject.Parse(document);

                    if (o["gml_id"] != null)
                    {
                        id = (string)o["gml_id"];
                    }
                    else
                    {
                        id = (string)o["id_lokalId"];
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
    }
}
