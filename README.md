# Datafordelen
This is an event streaming application which retrieves the data using the Danish API called Datafordeler and in order to process the information it is using Kafka,KsqlDb and Postgres.
In order to get acces of the necessary data an account on their website must first be created, this can be found at the following link:https://selfservice.datafordeler.dk/
The system uses two of their registers:
1. DAR, which holds the latest data for the adresses in Denmark 
2. GeoDanmarkVektor which holds the geographical data of Denmark.
<br>
More informations about the register structure and how to subscribe to them can be found on their website:https://datafordeler.dk/

---
The application has multiple functionalities which are separated into different methods.
1. Add the initial adress data  
#
        public static async Task getinitialAdressData()
        {
            client.getAdressInitialLoad("https://selfservice.datafordeler.dk/filedownloads/626/334",@"/home/mehigh/ftptrials/adress.zip");
            client.UnzipFile(@"/home/mehigh/ftptrials/",@"/home/mehigh/ftptrials/");
            await ProcessLatestAdresses("/home/mehigh/ftptrials/","/home/mehigh/newftp");

        }        
This method uses a web client to first retrieve a zip file with the data from a public atom feed, after that the data is unzipped and the method "ProcessLatestAdresses" goes through the files and adds them into a kafka topic. The producer can be observed below.
#

`

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

Furthermore before adding the adresses into a topic the fields are translated from danish to english using the following method:  
#


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
                        
2. Add the geographic data 
#
     public static async Task getLatestGeoData()
        {
            await client.getFileFtp("ftp3.datafordeler.dk","PCVZLGPTJE","sWRbn2M8y2tH!","/home/mehigh/geo/");
            client.UnzipFile(@"/home/mehigh/geo/",@"/home/mehigh/geo/geogml");
            convertToGeojson();
            ProcessGeoDirectory(@"/home/mehigh/geo/",@"/home/mehigh/newgeo/", new List<string>(){"trae","bygning,chikane,bygvaerk,erhverv,systemlinje,vejkant,vejmidte"});
        }

In the case of the geographic data, first the files have to be download from an ftp server using the account created on the datafordeler website. The files are in the GML format but the system uses GeoJson for that reason the files have to be converted. Afterwards the data are added into the specific Kafka topic, furthermore you also have the option to pass a string array to select only the data that you are interested in. 
#
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
                Directory.Move(fileName,dest);
            }
            
        }
3.  Add the latest adress data  
Similarly to the previous steps to get the latest adress data we first dowload the zip file from an ftp server and then unzip  it and add it into the specific Kafka topic.  
#
    public static async Task getLatestAdressData()
        {
            await client.getFileFtp("ftp3.datafordeler.dk","JFOWRLSDKM","sWRbn2M8y2tH!","/home/mehigh/ftptrials/");
            client.UnzipFile(@"/home/mehigh/ftptrials/",@"/home/mehigh/ftptrials/");
            await ProcessLatestAdresses("/home/mehigh/ftptrials/","/home/mehigh/newftp");
        }
        
        



 The system is ran from a docker container which contains all the necessary instruments such as Kafka,Ksql,Postgres. For proccessing the data, Ksql use a similar syntax to any other SQL-based database being able to create tables or streams and even join them for example:
#

    create table accessaddress with (value_format='avro') as select h.id_lokalId, h.houseNumberText, h.accessAddressDescription, h.namedRoad,      h.postalCode,h.geoDenmarkBuilding, a.position from houses h  inner join addresspunkt a on h.rowkey = a.rowkey;
    
    create table trae("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='trae',value_format='json',partitions=1,replicas=1);

    create table tree with (value_format='avro') as select * from trae;

The queries can be written in the interactive console or in a SQL file and then created using the following command in Ksql:
#
    run script '/etc/ksql-queries/ksql.sql'
    

After processing the data into ksql the data is added into a relational database using the Postgres sink connector, creating for each topic a table with the latest data, moreover it can also flatten structure such as struct:
#
      {"name": "postgres-sink-address",
     "config": {"connector.class":"io.confluent.connect.jdbc.JdbcSinkConnector",
      "tasks.max":"1",
      "topics": "ACCESSADDRESS,ADRESSELIST,ROADNAMES,ROADKOMMUNE,POSTALAREA,BUILDING,CHICANE,CONSTRUCTION,TREE,COMMERCIAL,SYSTEMLINE,ROADEDGE,ROADMID",
       "key.converter": "org.apache.kafka.connect.storage.StringConverter",
       "value.converter": "io.confluent.connect.avro.AvroConverter",
       "value.converter.schema.registry.url": "http://schema-registry:8081",
       "connection.url": "jdbc:postgresql://postgres:5432/postgres?user=postgres&password=postgres",
       "key.converter.schemas.enable": "false",
       "value.converter.schemas.enable": "true",
       "auto.create": "true",
       "auto.evolve": "true",
       "insert.mode": "upsert",
       "transforms": "flatten",
       "transforms.flatten.type": "org.apache.kafka.connect.transforms.Flatten$Value",
       "pk.fields": "MESSAGE_KEY",
       "pk.mode": "record_key"
      }
    }
