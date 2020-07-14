create table houses (id_lokalId varchar,accessAddressDescription varchar, houseNumberText varchar, addressPoint varchar, namedRoad varchar, postalCode varchar, geoDenmarkBuilding varchar ) with (kafka_topic='HusnummerList',value_format='json',key='addressPoint', partitions=1,replicas=1);

create table addresspunkt(id_lokalId varchar, position varchar) with (kafka_topic='AdressepunktList',value_format='json',key='id_lokalId', partitions=1,replicas=1);

create table accessaddress with (value_format='avro') as select h.id_lokalId, h.houseNumberText, h.accessAddressDescription, h.namedRoad, h.postalCode,h.geoDenmarkBuilding, a.position from houses h  inner join addresspunkt a on h.rowkey = a.rowkey;

create table unitaddress (id_lokalId varchar, unitAddressDescription varchar, door varchar, floor varchar, houseNumber varchar) with (kafka_topic='AdresseList',value_format='json',key='id_lokalId',partitions=1,replicas=1);

create table adresseList with (value_format='avro') as select * from unitaddress;

create table road (id_lokalId varchar, roadName varchar, municipalityAdministration varchar, roadRegistrationRoadLine varchar) with (kafka_topic='NavngivenVejList', value_format='json',key='id_lokalId',partitions=1,replicas=1);

create table roadnames with (value_format='avro') as select * from road;

create table roadcity (id_lokalId varchar, municipality varchar,roadcode varchar, namedRoad varchar) with (kafka_topic='NavngivenVejKommunedelList',value_format='json',key='id_lokalId',partitions=1,replicas=1);

create table roadkommune with (value_format='avro') as select * from roadcity;

create table postal(id_lokalId varchar, navn varchar, postnr varchar, postalCodeDistrict varchar) with (value_format='json', kafka_topic='PostnummerList',key='id_lokalId',partitions=1,replicas=1);

create table postalarea with (value_format='avro') as select * from postal;

create table bygning("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='bygning',value_format='json',partitions=1,replicas=1);

create table building with (value_format='avro') as select * from bygning;

create table chikane("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='chikane',value_format='json',partitions=1,replicas=1);

create table chicane with (value_format='avro') as select * from chikane;

create table bygvaerk("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='bygvaerk',value_format='json',partitions=1,replicas=1);

create table construction with (value_format='avro') as select * from bygvaerk;

create table trae("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='trae',value_format='json',partitions=1,replicas=1);

create table tree with (value_format='avro') as select * from trae;

create table erhverv("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='erhverv',value_format='json',partitions=1,replicas=1);

create table commercial with (value_format='avro') as select * from erhverv;

create table systemlinje("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='systemlinje',value_format='json',partitions=1,replicas=1);

create table systemline with (value_format='avro') as select * from systemlinje;

create table vejkant("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='vejkant',value_format='json',partitions=1,replicas=1);

create table roadedge with (value_format='avro') as select * from vejkant;

create table vejmidte("properties" struct<gml_id varchar, id_lokalid varchar>, geometry varchar) with (kafka_topic='vejmidte',value_format='json',partitions=1,replicas=1);

create table roadmid with (value_format='avro') as select * from vejmidte;

