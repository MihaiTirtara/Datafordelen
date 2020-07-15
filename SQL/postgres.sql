/* Create  the extension for postgis */ 
Create extension postgis;


/* Create address tables */
create table "ACCESSADDRESS" ("MESSAGE_KEY" text PRIMARY KEY ,"H_ID_LOKALID" text, "ACCESSADDRESSDESCRIPTION" text, "HOUSENUMBERTEXT" text, "NAMEDROAD" text, "POSTALCODE" text, "GEODENMARKBUILDING" text, "POSITION" text, "GEO" geometry);
create table "ADRESSELIST" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "UNITADDRESSDESCRIPTION" text, "DOOR" text, "FLOOR" text,"HOUSENUMBER" text);
create table "ROADNAMES" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "ROADNAME" text, "MUNICIPALITYADMINISTRATION" text, "ROADREGISTRATIONROADLINE" text, "GEO" geometry);
CREATE TABLE "ROADKOMMUNE" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text , "MUNICIPALITY" text,"ROADCODE" text, "NAMEDROAD" text);
CREATE TABLE "POSTALAREA"("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "NAVN" text, "POSTNR" text, "POSTALCODEDISTRICT" text);

/* Create spatial index for adress tables */
CREATE INDEX access_address_index
    ON public."ACCESSADDRESS" USING gist ("GEO");
CREATE INDEX roadnames_index
    ON public."ROADNAMES" USING gist ("GEO");	

/* Create geo tables */ 


Create table "BUILDING"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "CHICANE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "CONSTRUCTION"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "TREE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "COMMERCIAL"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "SYSTEMLINE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "ROADEDGE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "ROADMID"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

/* Create spatial index for geographical data tables */
CREATE INDEX building_index
    ON public."BUILDING" USING gist ("GEO");
CREATE INDEX chicane_index
    ON public."CHICANE" USING gist ("GEO");
CREATE INDEX construction_index
    ON public."CONSTRUCTION" USING gist ("GEO");
CREATE INDEX tree_index
    ON public."TREE" USING gist ("GEO");
CREATE INDEX commercial_index
    ON public."COMMERCIAL" USING gist ("GEO");
CREATE INDEX systemline_index
    ON public."SYSTEMLINE" USING gist ("GEO");
CREATE INDEX roadedge_index
    ON public."ROADEDGE" USING gist ("GEO");
CREATE INDEX roadmid_index
    ON public."ROADMID" USING gist ("GEO");



/* Create triggers to convert from wkt to geometry */


create or replace function update_adress_positioning_column()
  returns trigger
as
$$
begin
  new."GEO" := ST_GeomFromText(new."POSITION");
  return new;
end;
$$
language plpgsql;

create trigger geo_trigger 
   before insert or update on "ACCESSADDRESS"
   for each row
   execute procedure update_adress_positioning_column();
   
create or replace function update_adress_roadline_column()
  returns trigger
as
$$
begin
  new."GEO" := ST_GeomFromText(new."ROADREGISTRATIONROADLINE");
  return new;
end;
$$
language plpgsql;

create trigger road_trigger 
   before insert or update on "ROADNAMES"
   for each row
   execute procedure update_adress_roadline_column();   
   
      
create or replace function update_geo_column()
  returns trigger
as
$$
begin
  new."GEO" := ST_GeomFromGeoJSON(new."GEOMETRY");
  return new;
end;
$$
language plpgsql;

create trigger geobuilding_trigger 
   before insert or update on "BUILDING"
   for each row
   execute procedure update_geo_column();
   
create trigger geochicane_trigger 
   before insert or update on "CHICANE"
   for each row
   execute procedure update_geo_column();
   
   create trigger geoconstrcution_trigger 
   before insert or update on "CONSTRUCTION"
   for each row
   execute procedure update_geo_column();
   
   create trigger geotree_trigger 
   before insert or update on "TREE"
   for each row
   execute procedure update_geo_column();
   
   create trigger geocommercial_trigger 
   before insert or update on "COMMERCIAL"
   for each row
   execute procedure update_geo_column();
      
   create trigger geosystemline_trigger 
   before insert or update on "SYSTEMLINE"
   for each row
   execute procedure update_geo_column();
   
   create trigger georoadedge_trigger 
   before insert or update on "ROADEDGE"
   for each row
   execute procedure update_geo_column();
   
   create trigger georoadmid_trigger 
   before insert or update on "ROADMID"
   for each row
   execute procedure update_geo_column();
   
   
   					  
						  
