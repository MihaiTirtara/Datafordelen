/* Create  the extension for postgis */ 
Create extension postgis;


/* Create address tables */
create table "ACCESSADDRESS" ("MESSAGE_KEY" text PRIMARY KEY ,"H_ID_LOKALID" text, "ACCESSADDRESSDESCRIPTION" text, "HOUSENUMBERTEXT" text, "NAMEDROAD" text, "POSTALCODE" text, "GEODENMARKBUILDING" text, "POSITION" text, "GEO" geometry);
create table "ADRESSELIST" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "UNITADDRESSDESCRIPTION" text, "DOOR" text, "FLOOR" text,"HOUSENUMBER" text);
create table "ROADNAMES" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "ROADNAME" text, "MUNICIPALITYADMINISTRATION" text, "ROADREGISTRATIONROADLINE" text, "GEO" geometry);
CREATE TABLE "ROADKOMMUNE" ("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text , "MUNICIPALITY" text,"ROADCODE" text, "NAMEDROAD" text);
CREATE TABLE "POSTALAREA"("MESSAGE_KEY" text PRIMARY KEY ,"ID_LOKALID" text, "NAVN" text, "POSTNR" text, "POSTALCODEDISTRICT" text);	

/* Create geo tables */ 


Create table "BUILDING"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "CHICANE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "CONSTRUCTION"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "TREE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "COMMERCIAL"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "SYSTEMLINE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "ROADEDGE"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

Create table "ROADMID"("MESSAGE_KEY" text PRIMARY KEY ,"properties.GML_ID" text , "properties.ID_LOKALID" text ,"GEOMETRY" text, "GEO" geometry);

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
   
   
   					  
						  
