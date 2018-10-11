create table gridareas (
    the_geom public.geometry(Geometry,4326),
    projparams text,
    gridid character varying primary key,
    is_reference boolean,    
    closest_geom_id integer,
    hausdorffdistance_to_closest_geom float
);
create table gribfiles (
 file varchar primary key
);
create table measurement (
 measurementid varchar primary key,
 parameterName varchar,
 parameterUnit varchar,
 typeOfLevel varchar,
 level integer
);
create table griblayers(
  file varchar references gribfiles(file),
  measurementid varchar references measurement(measurementid),
  analdate timestamp,
  validdate timestamp,
  layeridx integer,
  gridid varchar references gridareas(gridid));
create index on griblayers(validdate);
