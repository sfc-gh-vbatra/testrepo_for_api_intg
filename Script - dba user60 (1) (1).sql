--UPDATED 2020-09-15
----------------------------------------------------------------------------------------
-----DBA PART I
----------------------------------------------------------------------------------------

-- --3.1.2 Set your context to use your DBA role and assigned SCHEMA
use role dba60;
use schema citibike.schema60;


--2.1.4 Create a table called trips to hold our Citibike data
create or replace table trips
(tripduration integer,
  starttime timestamp,
  stoptime timestamp,
  start_station_id integer,
  start_station_name string,
  start_station_latitude float,
  start_station_longitude float,
  end_station_id integer,
  end_station_name string,
  end_station_latitude float,
  end_station_longitude float,
  bikeid integer,
  membership_type string,
  usertype string,
  birth_year integer,
  gender integer);

--2.2.3 Create the stage
--2.2.3 Create the stage
create or replace stage trip_data_s3
  url = 's3://snowflake-workshop-lab/citibike-trips-csv/';

--2.2.4 Have a look at the staged data
ls @trip_data_s3;

--2.3.1 Create a file format that matches our CSV data (OR RUN VIA UI)
create or replace file format csv type='csv'
compression = 'auto' field_delimiter = ',' record_delimiter = '\n'
skip_header = 0 field_optionally_enclosed_by = '\042' trim_space = false
error_on_column_count_mismatch = false escape = 'none' escape_unenclosed_field = '\134'
date_format = 'auto' timestamp_format = 'auto' null_if = ('');


--3.1.2 Create a Warehouse to load our data (OR RUN VIA UI)
create or replace warehouse load_wh60
with warehouse_size = 'medium'
auto_suspend = 300
auto_resume = true
min_cluster_count = 1
max_cluster_count = 1
scaling_policy = 'standard';


--3.2.1 Adjust your context to use your new warehouse
use warehouse load_wh60;
use database citibike;
use schema schema60;


--3.2.2 Load some of the data!
copy into trips
from @trip_data_s3/
pattern='.*2018.*csv[.]gz'
file_format=csv;


--3.2.3 Let’s scale our compute UP by increasing our Warehouse size to X-Large:
alter warehouse load_wh60 set warehouse_size='xlarge';

--3.2.4 Then load the rest of the data:
copy into trips
from @trip_data_s3/
file_format=csv;
--pattern='.*csv[.]gz';


--3.2.5 Now that the data is loaded, we can run simple SQL
--Check the number of rows loaded
select count(*) from trips;


-- And a sample of the data
select * from trips limit 20;


--4.2.2 Grant your ANALYST user access to the data
grant usage on database citibike to role analyst60;
grant usage on schema citibike.schema60 to role analyst60;
grant select on table citibike.schema60.trips to role analyst60;



----------------------------------------------------------------------------------------
-----DBA PART II
----------------------------------------------------------------------------------------

--SECTION 1: CLONE THE SCHEMA
--3.2.1 Create a dev schema by cloning schema
create schema schema60_dev clone schema60;

--SECTION 2: QUERY JSON
--2.2.1 Check out how we can query JSON data with SQL, as if it were a structured table!
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.UNCLUSTERED.WEATHER
where city_id = 5128638 limit 20;



--2.3.1 Let's first create a view in schema1_dev
create or replace view citibike.schema60_dev.weather_vw as
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.UNCLUSTERED.WEATHER
where city_id = 5128638;


--2.3.2 Verify the data before pushing to PROD schema
select * from citibike.schema60_dev.weather_vw
limit 20;


--2.3.3 All good, create the view in PROD!
create or replace view citibike.schema60.weather_vw as
select
  dateadd('year',-2,v:time::timestamp) as observation_time,
  v:city.id::int as city_id,
  v:city.name::string as city_name,
  v:city.country::string as country,
  v:city.coord.lat::float as city_lat,
  v:city.coord.lon::float as city_lon,
  v:clouds.all::int as clouds,
  (v:main.temp::float)-273.15 as temp_avg,
  (v:main.temp_min::float)-273.15 as temp_min,
  (v:main.temp_max::float)-273.15 as temp_max,
  v:weather[0].main::string as weather,
  v:weather[0].description::string as weather_desc,
  v:weather[0].icon::string as weather_icon,
  v:wind.deg::float as wind_dir,
  v:wind.speed::float as wind_speed
from WEATHER_DATA.UNCLUSTERED.WEATHER
where city_id = 5128638;

--2.3.4 Drop the DEV schema
drop schema schema60_dev;
use schema schema60;


-------------------------------------------------------------------------------
--SECTION 2.4: COMBINED VIEW
--2.4.1 Now that we have the weather_vw, let's combine it with our trips table
create or replace view trip_weather_vw as
select * from trips
left outer join weather_vw
on date_trunc('hour', observation_time) = date_trunc('hour', starttime);


--2.4.2 Now we can see what the weather was like at the start of a ride!
select weather as conditions,
count(*) as "num trips"
from trip_weather_vw
where conditions is not null
group by 1 order by 2 desc;

--2.5.1 Just like before, let's give our ANALYST access to this new view.
grant select on view citibike.schema60.trip_weather_vw to role analyst60;


-------------------------------------------------------------------------------
--SECTION 4.2: TIME TRAVEL
--4.1.1 Accidentally drop the trips table
drop table trips;


--4.1.3 Restore with an UNdrop!
undrop table trips;
select * from trips limit 10;


-------------------------------------------------------------------------------
--SECTION 4.3: ROLL BACK A TABLE
--4.2.1 Accidentally mess up the data and replace all the station names with "oops"
update trips set start_station_name = 'oops';


--4.2.2 Try to list the top 20 stations...
select
start_station_name as "station",
count(*) as "rides"
from trips
group by 1
order by 2 desc
limit 20;


--4.2.3 Fix it by finding the query_id and then rolling the table back
set query_id =
(select query_id from
table(information_schema.query_history_by_session (result_limit=>5))
where query_text like 'update%' order by start_time limit 1);

create or replace table trips as
    (select * from trips before (statement => $query_id));

--Run the query in 4.2.2 again and ta da! The data is fixed :)


-------------------------------------------------------------------------------
--SECTION 4.1 CREATE AN OUTBOUND SHARE

--4.1.1 Create a share called Citibike
create or replace share citibike60;

--4.1.2 Grant usage of the citibike database & schema to the share
grant usage on database citibike to share citibike60;
grant usage on schema citibike.schema60 to share citibike60;
grant select on all tables in schema citibike.schema0 to share citibike60;

--4.1.3 Add the pre-created Reader account (nu58393) to the share
alter share citibike60 add account=roa74561;


-------------------------------------------------------------------------------
--SECTION 4.3 UPDATE THE SHARE

--4.3.1 Let’s see how many rides per Membership type – notice there are a lot of NULLs:
select membership_type, count(*) from trips
group by 1 order by 2 desc;

--4.3.2 Let’s remove the NULLs from the table
delete from trips where membership_type is null;