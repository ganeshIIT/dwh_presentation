alter user ganesh set default_role = 'SYSADMIN';
alter user ganesh set default_warehouse = 'COMPUTE_WH';

use role sysadmin;

use ags_game_audience.raw;

select 
raw_log:agent::text as agent
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,*
from AGS_GAME_AUDIENCE.raw.game_logs;


create or replace view AGS_GAME_AUDIENCE.raw.logs as(
select 
raw_log:agent::text as agent
,raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,*
from AGS_GAME_AUDIENCE.raw.game_logs
);

select * from logs;

select current_timestamp();

--worksheets are sometimes called sessions -- we'll be changing the worksheet time zone
alter session set timezone = 'UTC';
select current_timestamp();

--how did the time differ after changing the time zone for the worksheet?
alter session set timezone = 'Africa/Nairobi';
select current_timestamp();

alter session set timezone = 'Pacific/Funafuti';
select current_timestamp();

alter session set timezone = 'Asia/Shanghai';
select current_timestamp();

--show the account parameter called timezone
show parameters like 'timezone';


select * from AGS_GAME_AUDIENCE.raw.game_logs;

select * from AGS_GAME_AUDIENCE.raw.logs;

list @uni_kishore;

select $1 
from @uni_kishore/updated_feed
(file_format => ff_json_logs);


copy into AGS_GAME_AUDIENCE.raw.game_logs
from @uni_kishore/updated_feed
file_format = (format_name = ff_json_logs);

select * from game_logs;

select $1:agent::text, $1:ip_address::text
from game_logs
where $1:agent::text is null;

create or replace view AGS_GAME_AUDIENCE.raw.logs as(
select
raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:ip_address::text as ip_address
,*
from AGS_GAME_AUDIENCE.raw.game_logs
where raw_log:agent::text is null
);

select * from AGS_GAME_AUDIENCE.raw.logs;

select * from AGS_GAME_AUDIENCE.raw.logs where user_login like '%prajina%';


use role sysadmin;

use AGS_GAME_AUDIENCE.raw;


select parse_ip('100.41.16.160','inet');

select parse_ip('100.41.16.160','inet'):ipv4;

create or replace schema enhanced;

--Look up Kishore and Prajina's Time Zone in the IPInfo share using his headset's IP Address with the PARSE_IP function.
select start_ip, end_ip, start_ip_int, end_ip_int, city, region, country, timezone
from IPINFO_GEOLOC.demo.location
where parse_ip('100.41.16.160', 'inet'):ipv4 --Kishore's Headset's IP Address
BETWEEN start_ip_int AND end_ip_int;

select *
from AGS_GAME_AUDIENCE.RAW.LOGS;

select *
from AGS_GAME_AUDIENCE.RAW.game_logs;

--Join the log and location tables to add time zone to each row using the PARSE_IP function.
select logs.*
       , loc.city
       , loc.region
       , loc.country
       , loc.timezone
from AGS_GAME_AUDIENCE.RAW.LOGS logs
join IPINFO_GEOLOC.demo.location loc
where parse_ip(logs.ip_address, 'inet'):ipv4 
BETWEEN start_ip_int AND end_ip_int;


SELECT logs.ip_address
, logs.user_login
, logs.user_event
, logs.datetime_iso8601
, city
, region
, country
, timezone 
, convert_timezone('UTC', timezone, logs.datetime_iso8601)
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

use role sysadmin;
use database ags_game_audience;
use schema raw;


--a Look Up table to convert from hour number to "time of day name"
create or replace table ags_game_audience.raw.time_of_day_lu
(  hour number
   ,tod_name varchar(25)
);

--insert statement to add all 24 rows to the table
insert into time_of_day_lu
values
(6,'Early morning'),
(7,'Early morning'),
(8,'Early morning'),
(9,'Mid-morning'),
(10,'Mid-morning'),
(11,'Late morning'),
(12,'Late morning'),
(13,'Early afternoon'),
(14,'Early afternoon'),
(15,'Mid-afternoon'),
(16,'Mid-afternoon'),
(17,'Late afternoon'),
(18,'Late afternoon'),
(19,'Early evening'),
(20,'Early evening'),
(21,'Late evening'),
(22,'Late evening'),
(23,'Late evening'),
(0,'Late at night'),
(1,'Late at night'),
(2,'Late at night'),
(3,'Toward morning'),
(4,'Toward morning'),
(5,'Toward morning');


--Check your table to see if you loaded it properly
select tod_name, listagg(hour,',') within group (order by hour desc)
from time_of_day_lu
group by tod_name;

SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone AS GAMER_LTZ_NAME
, convert_timezone('UTC', timezone, logs.datetime_iso8601)
, dayname(convert_timezone('UTC', timezone, logs.datetime_iso8601)) as DOW_NAME
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
join time_of_day_lu tod
on tod.hour = date_part(hour, convert_timezone('UTC', timezone, logs.datetime_iso8601))
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;


use role sysadmin;

--Wrap any Select in a CTAS statement
create or replace table ags_game_audience.enhanced.logs_enhanced as(
SELECT logs.ip_address
, logs.user_login as GAMER_NAME
, logs.user_event AS GAME_EVENT_NAME
, logs.datetime_iso8601 AS GAME_EVENT_UTC
, city
, region
, country
, timezone as gamer_ltz_name
, convert_timezone('UTC', timezone, logs.datetime_iso8601) AS game_event_ltz
, dayname(convert_timezone('UTC', timezone, logs.datetime_iso8601)) as DOW_NAME
, tod_name
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
join time_of_day_lu tod
on tod.hour = date_part(hour, convert_timezone('UTC', timezone, logs.datetime_iso8601))
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int
);


SELECT *
from AGS_GAME_AUDIENCE.RAW.LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int;

select * from ags_game_audience.enhanced.logs_enhanced;


