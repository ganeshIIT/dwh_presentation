--lesson 5
alter user ganesh set default_role = 'SYSADMIN';
alter user ganesh set default_warehouse = 'COMPUTE_WH';


use role sysadmin;

use AGS_GAME_AUDIENCE.RAW;

create or replace task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
as
    select 'hello';

use role accountadmin;
--You have to run this grant or you won't be able to test your tasks while in SYSADMIN role
--this is true even if SYSADMIN owns the task!!
grant execute task on account to role SYSADMIN;

use role sysadmin;
--Now you should be able to run the task, even if your role is set to SYSADMIN
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--the SHOW command might come in handy to look at the task 
show tasks in account;

--you can also look at any task more in depth using DESCRIBE
describe task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


create or replace task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
as
INSERT INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
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
BETWEEN start_ip_int AND end_ip_int;


--make a note of how many rows you have in the table
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


--first we dump all the rows out of the table
truncate table ags_game_audience.enhanced.LOGS_ENHANCED;

--then we put them all back in
INSERT INTO ags_game_audience.enhanced.LOGS_ENHANCED (
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


--clone the table to save this version as a backup
--since it holds the records from the UPDATED FEED file, we'll name it _UF
create table ags_game_audience.enhanced.LOGS_ENHANCED_UF 
clone ags_game_audience.enhanced.LOGS_ENHANCED;

MERGE INTO ENHANCED.LOGS_ENHANCED e
USING RAW.LOGS r
ON r.user_login = e.GAMER_NAME
and r.datetime_iso8601 = e.game_event_utc
and r.user_event = e.game_event_name
WHEN MATCHED THEN
UPDATE SET IP_ADDRESS = 'Hey I updated matching rows!';

select * from raw.logs order by user_login;
select * from ENHANCED.LOGS_ENHANCED order by gamer_name;

truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



create or replace task load_logs_enhanced
    warehouse = 'COMPUTE_WH'
    schedule = '5 minute'
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
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
BETWEEN start_ip_int AND end_ip_int) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN not MATCHED THEN
insert(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY , GAMER_LTZ_NAME, DOW_NAME, TOD_NAME)
        values
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME);

        
        
-- truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

--make a note of how many rows you have in the table
select *
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;



--Run the task to load more rows
execute task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--check to see how many rows were added
select count(*)
from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;




--Insert a test record into your Raw Table 
--You can change the user_event field each time to create "new" records 
--editing the ip_address or datetime_iso8601 can complicate things more than they need to 
--editing the user_login will make it harder to remove the fake records after you finish testing 
INSERT INTO ags_game_audience.raw.game_logs 
select PARSE_JSON('{"datetime_iso8601":"2025-01-01 00:00:00.000", "ip_address":"196.197.196.255", "user_event":"fake event", "user_login":"fake user"}');


--After inserting a new row, run the Merge again 
EXECUTE TASK AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED;

--Check to see if any rows were added 
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;


--When you are confident your merge is working, you can delete the raw records 
delete from ags_game_audience.raw.game_logs where raw_log like '%fake user%';

--You should also delete the fake rows from the enhanced table
delete from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED
where gamer_name = 'fake user';

--Row count should be back to what it was in the beginning
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED; 



use AGS_GAME_AUDIENCE;

alter user ganesh set default_role = 'SYSADMIN';
alter user ganesh set default_warehouse = 'COMPUTE_WH';


use role sysadmin;


show tasks;

show stages;

use schema raw;

create or replace stage AGS_GAME_AUDIENCE.raw.uni_kishore_pipeline
    url = 's3://uni-kishore-pipeline';

list @AGS_GAME_AUDIENCE.raw.uni_kishore_pipeline;

create or replace TABLE AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS  (
	RAW_LOG VARIANT
);

copy into AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
from @AGS_GAME_AUDIENCE.raw.uni_kishore_pipeline
file_format = (format_name = ff_json_logs);

select * from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;



use role accountadmin;
grant EXECUTE MANAGED TASK on account to SYSADMIN;
use role sysadmin;


create or replace task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    schedule = '5 minute'
as
copy into AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS
from @AGS_GAME_AUDIENCE.raw.uni_kishore_pipeline
file_format = (format_name = ff_json_logs);



create or replace view AGS_GAME_AUDIENCE.RAW.PL_LOGS(
	USER_EVENT,
	USER_LOGIN,
	DATETIME_ISO8601,
	IP_ADDRESS,
	RAW_LOG
) as(
select
raw_log:user_event::text as user_event
,raw_log:user_login::text as user_login
,raw_log:datetime_iso8601::timestamp_ntz as datetime_iso8601
,raw_log:ip_address::text as ip_address
,*
from AGS_GAME_AUDIENCE.raw.PIPELINE_LOGS
where raw_log:agent::text is null
);


select * from AGS_GAME_AUDIENCE.RAW.PL_LOGS;



execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;




create or replace task load_logs_enhanced
    USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE = 'XSMALL'
    after AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES
as
MERGE INTO ENHANCED.LOGS_ENHANCED e
USING (SELECT logs.ip_address
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
from AGS_GAME_AUDIENCE.RAW.PL_LOGS logs
JOIN IPINFO_GEOLOC.demo.location loc 
ON IPINFO_GEOLOC.public.TO_JOIN_KEY(logs.ip_address) = loc.join_key
join time_of_day_lu tod
on tod.hour = date_part(hour, convert_timezone('UTC', timezone, logs.datetime_iso8601))
AND IPINFO_GEOLOC.public.TO_INT(logs.ip_address) 
BETWEEN start_ip_int AND end_ip_int) r
ON r.gamer_name = e.GAMER_NAME
and r.game_event_utc = e.game_event_utc
and r.game_event_name = e.game_event_name
WHEN not MATCHED THEN
insert(IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY , GAMER_LTZ_NAME, DOW_NAME, TOD_NAME)
        values
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME, GAME_EVENT_UTC, CITY, REGION, COUNTRY, GAMER_LTZ_NAME, DOW_NAME, TOD_NAME);


execute task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES;
execute task AGS_GAME_AUDIENCE.RAW.load_logs_enhanced;

show tasks;
describe task GET_NEW_FILES;

-- truncate table ENHANCED.LOGS_ENHANCED;

--Turning on a task is done with a RESUME command
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES resume;


select * from ENHANCED.LOGS_ENHANCED;

--Keep this code handy for shutting down the tasks each day
alter task AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES suspend;
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;



--Step 1 - how many files in the bucket?
list @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

--Step 2 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PIPELINE_LOGS;

--Step 3 - number of rows in raw table (should be file count x 10)
select count(*) from AGS_GAME_AUDIENCE.RAW.PL_LOGS;

--Step 4 - number of rows in enhanced table (should be file count x 10 but fewer rows is okay)
select count(*) from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;




