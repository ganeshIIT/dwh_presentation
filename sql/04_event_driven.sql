
alter user ganesh set default_role = 'SYSADMIN';
alter user ganesh set default_warehouse = 'COMPUTE_WH';


use role sysadmin;
use AGS_GAME_AUDIENCE;
use schema raw;

SELECT $1
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE;

SELECT 
METADATA$FILENAME as log_file_name --new metadata column
, METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
, current_timestamp(0) as load_ltz --new local time of load
, get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
, get($1,'user_event')::text as USER_EVENT
, get($1,'user_login')::text as USER_LOGIN
, get($1,'ip_address')::text as IP_ADDRESS    
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');

create table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS
as
SELECT 
METADATA$FILENAME as log_file_name --new metadata column
, METADATA$FILE_ROW_NUMBER as log_file_row_id --new metadata column
, current_timestamp(0) as load_ltz --new local time of load
, get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
, get($1,'user_event')::text as USER_EVENT
, get($1,'user_login')::text as USER_LOGIN
, get($1,'ip_address')::text as IP_ADDRESS    
FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
(file_format => 'ff_json_logs');


select * from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--truncate the table rows that were input during the CTAS
truncate table ED_PIPELINE_LOGS;

--reload the table using your COPY INTO
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);

select * from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;


CREATE OR REPLACE PIPE GET_NEW_FILES
auto_ingest=true
aws_sns_topic='arn:aws:sns:us-west-2:321463406630:dngw_topic'
AS 
COPY INTO ED_PIPELINE_LOGS
FROM (
    SELECT 
    METADATA$FILENAME as log_file_name 
  , METADATA$FILE_ROW_NUMBER as log_file_row_id 
  , current_timestamp(0) as load_ltz 
  , get($1,'datetime_iso8601')::timestamp_ntz as DATETIME_ISO8601
  , get($1,'user_event')::text as USER_EVENT
  , get($1,'user_login')::text as USER_LOGIN
  , get($1,'ip_address')::text as IP_ADDRESS    
  FROM @AGS_GAME_AUDIENCE.RAW.UNI_KISHORE_PIPELINE
)
file_format = (format_name = ff_json_logs);


create or replace task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	schedule = '5 minute'
	as MERGE INTO ENHANCED.LOGS_ENHANCED e
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
from AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS  logs
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


alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED resume;

select * from ENHANCED.LOGS_ENHANCED;


--create a stream that will keep track of changes to the table
create or replace stream ags_game_audience.raw.ed_cdc_stream 
on table AGS_GAME_AUDIENCE.RAW.ED_PIPELINE_LOGS;

--look at the stream you created
show streams;

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--query the stream
select * 
from ags_game_audience.raw.ed_cdc_stream; 

--check to see if any changes are pending
select system$stream_has_data('ed_cdc_stream');

--if your stream remains empty for more than 10 minutes, make sure your PIPE is running
select SYSTEM$PIPE_STATUS('GET_NEW_FILES');

--if you need to pause or unpause your pipe
--alter pipe GET_NEW_FILES set pipe_execution_paused = true;
--alter pipe GET_NEW_FILES set pipe_execution_paused = false;

MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);

        

select * 
from ags_game_audience.raw.ed_cdc_stream; 





--turn off the other task (we won't need it anymore)
alter task AGS_GAME_AUDIENCE.RAW.LOAD_LOGS_ENHANCED suspend;

--Create a new task that uses the MERGE you just tested
create or replace task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED
	USER_TASK_MANAGED_INITIAL_WAREHOUSE_SIZE='XSMALL'
	SCHEDULE = '5 minutes'
when system$stream_has_data('ed_cdc_stream')
	as 
MERGE INTO AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED e
USING (
        SELECT cdc.ip_address 
        , cdc.user_login as GAMER_NAME
        , cdc.user_event as GAME_EVENT_NAME
        , cdc.datetime_iso8601 as GAME_EVENT_UTC
        , city
        , region
        , country
        , timezone as GAMER_LTZ_NAME
        , CONVERT_TIMEZONE( 'UTC',timezone,cdc.datetime_iso8601) as game_event_ltz
        , DAYNAME(game_event_ltz) as DOW_NAME
        , TOD_NAME
        from ags_game_audience.raw.ed_cdc_stream cdc
        JOIN ipinfo_geoloc.demo.location loc 
        ON ipinfo_geoloc.public.TO_JOIN_KEY(cdc.ip_address) = loc.join_key
        AND ipinfo_geoloc.public.TO_INT(cdc.ip_address) 
        BETWEEN start_ip_int AND end_ip_int
        JOIN AGS_GAME_AUDIENCE.RAW.TIME_OF_DAY_LU tod
        ON HOUR(game_event_ltz) = tod.hour
      ) r
ON r.GAMER_NAME = e.GAMER_NAME
AND r.GAME_EVENT_UTC = e.GAME_EVENT_UTC
AND r.GAME_EVENT_NAME = e.GAME_EVENT_NAME 
WHEN NOT MATCHED THEN 
INSERT (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME)
        VALUES
        (IP_ADDRESS, GAMER_NAME, GAME_EVENT_NAME
        , GAME_EVENT_UTC, CITY, REGION
        , COUNTRY, GAMER_LTZ_NAME, GAME_EVENT_LTZ
        , DOW_NAME, TOD_NAME);
        
--Resume the task so it is running
alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED resume;

-- truncate table AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

select system$stream_has_data('ed_cdc_stream');
select * from ags_game_audience.raw.ed_cdc_stream; 
select count(*) from AGS_GAME_AUDIENCE.raw.ED_PIPELINE_LOGS;
select * from AGS_GAME_AUDIENCE.ENHANCED.LOGS_ENHANCED;

alter task AGS_GAME_AUDIENCE.RAW.CDC_LOAD_LOGS_ENHANCED suspend;
alter pipe AGS_GAME_AUDIENCE.RAW.GET_NEW_FILES set pipe_execution_paused = true;

show tasks in schema ags_game_audience.raw;
select SYSTEM$PIPE_STATUS('GET_NEW_FILES');