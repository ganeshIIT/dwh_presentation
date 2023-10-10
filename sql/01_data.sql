alter user ganesh set default_role = 'SYSADMIN';
alter user ganesh set default_warehouse = 'COMPUTE_WH';

select current_user();

use role sysadmin;

create or replace database AGS_GAME_AUDIENCE;

drop schema public;

create schema raw;

use schema raw;

create or replace table AGS_GAME_AUDIENCE.raw.game_logs(
    raw_log variant
);

create or replace stage uni_kishore
    url = 's3://uni-kishore';

list @uni_kishore/kickoff;

//Create a JSON file format in the new database
CREATE or replace FILE FORMAT AGS_GAME_AUDIENCE.RAW.FF_JSON_LOGS
    TYPE = 'JSON' 
    COMPRESSION = 'AUTO' 
    ENABLE_OCTAL = FALSE 
    ALLOW_DUPLICATE = FALSE 
    STRIP_OUTER_ARRAY = TRUE 
    STRIP_NULL_VALUES = FALSE 
    IGNORE_UTF8_ERRORS = FALSE;

select $1 
from @uni_kishore/kickoff
(file_format => ff_json_logs);

copy into AGS_GAME_AUDIENCE.raw.game_logs
from @uni_kishore/kickoff
file_format = (format_name = ff_json_logs);

select * from AGS_GAME_AUDIENCE.raw.game_logs;

