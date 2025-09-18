-- Frosty Friday Inc., your benevolent employer, has an S3 bucket that was filled with .csv data dumps. These dumps aren’t very complicated and all have the same style and contents. All of these files should be placed into a single table.

-- However, it might occur that some important data is uploaded as well, these files have a different naming scheme and need to be tracked. We need to have the metadata stored for reference in a separate table. You can recognize these files because of a file inside of the S3 bucket. This file, keywords.csv, contains all of the keywords that mark a file as important.

-- Create a table that lists all the files in our stage that contain any of the keywords in the keywords.csv file.

-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_3/

CREATE OR REPLACE STAGE frostyfriday.code_challenges.metadata_stage_week3
URL = 's3://frostyfridaychallenges/challenge_3/'
;

list @frostyfriday.code_challenges.metadata_stage_week3;

select $1, $2, $3 , $4 from @frostyfriday.code_challenges.metadata_stage_week3/keywords.csv;

USE DATABASE frostyfriday;
USE SCHEMA code_challenges;
-- Creating two file formats one for keywords and other for the data files. 
CREATE OR REPLACE FILE FORMAT csv_format
TYPE = 'CSV'
FIELD_DELIMITER = ','
SKIP_HEADER = 1;

CREATE OR REPLACE TABLE keywords 
( keyword VARCHAR, added_by VARCHAR, nonsense VARCHAR) ;

COPY INTO keywords
FROM @frostyfriday.code_challenges.metadata_stage_week3/keywords.csv
FILE_FORMAT = (FORMAT_NAME = 'csv_format');

select * from keywords;


SELECT *
FROM TABLE(
  INFER_SCHEMA(
    LOCATION => '@frostyfriday.code_challenges.metadata_stage_week3/week3_data1.csv',
    FILE_FORMAT => 'csv_format'
  )
);

select $1, $2, $3, $4, $5, $6 from @frostyfriday.code_challenges.metadata_stage_week3/week3_data1.csv;

create or replace table WEEK3_DATA (
    FILE_NAME STRING
  , ROW_NUMBER INT
  , ID STRING
  , FIRST_NAME STRING
  , LAST_NAME STRING
  , CATCH_PHRASE STRING
  , TIMESTAMP_RAW STRING
)
;

COPY INTO WEEK3_DATA
FROM (
    select METADATA$FILENAME::STRING as FILE_NAME
    , METADATA$FILE_ROW_NUMBER as ROW_NUMBER
    , $1::string as ID
    , $2::string as FIRST_NAME
    , $3::string as LAST_NAME
    , $4::string as CATCH_PHRASE
    , $5::string as TIMESTAMP_RAW
FROM @frostyfriday.code_challenges.metadata_stage_week3
(FILE_FORMAT  => 'csv_format', PATTERN  => '.*week3.*' )
);;


select file_name as FILENAME, count(row_number) as NUMBER_OF_ROWS from WEEK3_DATA
where file_name LIKE ANY (select CONCAT('%', keyword,'%') from keywords)
GROUP BY file_name
ORDER BY FILENAME;
