-- =============================================================================
-- Snowflake Frosty Friday Challenge - Week 1
-- =============================================================================
-- FrostyFriday Inc., your benevolent employer, has an S3 bucket that is filled with .csv data dumps. This data is needed for analysis. Your task is to create an external stage, and load the csv files directly from that stage into a table.
-- The S3 bucket’s URI is: s3://frostyfridaychallenges/challenge_1/
-- Objective: 
-- - Create an external stage pointing to S3 bucket
-- - Load CSV data directly from the stage into a Snowflake table
-- - Handle file format and data structure properly
-- =============================================================================


-- Create Database and Schema
CREATE DATABASE frostyfriday;

-- Create a schema specifically for code challenges
CREATE SCHEMA frostyfriday.code_challenges;


-- Create External Stage
-- Create an external stage that points to the S3 bucket
-- This allows Snowflake to access files in S3 without loading them locally
CREATE OR REPLACE STAGE frostyfriday.code_challenges.external_stage_challenge_1
URL = 's3://frostyfridaychallenges/challenge_1/'
;

-- List all files in the external stage to see what's available
list @frostyfriday.code_challenges.external_stage_challenge_1;
-- Result: 3 CSV files are available in the path (1.csv, 2.csv, 3.csv)

-- Querying the files to see the data structure
select $1, $2, $3, $4 from @frostyfriday.code_challenges.external_stage_challenge_1/1.csv; 
select $1, $2, $3, $4 from @frostyfriday.code_challenges.external_stage_challenge_1/2.csv; 
select $1, $2, $3, $4 from @frostyfriday.code_challenges.external_stage_challenge_1/3.csv; 

-- Observations from the file exploration:
-- - All three files contain only 1 column (the rest columns are coming as null)
-- - Column header appears to be "result"
-- - Data appears to be simple text/string values


-- Create File Format
CREATE OR REPLACE FILE FORMAT frostyfriday.code_challenges.csv_format
TYPE = 'CSV'                    -- Specify file type as CSV
FIELD_DELIMITER = ','           -- Use comma as field separator
SKIP_HEADER = 1 ;               -- Skip the first row (header)

-- Verify the file format was created successfully
show file formats in frostyfriday.code_challenges;

-- Create Target Table
CREATE OR REPLACE TABLE frostyfriday.code_challenges.challenge_1
(
    result VARCHAR(255)          -- Single column to store the result data
);

-- Load Data from External Stage
-- Use COPY INTO command to load data directly from the external stage
COPY INTO frostyfriday.code_challenges.challenge_1
FROM @frostyfriday.code_challenges.external_stage_challenge_1    -- Source: external stage
FILE_FORMAT = (FORMAT_NAME = 'frostyfriday.code_challenges.csv_format')  -- Use our custom file format
;

-- Verify the Results
select * from frostyfriday.code_challenges.challenge_1;

