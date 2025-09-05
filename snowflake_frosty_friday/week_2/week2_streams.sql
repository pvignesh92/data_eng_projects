-- =============================================================================
-- Snowflake Frosty Friday Challenge - Week 2: Streams and Change Data Capture
-- =============================================================================
-- Challenge: Create a stream that only tracks changes to DEPT and JOB_TITLE columns
-- 
-- Objective: 
-- - Load parquet data from S3 into Snowflake
-- - Transform VARIANT data into structured table
-- - Create a view with only relevant columns (DEPT, JOB_TITLE)
-- - Create a stream on the view to track only specific changes
-- - Demonstrate change tracking with test updates
-- =============================================================================

-- =============================================================================
-- Step 1: Setup Database and Schema
-- =============================================================================
-- Use the database and schema created for the challenge
USE DATABASE frostyfriday;
USE SCHEMA code_challenges;

-- =============================================================================
-- Step 2: Create External Stage
-- =============================================================================
-- Create a stage to load data from local machine to Snowflake
CREATE OR REPLACE STAGE STAGE_WEEK2_CHALLENGE;

-- Verify stage creation
SHOW STAGES;

-- =============================================================================
-- Step 3: Load Parquet Data (Manual Step)
-- =============================================================================
-- Note: This step requires SnowSQL or Snowflake CLI to upload the file
-- Command to run in SnowSQL: 
-- PUT file:///snowflake_frosty_friday/week_2/employees.parquet @STAGE_WEEK2_CHALLENGE;

-- =============================================================================
-- Step 4: Create File Format for Parquet
-- =============================================================================
-- Create a file format to handle parquet files
CREATE OR REPLACE FILE FORMAT parquet_format
TYPE = 'PARQUET';

-- =============================================================================
-- Step 5: Explore Data Structure
-- =============================================================================
-- Examine the parquet file structure to understand the data format
SELECT $1
FROM @frostyfriday.code_challenges.STAGE_WEEK2_CHALLENGE/employees.parquet
(FILE_FORMAT => 'parquet_format');

-- The data appears to be in JSON format loaded in the first column as VARIANT
-- We need to unpack this VARIANT data into structured columns

-- =============================================================================
-- Step 6: Create Raw Data Table
-- =============================================================================
-- Create a table to store the raw VARIANT data from the parquet file
CREATE OR REPLACE TABLE employee_raw
(
    contents VARIANT  -- Store the entire record as VARIANT for unpacking
);

-- Load the parquet data into the raw table
COPY INTO employee_raw
FROM @frostyfriday.code_challenges.STAGE_WEEK2_CHALLENGE/employees.parquet
FILE_FORMAT = (FORMAT_NAME => 'parquet_format');

-- Verify data loading
SELECT * FROM employee_raw;

-- =============================================================================
-- Step 7: Inspect VARIANT Structure
-- =============================================================================
-- Examine the keys available in the VARIANT data to understand the schema
SELECT OBJECT_KEYS(contents) FROM employee_raw LIMIT 1;
-- Result: ["city", "country", "country_code", "dept", "education", "email", 
--          "employee_id", "first_name", "job_title", "last_name", "payroll_iban", 
--          "postcode", "street_name", "street_num", "time_zone", "title"]

-- =============================================================================
-- Step 8: Create Structured Table
-- =============================================================================
-- Create a properly structured table with all available fields
CREATE OR REPLACE TABLE employee_parsed (
    employee_id INTEGER,           -- Primary identifier
    first_name VARCHAR(255),       -- Employee first name
    last_name VARCHAR(255),        -- Employee last name
    title VARCHAR(100),            -- Title (Mr, Ms, Dr, etc.)
    email VARCHAR(255),            -- Email address
    dept VARCHAR(100),             -- Department (key field for tracking)
    job_title VARCHAR(100),        -- Job title (key field for tracking)
    education VARCHAR(255),        -- Education level
    street_num VARCHAR(50),        -- Street number
    street_name VARCHAR(255),      -- Street name
    city VARCHAR(100),             -- City
    postcode VARCHAR(20),          -- Postal code
    country VARCHAR(100),          -- Country
    country_code VARCHAR(10),      -- Country code
    time_zone VARCHAR(50),         -- Time zone
    payroll_iban VARCHAR(50)       -- IBAN for payroll
);

-- =============================================================================
-- Step 9: Transform and Load Data
-- =============================================================================
-- Insert data by extracting and casting fields from the VARIANT column
INSERT INTO employee_parsed
SELECT 
    contents:employee_id::INTEGER as employee_id,
    contents:first_name::VARCHAR as first_name,
    contents:last_name::VARCHAR as last_name,
    contents:title::VARCHAR as title,
    contents:email::VARCHAR as email,
    contents:dept::VARCHAR as dept,           -- Key field for change tracking
    contents:job_title::VARCHAR as job_title, -- Key field for change tracking
    contents:education::VARCHAR as education,
    contents:street_num::VARCHAR as street_num,
    contents:street_name::VARCHAR as street_name,
    contents:city::VARCHAR as city,
    contents:postcode::VARCHAR as postcode,
    contents:country::VARCHAR as country,
    contents:country_code::VARCHAR as country_code,
    contents:time_zone::VARCHAR as time_zone,
    contents:payroll_iban::VARCHAR as payroll_iban
FROM employee_raw;

-- Verify the transformed data
SELECT * FROM employee_parsed;

-- =============================================================================
-- Step 10: Create Filtered View
-- =============================================================================
-- Create a view that only contains the columns we want to track for changes
-- This addresses the stakeholder's concern about "too much info"
CREATE OR REPLACE VIEW cdc_tracking
AS 
SELECT 
    employee_id,    -- Identifier for the record
    dept,           -- Department changes (what HR cares about)
    job_title       -- Job title changes (what HR cares about)
FROM employee_parsed;

-- Verify the view contains only the relevant columns
SELECT * FROM cdc_tracking;

-- =============================================================================
-- Step 11: Create Stream for Change Tracking
-- =============================================================================
-- Create a stream on the view to track only changes to DEPT and JOB_TITLE
-- This stream will only capture changes to the columns in the view
CREATE OR REPLACE STREAM employee_cdc_tracking
ON VIEW cdc_tracking;

-- Initially, the stream will be empty (no changes yet)
SELECT * FROM employee_cdc_tracking;

-- =============================================================================
-- Step 12: Test Change Tracking
-- =============================================================================
-- Apply test changes to demonstrate the stream functionality
-- Note: Only changes to DEPT and JOB_TITLE will be captured by the stream

-- Change 1: Update country (NOT tracked by stream - not in view)
UPDATE employee_parsed SET COUNTRY = 'Japan' WHERE EMPLOYEE_ID = 8;

-- Change 2: Update last name (NOT tracked by stream - not in view)
UPDATE employee_parsed SET LAST_NAME = 'Forester' WHERE EMPLOYEE_ID = 22;

-- Change 3: Update department (TRACKED by stream - in view)
UPDATE employee_parsed SET DEPT = 'Marketing' WHERE EMPLOYEE_ID = 25;

-- Change 4: Update title (NOT tracked by stream - not in view)
UPDATE employee_parsed SET TITLE = 'Ms' WHERE EMPLOYEE_ID = 32;

-- Change 5: Update job title (TRACKED by stream - in view)
UPDATE employee_parsed SET JOB_TITLE = 'Senior Financial Analyst' WHERE EMPLOYEE_ID = 68;

-- =============================================================================
-- Step 13: Verify Stream Results
-- =============================================================================
-- Check the stream to see which changes were captured
-- Only changes to DEPT and JOB_TITLE should appear here
SELECT * FROM employee_cdc_tracking;

-- =============================================================================
-- Summary
-- =============================================================================
-- This challenge demonstrates:
-- 1. Loading parquet data with VARIANT columns
-- 2. Unpacking VARIANT data into structured tables
-- 3. Creating filtered views for specific business needs
-- 4. Using streams on views to track only relevant changes
-- 5. Change Data Capture (CDC) with selective column tracking
-- 
-- Key Learning: Streams on views only track changes to columns present in the view,
-- providing a clean way to filter change tracking to business-relevant fields.
-- =============================================================================