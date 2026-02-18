CREATE DATABASE healthcare_analytics;
GO

USE healthcare_analytics;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = 'Temp@123';
GO

-- CREATING A SCOPE
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';
GO

-- DEFINE DATA SOURCE
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    LOCATION = 'abfss://gold@storagehospital99.dfs.core.windows.net/',
    CREDENTIAL = storage_credential
);
GO

-- CREATE FILE FORMAT
CREATE EXTERNAL FILE FORMAT ParquetFileFormat
WITH (
    FORMAT_TYPE = PARQUET
);
GO

-- CREATE TABLES
-- a. dim_patient
CREATE EXTERNAL TABLE dbo.dim_patient (
    patient_sk      BIGINT,
    patient_id      VARCHAR(100),
    gender          VARCHAR(20),
    age             INT,
    insurance_type  VARCHAR(50),
    effective_from  DATETIME2,
    effective_to    DATETIME2,
    is_current      BIT
)
WITH (
    LOCATION = 'dim_patient/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO


--b. dim_department
CREATE EXTERNAL TABLE dbo.dim_department (
    department_sk   BIGINT,
    department      VARCHAR(50),
    hospital_id     INT,
    hospital_name   VARCHAR(100),
    bed_capacity    INT
)
WITH (
    LOCATION = 'dim_department/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO


-- c. dim_date 
CREATE EXTERNAL TABLE dbo.dim_date (
    date_key    DATE,
    [year]      INT,
    [quarter]   INT,
    [month]     INT,
    month_name  VARCHAR(20),
    [day]       INT,
    day_of_week INT,
    day_name    VARCHAR(20),
    is_weekend  BIT
)
WITH (
    LOCATION = 'dim_date/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO


-- d. fact_patient_flow
CREATE EXTERNAL TABLE dbo.fact_patient_flow (
    fact_id                 BIGINT,
    patient_sk              BIGINT,
    department_sk           BIGINT,
    event_type              VARCHAR(20),
    event_id                VARCHAR(100),
    patient_id              VARCHAR(100),
    admission_date          DATE,
    admission_time          DATETIME2,
    discharge_time          DATETIME2,
    expected_discharge_time DATETIME2,
    length_of_stay_hrs      FLOAT,
    is_currently_admitted   BIT,
    admission_priority      VARCHAR(50),
    diagnosis_code          VARCHAR(100),
    discharge_status        VARCHAR(50),
    bed_id                  VARCHAR(50),
    heart_rate              INT,
    bp_systolic             INT,
    bp_diastolic            INT,
    temperature_f           FLOAT,
    oxygen_saturation       INT,
    _gold_processed_at      DATETIME2
)
WITH (
    LOCATION = 'fact_patient_flow/',
    DATA_SOURCE = gold_data_source,
    FILE_FORMAT = ParquetFileFormat
);
GO

-- VERIFY TABLES

SELECT 'fact_patient_flow' AS table_name, COUNT(*) AS row_count FROM dbo.fact_patient_flow
UNION ALL
SELECT 'dim_patient', COUNT(*) FROM dbo.dim_patient
UNION ALL
SELECT 'dim_department', COUNT(*) FROM dbo.dim_department
UNION ALL
SELECT 'dim_date', COUNT(*) FROM dbo.dim_date;

-- Create Deduplicated dim_department view
CREATE OR ALTER VIEW dbo.v_dim_department AS
SELECT 
    ROW_NUMBER() OVER (ORDER BY department, hospital_id) AS department_sk,
    department,
    hospital_id,
    hospital_name,
    bed_capacity
FROM (
    SELECT DISTINCT 
        department, 
        hospital_id, 
        hospital_name, 
        bed_capacity
    FROM dbo.dim_department
) deduped;
GO

-- Create deduplicated dim_date view
CREATE OR ALTER VIEW dbo.v_dim_date AS
SELECT DISTINCT
    date_key,
    [year],
    [quarter],
    [month],
    month_name,
    [day],
    day_of_week,
    day_name,
    is_weekend
FROM dbo.dim_date;
GO


-- Create dim_patient view
CREATE OR ALTER VIEW dbo.v_dim_patient AS
SELECT DISTINCT
    patient_sk,
    patient_id,
    gender,
    age,
    insurance_type,
    effective_from,
    effective_to,
    is_current
FROM dbo.dim_patient;
GO

-- Create fact view with corrected department_sk
-- Rejoins to v_dim_department to get the new unique keys
CREATE OR ALTER VIEW dbo.v_fact_patient_flow AS
WITH dept_old AS (
    -- Get original department data with its old sk values
    SELECT 
        department_sk AS old_department_sk,
        department,
        hospital_id
    FROM dbo.dim_department
),
dept_new AS (
    -- Get new unique sk values
    SELECT 
        department_sk AS new_department_sk,
        department,
        hospital_id
    FROM dbo.v_dim_department
),
dept_mapping AS (
    -- Map old sk → new sk via department + hospital_id
    SELECT DISTINCT
        o.old_department_sk,
        n.new_department_sk
    FROM dept_old o
    JOIN dept_new n ON o.department = n.department AND o.hospital_id = n.hospital_id
)
SELECT
    f.fact_id,
    f.patient_sk,
    COALESCE(dm.new_department_sk, f.department_sk) AS department_sk,
    f.event_type,
    f.event_id,
    f.patient_id,
    f.admission_date,
    f.admission_time,
    f.discharge_time,
    f.expected_discharge_time,
    f.length_of_stay_hrs,
    f.is_currently_admitted,
    f.admission_priority,
    f.diagnosis_code,
    f.discharge_status,
    f.bed_id,
    f.heart_rate,
    f.bp_systolic,
    f.bp_diastolic,
    f.temperature_f,
    f.oxygen_saturation,
    f._gold_processed_at
FROM dbo.fact_patient_flow f
LEFT JOIN dept_mapping dm ON f.department_sk = dm.old_department_sk;
GO

-- Verify duplicates

-- Check for duplicates (should return 0 for all)
SELECT 'v_dim_department duplicates' AS check_name, 
    COUNT(*) - COUNT(DISTINCT department_sk) AS duplicate_count 
FROM dbo.v_dim_department
UNION ALL
SELECT 'v_dim_date duplicates', 
    COUNT(*) - COUNT(DISTINCT date_key) AS duplicate_count 
FROM dbo.v_dim_date
UNION ALL
SELECT 'v_dim_patient duplicates', 
    COUNT(*) - COUNT(DISTINCT patient_sk) AS duplicate_count 
FROM dbo.v_dim_patient;

-- Row counts
SELECT 'v_fact_patient_flow' AS view_name, COUNT(*) AS row_count FROM dbo.v_fact_patient_flow
UNION ALL
SELECT 'v_dim_patient', COUNT(*) FROM dbo.v_dim_patient
UNION ALL
SELECT 'v_dim_department', COUNT(*) FROM dbo.v_dim_department
UNION ALL
SELECT 'v_dim_date', COUNT(*) FROM dbo.v_dim_date;

-- Preview each view
SELECT TOP 5 * FROM dbo.v_dim_department;
SELECT TOP 5 * FROM dbo.v_dim_date;
SELECT TOP 5 * FROM dbo.v_dim_patient;
SELECT TOP 5 * FROM dbo.v_fact_patient_flow;

-- Analytics Query

-- 1: Current Occupancy % by Department
SELECT
    d.department,
    d.hospital_name,
    d.bed_capacity,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN 1 END) AS occupied_beds,
    CAST(COUNT(CASE WHEN f.is_currently_admitted = 1 THEN 1 END) AS FLOAT) 
        / d.bed_capacity * 100 AS occupancy_pct
FROM dbo.v_fact_patient_flow f
JOIN dbo.v_dim_department d ON f.department_sk = d.department_sk
GROUP BY d.department, d.hospital_name, d.bed_capacity
ORDER BY occupancy_pct DESC;

-- 2: Gender-Based Distribution
SELECT
    p.gender,
    d.department,
    COUNT(*) AS patient_count
FROM dbo.v_fact_patient_flow f
JOIN dbo.v_dim_patient p ON f.patient_sk = p.patient_sk AND p.is_current = 1
JOIN dbo.v_dim_department d ON f.department_sk = d.department_sk
GROUP BY p.gender, d.department
ORDER BY d.department, p.gender;

-- 3: Average Length of Stay by Department
SELECT
    d.department,
    ROUND(AVG(f.length_of_stay_hrs), 2) AS avg_stay_hrs,
    COUNT(*) AS total_discharges
FROM dbo.v_fact_patient_flow f
JOIN dbo.v_dim_department d ON f.department_sk = d.department_sk
WHERE f.event_type = 'DISCHARGE' AND f.length_of_stay_hrs > 0
GROUP BY d.department
ORDER BY avg_stay_hrs DESC;

-- 4: Admissions by Day of Week
SELECT
    dd.day_name,
    dd.is_weekend,
    COUNT(*) AS admissions
FROM dbo.v_fact_patient_flow f
JOIN dbo.v_dim_date dd ON f.admission_date = dd.date_key
WHERE f.event_type = 'ADMISSION'
GROUP BY dd.day_name, dd.day_of_week, dd.is_weekend
ORDER BY dd.day_of_week;