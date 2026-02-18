CREATE DATABASE healthcare_analytics;
GO

USE healthcare_analytics;
GO

CREATE MASTER KEY ENCRYPTION BY PASSWORD = '${MASTER_KEY_PASSWORD}';
GO

-- CREATING A SCOPE
CREATE DATABASE SCOPED CREDENTIAL storage_credential
WITH IDENTITY = 'Managed Identity';
GO

-- DEFINE DATA SOURCE
CREATE EXTERNAL DATA SOURCE gold_data_source
WITH (
    LOCATION = 'abfss://gold@${STORAGE_ACCOUNT_NAME}.dfs.core.windows.net/',
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

-- Analytics Query

-- 1: Current Occupancy % by Department
SELECT
    d.department,
    d.hospital_name,
    d.bed_capacity,
    COUNT(CASE WHEN f.is_currently_admitted = 1 THEN 1 END) AS occupied_beds,
    CAST(COUNT(CASE WHEN f.is_currently_admitted = 1 THEN 1 END) AS FLOAT) 
        / d.bed_capacity * 100 AS occupancy_pct
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d ON f.department_sk = d.department_sk
GROUP BY d.department, d.hospital_name, d.bed_capacity
ORDER BY occupancy_pct DESC;


-- 2: Gender-Based Occupancy Distribution
SELECT
    p.gender,
    d.department,
    COUNT(*) AS patient_count,
    CAST(COUNT(*) AS FLOAT) 
        / SUM(COUNT(*)) OVER (PARTITION BY d.department) * 100 AS gender_pct
FROM dbo.fact_patient_flow f
JOIN dbo.dim_patient p ON f.patient_sk = p.patient_sk AND p.is_current = 1
JOIN dbo.dim_department d ON f.department_sk = d.department_sk
GROUP BY p.gender, d.department
ORDER BY d.department, p.gender;


-- 3: Average Length of Stay by Department
SELECT
    d.department,
    ROUND(AVG(f.length_of_stay_hrs), 2) AS avg_stay_hrs,
    ROUND(MIN(f.length_of_stay_hrs), 2) AS min_stay_hrs,
    ROUND(MAX(f.length_of_stay_hrs), 2) AS max_stay_hrs,
    COUNT(*) AS total_discharges
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d ON f.department_sk = d.department_sk
WHERE f.event_type = 'DISCHARGE' AND f.length_of_stay_hrs > 0
GROUP BY d.department
ORDER BY avg_stay_hrs DESC;


-- 4: Admissions by Priority Type
SELECT
    admission_priority,
    COUNT(*) AS admission_count,
    CAST(COUNT(*) AS FLOAT) 
        / SUM(COUNT(*)) OVER () * 100 AS priority_pct
FROM dbo.fact_patient_flow
WHERE event_type = 'ADMISSION'
GROUP BY admission_priority
ORDER BY admission_count DESC;


-- 5: Top 10 Diagnoses by Volume
SELECT TOP 10
    diagnosis_code,
    COUNT(*) AS case_count,
    ROUND(AVG(length_of_stay_hrs), 2) AS avg_stay_hrs
FROM dbo.fact_patient_flow
WHERE diagnosis_code IS NOT NULL
GROUP BY diagnosis_code
ORDER BY case_count DESC;


-- 6: Admissions by Day of Week (Weekend vs Weekday patterns)
SELECT
    dd.day_name,
    dd.is_weekend,
    COUNT(*) AS admissions
FROM dbo.fact_patient_flow f
JOIN dbo.dim_date dd ON f.admission_date = dd.date_key
WHERE f.event_type = 'ADMISSION'
GROUP BY dd.day_name, dd.day_of_week, dd.is_weekend
ORDER BY dd.day_of_week;


-- 7: Average Vitals by Department (clinical insight)
SELECT
    d.department,
    ROUND(AVG(CAST(f.heart_rate AS FLOAT)), 1) AS avg_heart_rate,
    ROUND(AVG(CAST(f.bp_systolic AS FLOAT)), 1) AS avg_bp_systolic,
    ROUND(AVG(CAST(f.oxygen_saturation AS FLOAT)), 1) AS avg_o2_sat,
    ROUND(AVG(f.temperature_f), 1) AS avg_temp_f
FROM dbo.fact_patient_flow f
JOIN dbo.dim_department d ON f.department_sk = d.department_sk
WHERE f.heart_rate IS NOT NULL
GROUP BY d.department
ORDER BY d.department;