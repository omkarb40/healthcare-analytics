# 🏥 Real-Time Healthcare Data Platform
### Patient Flow & Bed Occupancy Analytics — End-to-End Azure Data Engineering

<p align="center">
  <img src="docs/architecture_diagram.png" alt="Architecture Diagram" width="100%"/>
</p>

<p align="center">
  <a href="#architecture"><img src="https://img.shields.io/badge/Azure-Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white" /></a>
  <a href="#architecture"><img src="https://img.shields.io/badge/Azure-Data_Factory-0078D4?style=flat-square&logo=microsoftazure&logoColor=white" /></a>
  <a href="#architecture"><img src="https://img.shields.io/badge/Azure-Synapse-0078D4?style=flat-square&logo=microsoftazure&logoColor=white" /></a>
  <a href="#architecture"><img src="https://img.shields.io/badge/Azure-Event_Hubs-0078D4?style=flat-square&logo=microsoftazure&logoColor=white" /></a>
  <a href="#architecture"><img src="https://img.shields.io/badge/Delta-Lake-003366?style=flat-square&logo=delta&logoColor=white" /></a>
  <a href="#architecture"><img src="https://img.shields.io/badge/Python-3.11-3776AB?style=flat-square&logo=python&logoColor=white" /></a>
</p>

---

## 📋 Project Overview

A production-grade, end-to-end data engineering platform built for **Midwest Health Alliance (MHA)**, a fictional network of 7 hospitals. The platform ingests real-time patient admission and discharge events, processes them through a Medallion Architecture (Bronze → Silver → Gold), and serves analytics-ready data through Azure Synapse for dashboarding.

**Business Problem:** MHA lacked a centralized, real-time system to monitor bed occupancy, patient flow patterns, and department-level bottlenecks — especially critical during seasonal surges like flu outbreaks.

**Solution:** An automated Azure-based pipeline that streams patient events, cleans and validates data, builds a Star Schema data warehouse, and delivers KPIs including occupancy rates, average length of stay, gender-based demographics, and clinical vitals by department.

---

## 🏗️ Architecture

<a name="architecture"></a>

```
┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────────────────────┐     ┌───────────────┐
│   Patient    │     │    Azure     │     │          Azure Databricks + ADLS Gen2        │     │    Azure      │
│    Flow      │────▶│  Event Hubs  │────▶│                                              │────▶│   Synapse     │
│  Simulator   │     │  (Kafka)     │     │  ┌─────────┐  ┌─────────┐  ┌─────────────┐  │     │  Serverless   │
│  (Python)    │     │              │     │  │ BRONZE  │─▶│ SILVER  │─▶│    GOLD     │  │     │  SQL Pool     │
└─────────────┘     └──────────────┘     │  │ Raw JSON│  │Cleaned +│  │Star Schema │  │     └───────────────┘
                                          │  │         │  │  DQ Flags│  │ SCD Type 2 │  │
       ┌────────────────┐                 │  └─────────┘  └─────────┘  └─────────────┘  │
       │  Azure Key     │                 │                                              │
       │  Vault         │─── Secrets ────▶│     Orchestrated by Azure Data Factory       │
       └────────────────┘                 └─────────────────────────────────────────────┘
```

### Component Breakdown

| Component | Service | Purpose |
|-----------|---------|---------|
| **Data Source** | Python Simulator | Generates realistic patient admission/discharge events with configurable dirty data |
| **Streaming** | Azure Event Hubs (Kafka) | Ingests real-time events via Kafka protocol on port 9093 |
| **Orchestration** | Azure Data Factory | Chains Bronze → Silver → Gold notebooks with success/failure triggers |
| **Processing** | Azure Databricks (PySpark) | Runs Medallion Architecture transformations across 3 notebook layers |
| **Storage** | ADLS Gen2 (Delta Lake) | Stores all layers as Delta tables with schema evolution support |
| **Security** | Azure Key Vault | Stores Event Hub connection strings and storage account keys |
| **Serving** | Azure Synapse (Serverless SQL) | Exposes Gold layer via external tables and views for analytics |

---

## 📂 Project Structure

```
healthcare-analytics/
├── simulator/
│   ├── patient_flow_simulator.py    # Event generator with 7 dirty data scenarios
│   ├── .env.example                 # Environment variable template
│   └── requirements.txt             # Python dependencies
│
├── databricks/
│   ├── 01_bronze_ingestion.py       # Event Hub → Bronze (raw JSON + metadata)
│   ├── 02_silver_data_cleaning.py   # Bronze → Silver (7 DQ rules + deduplication)
│   └── 03_gold_data_transform.py    # Silver → Gold (Star Schema + SCD2)
│
├── synapse/
│   ├── synapse_external_tables.sql  # External tables pointing to Gold Delta files
│   └── synapse_fix_views.sql        # Deduplicated views for analytics
│
├── docs/
│   ├── architecture_diagram.png     # System architecture visual
│   └── pipeline_architecture.html   # Interactive architecture reference
│
├── .gitignore
└── README.md
```

---

## 🔧 Technical Deep Dive

### 1. Patient Flow Simulator (`simulator/`)

A Python-based event generator that simulates realistic hospital operations across 7 hospitals and 7 departments.

**Key Features:**
- **Admission → Discharge Lifecycle:** Tracks active patients in memory; generates separate `ADMISSION` and `DISCHARGE` events for the same `patient_id`, simulating real hospital data flow
- **Rich Data Model:** 19+ fields per event including ICD-10 diagnosis codes, insurance type, admission priority (weighted distribution), vitals (heart rate, BP, O2 saturation, temperature), and structured bed IDs (`H3-SUR-15`)
- **7 Configurable Dirty Data Scenarios:**
  - Invalid age (negative, >120)
  - Future admission timestamps
  - Missing/null critical fields
  - Discharge before admission
  - Department name typos (`Emergancy`, `PEDS`, `Cardiolgy`)
  - Invalid hospital IDs (outside 1-7)
  - Duplicate events (same event sent twice)
- **Seasonal Surge Simulation:** 3x admission rates during flu season (Dec-Feb) and weekend evenings
- **CLI Interface:** Supports `--dry-run`, `--max-events`, and `--connection-string` flags
- **Environment Variable Security:** Connection strings loaded from environment, never hardcoded

```bash
# Dry run (no Event Hub needed)
python patient_flow_simulator.py --dry-run --max-events 20

# Live streaming
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://..."
python patient_flow_simulator.py --max-events 500
```

### 2. Bronze Layer — Raw Ingestion (`01_bronze_ingestion.py`)

Reads from Azure Event Hubs via Spark Structured Streaming (Kafka protocol) and lands raw JSON into Delta format.

**What it captures:**
- Raw JSON payload (untouched)
- Kafka metadata: `topic`, `partition`, `offset`, `eventhub_timestamp`
- Ingestion metadata: `ingested_at`, `data_source`
- Schema evolution enabled (`mergeSchema: true`)

**Design Decisions:**
- Credentials fetched from Databricks Secret Scope (backed by Azure Key Vault)
- `maxOffsetsPerTrigger: 1000` to control micro-batch size for cost management
- 30-second trigger interval balancing near-real-time with compute costs

### 3. Silver Layer — Data Cleaning (`02_silver_data_cleaning.py`)

Parses raw JSON against the expected schema and applies data quality rules for all 7 dirty data scenarios.

**Data Quality Rules Applied:**

| Rule | Detection | Action | DQ Flag |
|------|-----------|--------|---------|
| Future admission time | `admission_time > current_timestamp()` | Replace with `event_timestamp` | `FUTURE_ADMISSION` |
| Invalid age | `age < 0 OR age > 120` | Set to `NULL` | `INVALID_AGE` |
| Discharge before admission | `discharge_time < admission_time` | Null out discharge | `DISCHARGE_BEFORE_ADMIT` |
| Department typo | Not in valid list | Map to correct name | `DEPT_TYPO` |
| Invalid hospital ID | `< 1 OR > 7` | Set to `NULL` | `INVALID_HOSPITAL_ID` |
| Missing critical fields | `patient_id`, `event_type`, or `department` is null | Flag only | `MISSING_{FIELD}` |
| Duplicate events | Same `event_id` | `dropDuplicates()` | — |

**Key Feature:** Every record gets a `_dq_flags` column (e.g., `FUTURE_ADMISSION\|DEPT_TYPO`) providing full audit trail of what was corrected — not just silently fixed.

### 4. Gold Layer — Star Schema (`03_gold_data_transform.py`)

Transforms Silver data into a dimensional model optimized for analytics.

**Star Schema:**

```
                    ┌─────────────────┐
                    │   dim_patient   │
                    │   (SCD Type 2)  │
                    │─────────────────│
                    │ patient_sk (PK) │
                    │ patient_id      │
                    │ gender          │
                    │ age             │
                    │ insurance_type  │
                    │ effective_from  │
                    │ effective_to    │
                    │ is_current      │
                    └────────┬────────┘
                             │
┌─────────────────┐   ┌──────┴──────────────────────┐   ┌──────────────────┐
│ dim_department  │   │    fact_patient_flow         │   │    dim_date      │
│─────────────────│   │─────────────────────────────│   │──────────────────│
│ department_sk   │◄──│ fact_id (PK)                │──▶│ date_key (PK)    │
│ department      │   │ patient_sk (FK)             │   │ year             │
│ hospital_id     │   │ department_sk (FK)          │   │ quarter          │
│ hospital_name   │   │ admission_date (FK)         │   │ month / month_name│
│ bed_capacity    │   │ event_type                  │   │ day / day_name   │
└─────────────────┘   │ admission_time              │   │ day_of_week      │
                      │ discharge_time              │   │ is_weekend       │
                      │ length_of_stay_hrs          │   └──────────────────┘
                      │ admission_priority          │
                      │ diagnosis_code              │
                      │ heart_rate, bp_systolic ... │
                      └─────────────────────────────┘
```

**SCD Type 2 Implementation:** The `dim_patient` table tracks historical changes using hash-based change detection. When a patient's `gender`, `age`, or `insurance_type` changes between events, the old record is expired (`is_current = false`, `effective_to` set) and a new current record is inserted.

**Vitals Flattening:** The nested `vitals` struct from Silver is flattened into individual columns (`heart_rate`, `bp_systolic`, `bp_diastolic`, `temperature_f`, `oxygen_saturation`) for direct query access in Synapse and dashboards.

### 5. Synapse Analytics Layer (`synapse/`)

Azure Synapse Serverless SQL pool exposes Gold Delta tables via external tables and deduplicated views.

**Views handle:**
- Duplicate surrogate key resolution (Spark's `monotonically_increasing_id` can produce duplicates across partitions)
- `ROW_NUMBER()` reassignment for unique dimension keys
- Old-to-new key mapping for fact table foreign keys

**Sample KPIs Available:**
- Current occupancy % by department and hospital
- Gender-based occupancy distribution
- Average length of stay by department
- Admissions by priority type and day of week
- Top diagnoses by volume
- Average vitals (HR, BP, O2 sat) by department

### 6. ADF Orchestration

Azure Data Factory pipeline chains the 3 Databricks notebooks:

```
[01_bronze_ingestion] ──✅──▶ [02_silver_data_cleaning] ──✅──▶ [03_gold_data_transform]
                       │                                  │
                       ❌──▶ Alert                        ❌──▶ Alert
```

---

## 🚀 Setup & Deployment

### Prerequisites
- Azure account with active subscription
- Azure Databricks workspace
- Azure Event Hubs namespace
- ADLS Gen2 storage account with containers: `bronze`, `silver`, `gold`
- Azure Key Vault with secrets for Event Hub and Storage connections
- Python 3.11+

### Step 1: Configure Secrets

```bash
# Databricks CLI — create secret scope backed by Key Vault
databricks secrets create-scope --scope healthcare-analytics
databricks secrets put --scope healthcare-analytics --key eventhub-connection-string
databricks secrets put --scope healthcare-analytics --key adls-access-key
```

### Step 2: Run the Simulator

```bash
cd simulator/
pip install -r requirements.txt

# Set connection string
export EVENTHUB_CONNECTION_STRING="Endpoint=sb://your-namespace.servicebus.windows.net/;..."

# Stream events
python patient_flow_simulator.py --max-events 500
```

### Step 3: Run Databricks Notebooks

Import the 3 notebooks into your Databricks workspace and run in order:
1. `01_bronze_ingestion.py` — Streams Event Hub data to Bronze
2. `02_silver_data_cleaning.py` — Cleans and validates data
3. `03_gold_data_transform.py` — Builds Star Schema

### Step 4: Set Up Synapse

Run the SQL scripts in `synapse/` against your Synapse Serverless SQL pool to create external tables and views.

### Step 5: Configure ADF Pipeline

Create a pipeline with 3 Databricks Notebook activities chained with success dependencies.

---

## 📊 Key Metrics & Results

| Metric | Value |
|--------|-------|
| Events Processed | ~2,400+ patient events |
| Hospitals Covered | 7 |
| Departments | 7 (Emergency, Surgery, ICU, Pediatrics, Maternity, Oncology, Cardiology) |
| Dirty Data Scenarios | 7 types injected and handled |
| Data Quality Coverage | 100% of records flagged with `_dq_flags` audit trail |
| Schema Fields | 19+ per event (vs 8 in standard implementations) |
| Pipeline Automation | Fully orchestrated via ADF with failure alerting |

---

## 🛠️ Tech Stack

| Category | Technologies |
|----------|-------------|
| **Languages** | Python 3.11, PySpark, SQL |
| **Streaming** | Azure Event Hubs (Kafka protocol), Spark Structured Streaming |
| **Processing** | Azure Databricks, Delta Lake |
| **Storage** | Azure Data Lake Storage Gen2 |
| **Orchestration** | Azure Data Factory |
| **Data Warehouse** | Azure Synapse Analytics (Serverless SQL) |
| **Security** | Azure Key Vault, Databricks Secret Scopes, RBAC |
| **Data Modeling** | Star Schema, SCD Type 2, Medallion Architecture |
| **Version Control** | Git / GitHub |

---

## 🧠 What I Learned

- Designing and implementing a **Medallion Architecture** (Bronze/Silver/Gold) with Delta Lake for reliable, incremental data processing
- Building **SCD Type 2** dimensions with hash-based change detection for tracking historical attribute changes
- Connecting Azure Event Hubs via **Kafka protocol** for real-time streaming ingestion
- Implementing **7 data quality rules** with audit trail flags — not just fixing data, but documenting what was fixed
- Managing **schema evolution** gracefully so new fields don't break existing pipelines
- Using **Azure Key Vault + Databricks Secret Scopes** for production-grade secrets management
- Orchestrating multi-notebook pipelines with **Azure Data Factory** including failure handling
- Creating **Synapse Serverless SQL views** to resolve surrogate key issues without reprocessing data
- Cost optimization strategies for Azure free tier (batch vs. streaming, trigger intervals, single-node clusters)

---

## 📄 License

This project is for educational and portfolio purposes. The patient data is entirely simulated — no real patient information is used.

## Reference and Shoutout 

Huge thanks and shoutout to Jaya Chandra (https://github.com/Jay61616) for a comprehensive and in-dept demo of this project. 