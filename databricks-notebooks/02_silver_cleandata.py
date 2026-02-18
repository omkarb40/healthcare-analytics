# Databricks notebook source

from pyspark.sql.types import *
from pyspark.sql.functions import *

storage_account_name = "${STORAGE_ACCOUNT_NAME}"  # Update with your storage account name
storage_account_key = dbutils.secrets.get(scope="${KEY_VAULT_SCOPE}", key="storageconnection")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/patient_flow"
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/patient_flow"

# Read from Bronze (batch mode)
bronze_df = spark.read.format("delta").load(bronze_path)

# Vitals nested schema
vitals_schema = StructType([
    StructField("heart_rate", IntegerType()),
    StructField("bp_systolic", IntegerType()),
    StructField("bp_diastolic", IntegerType()),
    StructField("temperature_f", DoubleType()),
    StructField("oxygen_saturation", IntegerType())
])

# Full event schema matching our simulator output
event_schema = StructType([
    StructField("event_type", StringType()),
    StructField("event_id", StringType()),
    StructField("patient_id", StringType()),
    StructField("gender", StringType()),
    StructField("age", IntegerType()),
    StructField("department", StringType()),
    StructField("hospital_id", IntegerType()),
    StructField("hospital_name", StringType()),
    StructField("bed_id", StringType()),
    StructField("admission_time", StringType()),
    StructField("discharge_time", StringType()),
    StructField("expected_discharge_time", StringType()),
    StructField("length_of_stay_hrs", DoubleType()),
    StructField("admission_priority", StringType()),
    StructField("diagnosis_code", StringType()),
    StructField("insurance_type", StringType()),
    StructField("discharge_status", StringType()),
    StructField("vitals", vitals_schema),
    StructField("event_timestamp", StringType())
])

# Parse JSON and preserve Bronze metadata
parsed_df = (
    bronze_df
    .withColumn("data", from_json(col("raw_json"), event_schema))
    .select(
        "data.*",
        col("eventhub_timestamp").alias("_bronze_eventhub_ts"),
        col("ingested_at").alias("_bronze_ingested_at")
    )
)

# Start data quality processing
clean_df = parsed_df.withColumn("_dq_flags", lit(""))

# Convert string timestamps to proper timestamp type
clean_df = (
    clean_df
    .withColumn("admission_time", to_timestamp("admission_time"))
    .withColumn("discharge_time", to_timestamp("discharge_time"))
    .withColumn("expected_discharge_time", to_timestamp("expected_discharge_time"))
    .withColumn("event_timestamp", to_timestamp("event_timestamp"))
)

# Fix future admission times
clean_df = clean_df.withColumn(
    "_dq_flags",
    when(
        col("admission_time").isNotNull() & (col("admission_time") > current_timestamp()),
        concat(col("_dq_flags"), lit("FUTURE_ADMISSION|"))
    ).otherwise(col("_dq_flags"))
).withColumn(
    "admission_time",
    when(
        col("admission_time").isNull() | (col("admission_time") > current_timestamp()),
        coalesce(col("event_timestamp"), current_timestamp())
    ).otherwise(col("admission_time"))
)

# Fix invalid age
clean_df = clean_df.withColumn(
    "_dq_flags",
    when(
        col("age").isNotNull() & ((col("age") < 0) | (col("age") > 120)),
        concat(col("_dq_flags"), lit("INVALID_AGE|"))
    ).otherwise(col("_dq_flags"))
).withColumn(
    "age",
    when(
        (col("age") < 0) | (col("age") > 120),
        lit(None).cast("int")
    ).otherwise(col("age"))
)

# Fix discharge before admission
clean_df = clean_df.withColumn(
    "_dq_flags",
    when(
        col("discharge_time").isNotNull() & col("admission_time").isNotNull()
        & (col("discharge_time") < col("admission_time")),
        concat(col("_dq_flags"), lit("DISCHARGE_BEFORE_ADMIT|"))
    ).otherwise(col("_dq_flags"))
).withColumn(
    "discharge_time",
    when(
        col("discharge_time").isNotNull() & col("admission_time").isNotNull()
        & (col("discharge_time") < col("admission_time")),
        lit(None).cast("timestamp")
    ).otherwise(col("discharge_time"))
)

# Standardize department name typos
valid_departments = ["Emergency", "Surgery", "ICU", "Pediatrics", "Maternity", "Oncology", "Cardiology"]

department_mapping = {
    "Emergancy": "Emergency", "Emergncy": "Emergency", "ER": "Emergency", "EMERGENCY": "Emergency",
    "Surgey": "Surgery", "Surgeyr": "Surgery", "SURGERY": "Surgery",
    "icu": "ICU", "I.C.U.": "ICU", "IntensiveCare": "ICU",
    "Paediatrics": "Pediatrics", "Pedeatrics": "Pediatrics", "PEDS": "Pediatrics",
    "Maternty": "Maternity", "Maternitiy": "Maternity", "OB/GYN": "Maternity",
    "Oncolgy": "Oncology", "Oncologgy": "Oncology", "ONCO": "Oncology",
    "Cardiolgy": "Cardiology", "Cardilogy": "Cardiology", "CARDIO": "Cardiology"
}

dept_expr = col("department")
for typo, correct in department_mapping.items():
    dept_expr = when(col("department") == typo, lit(correct)).otherwise(dept_expr)

clean_df = clean_df.withColumn(
    "_dq_flags",
    when(
        ~col("department").isin(valid_departments) & col("department").isNotNull(),
        concat(col("_dq_flags"), lit("DEPT_TYPO|"))
    ).otherwise(col("_dq_flags"))
).withColumn("department", dept_expr)

# Fix invalid hospital IDs
clean_df = clean_df.withColumn(
    "_dq_flags",
    when(
        col("hospital_id").isNotNull() & ((col("hospital_id") < 1) | (col("hospital_id") > 7)),
        concat(col("_dq_flags"), lit("INVALID_HOSPITAL_ID|"))
    ).otherwise(col("_dq_flags"))
).withColumn(
    "hospital_id",
    when(
        (col("hospital_id") < 1) | (col("hospital_id") > 7),
        lit(None).cast("int")
    ).otherwise(col("hospital_id"))
)

# Flag missing critical fields
critical_fields = ["patient_id", "event_type", "department"]
for field in critical_fields:
    clean_df = clean_df.withColumn(
        "_dq_flags",
        when(
            col(field).isNull(),
            concat(col("_dq_flags"), lit(f"MISSING_{field.upper()}|"))
        ).otherwise(col("_dq_flags"))
    )

# Recalculate length of stay
clean_df = clean_df.withColumn(
    "length_of_stay_hrs",
    when(
        col("admission_time").isNotNull() & col("discharge_time").isNotNull(),
        round(
            (unix_timestamp("discharge_time") - unix_timestamp("admission_time")) / 3600, 2
        )
    ).otherwise(col("length_of_stay_hrs"))
)

# Clean up dq_flags
clean_df = clean_df.withColumn(
    "_dq_flags",
    when(col("_dq_flags") == "", lit("CLEAN"))
    .otherwise(rtrim(regexp_replace(col("_dq_flags"), "\\|$", "")))
)

# Add Silver metadata
clean_df = clean_df.withColumn("_silver_processed_at", current_timestamp())

# Deduplicate by event_id
deduped_df = clean_df.dropDuplicates(["event_id"])


# Schema evolution — add missing columns
expected_cols = [
    "event_type", "event_id", "patient_id", "gender", "age", "department",
    "hospital_id", "hospital_name", "bed_id", "admission_time", "discharge_time",
    "expected_discharge_time", "length_of_stay_hrs", "admission_priority",
    "diagnosis_code", "insurance_type", "discharge_status", "vitals",
    "event_timestamp", "_bronze_eventhub_ts", "_bronze_ingested_at",
    "_dq_flags", "_silver_processed_at"
]

for col_name in expected_cols:
    if col_name not in deduped_df.columns:
        deduped_df = deduped_df.withColumn(col_name, lit(None))

# Write to Silver (batch mode)
deduped_df.write.format("delta").mode("append").option("mergeSchema", "true").save(silver_path)

# Verify Silver data
silver_df = spark.read.format("delta").load(silver_path)
print(f"Silver record count: {silver_df.count()}")
silver_df.groupBy("_dq_flags").count().orderBy(col("count").desc()).show(truncate=False)