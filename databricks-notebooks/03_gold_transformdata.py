from pyspark.sql import functions as F
from pyspark.sql.functions import (
    lit, col, expr, current_timestamp, to_timestamp,
    sha2, concat_ws, coalesce, monotonically_increasing_id,
    when, to_date, year, month, dayofmonth, dayofweek,
    quarter, date_format, unix_timestamp, round as spark_round
)
from pyspark.sql.types import *
from pyspark.sql import Window
from delta.tables import DeltaTable

storage_account_name = "${STORAGE_ACCOUNT_NAME}"  # Update with your storage account name
storage_account_key = dbutils.secrets.get(scope="${KEY_VAULT_SCOPE}", key="storageconnection")

spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    storage_account_key
)

# Paths
silver_path = f"abfss://silver@{storage_account_name}.dfs.core.windows.net/patient_flow"
gold_dim_patient = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_patient"
gold_dim_department = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_department"
gold_dim_date = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/dim_date"
gold_fact = f"abfss://gold@{storage_account_name}.dfs.core.windows.net/fact_patient_flow"

silver_df = spark.read.format("delta").load(silver_path)
print(f"Silver records loaded: {silver_df.count()}")

patient_window = Window.partitionBy("patient_id").orderBy(col("event_timestamp").desc())

latest_patients = (
    silver_df
    .withColumn("_row_num", F.row_number().over(patient_window))
    .filter(col("_row_num") == 1)
    .drop("_row_num")
)

incoming_patient = (
    latest_patients
    .select("patient_id", "gender", "age", "insurance_type")
    .withColumn("effective_from", current_timestamp())
    .withColumn(
        "_hash",
        sha2(concat_ws("||",
            coalesce(col("gender"), lit("NA")),
            coalesce(col("age").cast("string"), lit("NA")),
            coalesce(col("insurance_type"), lit("NA"))
        ), 256)
    )
)

# Initialize dim_patient if it doesn't exist
if not DeltaTable.isDeltaTable(spark, gold_dim_patient):
    (
        incoming_patient
        .withColumn("patient_sk", monotonically_increasing_id())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .select("patient_sk", "patient_id", "gender", "age", "insurance_type",
                "effective_from", "effective_to", "is_current")
        .write.format("delta").mode("overwrite").save(gold_dim_patient)
    )
    print("dim_patient initialized.")

else:
    # ── SCD Type 2 Logic ──
    target_patient = DeltaTable.forPath(spark, gold_dim_patient)

    target_patient_df = (
        spark.read.format("delta").load(gold_dim_patient)
        .filter(col("is_current") == True)
        .withColumn(
            "_target_hash",
            sha2(concat_ws("||",
                coalesce(col("gender"), lit("NA")),
                coalesce(col("age").cast("string"), lit("NA")),
                coalesce(col("insurance_type"), lit("NA"))
            ), 256)
        )
    )

    # Find changed records
    changes_df = (
        target_patient_df.alias("t")
        .join(incoming_patient.alias("i"), "patient_id")
        .filter(col("t._target_hash") != col("i._hash"))
        .select("t.patient_sk")
    )

    changed_keys = [row["patient_sk"] for row in changes_df.collect()]

    # Step 1: Expire old current records
    if changed_keys:
        target_patient.update(
            condition=col("is_current") == True,
            set={
                "is_current": lit(False),
                "effective_to": current_timestamp()
            }
        )
        # Only update the specific changed keys
        # Re-filter to only expire the ones that actually changed
        for key in changed_keys:
            target_patient.update(
                condition=(col("patient_sk") == key) & (col("is_current") == True),
                set={
                    "is_current": lit(False),
                    "effective_to": current_timestamp()
                }
            )

    # Step 2: Insert new and changed records
    inserts_df = (
        incoming_patient.alias("i")
        .join(
            target_patient_df.alias("t"),
            (col("i.patient_id") == col("t.patient_id")) & (col("t.is_current") == True),
            "left"
        )
        .filter(
            col("t.patient_id").isNull() |  # New patients
            (col("t._target_hash") != col("i._hash"))  # Changed patients
        )
        .select(
            col("i.patient_id"), col("i.gender"), col("i.age"),
            col("i.insurance_type"), col("i.effective_from")
        )
        .withColumn("patient_sk", monotonically_increasing_id())
        .withColumn("effective_to", lit(None).cast("timestamp"))
        .withColumn("is_current", lit(True))
        .select("patient_sk", "patient_id", "gender", "age", "insurance_type",
                "effective_from", "effective_to", "is_current")
    )

    insert_count = inserts_df.count()
    if insert_count > 0:
        inserts_df.write.format("delta").mode("append").save(gold_dim_patient)
        print(f"dim_patient: {insert_count} new/changed records inserted.")
    else:
        print("dim_patient: No changes detected.")

# Hospital name mapping (matches simulator config)
hospital_names = {
    1: "MHA Central Hospital",
    2: "MHA Lakeside Medical Center",
    3: "MHA Riverside General",
    4: "MHA Prairie Health",
    5: "MHA Summit Medical",
    6: "MHA Valley Care",
    7: "MHA Metro Hospital"
}

# Department capacity reference
dept_capacity = {
    "Emergency": 40, "Surgery": 30, "ICU": 20, "Pediatrics": 25,
    "Maternity": 20, "Oncology": 25, "Cardiology": 25
}

incoming_dept = (
    silver_df
    .select("department", "hospital_id", "hospital_name")
    .dropDuplicates(["department", "hospital_id"])
    .filter(col("department").isNotNull() & col("hospital_id").isNotNull())
)

# Add capacity from reference data
capacity_expr = F.create_map([lit(x) for item in dept_capacity.items() for x in item])
incoming_dept = (
    incoming_dept
    .withColumn("bed_capacity", capacity_expr[col("department")])
    .withColumn("department_sk", monotonically_increasing_id())
    .select("department_sk", "department", "hospital_id", "hospital_name", "bed_capacity")
)

incoming_dept.write.format("delta").mode("overwrite").save(gold_dim_department)
print(f"dim_department: {incoming_dept.count()} records written.")

# Get date range from Silver data
date_range = silver_df.select(
    F.min(to_date("admission_time")).alias("min_date"),
    F.max(to_date("admission_time")).alias("max_date")
).collect()[0]

# Generate date sequence
date_dim = (
    spark.sql(f"""
        SELECT explode(sequence(
            to_date('{date_range['min_date']}'),
            to_date('{date_range['max_date']}'),
            interval 1 day
        )) AS date_key
    """)
    .withColumn("year", year("date_key"))
    .withColumn("quarter", quarter("date_key"))
    .withColumn("month", month("date_key"))
    .withColumn("month_name", date_format("date_key", "MMMM"))
    .withColumn("day", dayofmonth("date_key"))
    .withColumn("day_of_week", dayofweek("date_key"))
    .withColumn("day_name", date_format("date_key", "EEEE"))
    .withColumn("is_weekend", when(dayofweek("date_key").isin(1, 7), True).otherwise(False))
)

date_dim.write.format("delta").mode("overwrite").save(gold_dim_date)
print(f"dim_date: {date_dim.count()} records written.")

# Read current dimensions
dim_patient_df = (
    spark.read.format("delta").load(gold_dim_patient)
    .filter(col("is_current") == True)
    .select(col("patient_sk"), "patient_id")
)

dim_dept_df = (
    spark.read.format("delta").load(gold_dim_department)
    .select(col("department_sk"), "department", "hospital_id")
)

# Build fact base from Silver
fact_base = (
    silver_df
    .select(
        "event_type", "event_id", "patient_id", "department", "hospital_id",
        "admission_time", "discharge_time", "expected_discharge_time",
        "length_of_stay_hrs", "admission_priority", "diagnosis_code",
        "discharge_status", "bed_id",
        # Flatten vitals for analytics
        col("vitals.heart_rate").alias("heart_rate"),
        col("vitals.bp_systolic").alias("bp_systolic"),
        col("vitals.bp_diastolic").alias("bp_diastolic"),
        col("vitals.temperature_f").alias("temperature_f"),
        col("vitals.oxygen_saturation").alias("oxygen_saturation")
    )
    .withColumn("admission_date", to_date("admission_time"))
)

# Join with dimensions to get surrogate keys
fact_enriched = (
    fact_base
    .join(dim_patient_df, on="patient_id", how="left")
    .join(dim_dept_df, on=["department", "hospital_id"], how="left")
)

# Add computed metrics
fact_final = (
    fact_enriched
    .withColumn(
        "is_currently_admitted",
        when(
            (col("event_type") == "ADMISSION") & col("discharge_time").isNull(),
            lit(True)
        ).otherwise(lit(False))
    )
    .withColumn(
        "length_of_stay_hrs",
        when(
            col("admission_time").isNotNull() & col("discharge_time").isNotNull(),
            spark_round(
                (unix_timestamp("discharge_time") - unix_timestamp("admission_time")) / 3600.0, 2
            )
        ).otherwise(col("length_of_stay_hrs"))
    )
    .withColumn("fact_id", monotonically_increasing_id())
    .withColumn("_gold_processed_at", current_timestamp())
    .select(
        "fact_id",
        "patient_sk",
        "department_sk",
        "event_type",
        "event_id",
        "patient_id",
        "admission_date",
        "admission_time",
        "discharge_time",
        "expected_discharge_time",
        "length_of_stay_hrs",
        "is_currently_admitted",
        "admission_priority",
        "diagnosis_code",
        "discharge_status",
        "bed_id",
        "heart_rate",
        "bp_systolic",
        "bp_diastolic",
        "temperature_f",
        "oxygen_saturation",
        "_gold_processed_at"
    )
)

# Write fact table
fact_final.write.format("delta").mode("overwrite").save(gold_fact)
print(f"fact_patient_flow: {fact_final.count()} records written.")

print("=" * 50)
print("GOLD LAYER SUMMARY")
print("=" * 50)
print(f"dim_patient     : {spark.read.format('delta').load(gold_dim_patient).count()} rows")
print(f"dim_department  : {spark.read.format('delta').load(gold_dim_department).count()} rows")
print(f"dim_date        : {spark.read.format('delta').load(gold_dim_date).count()} rows")
print(f"fact_patient_flow: {spark.read.format('delta').load(gold_fact).count()} rows")
print("=" * 50)

# Preview fact table
spark.read.format("delta").load(gold_fact).show(5, truncate=False)

# Preview patient dimension (SCD2 - show current records)
spark.read.format("delta").load(gold_dim_patient).filter(col("is_current") == True).show(5, truncate=False)

# Preview department dimension
spark.read.format("delta").load(gold_dim_department).show(truncate=False)

# Preview date dimension
spark.read.format("delta").load(gold_dim_date).show(5, truncate=False)