from pyspark.sql.functions import *
from pyspark.sql.types import *

event_hub_namespace = "${EVENT_HUB_NAMESPACE}"  # e.g., healthcare-analytics-namespace.servicebus.windows.net
event_hub_name = "${EVENT_HUB_NAME}"  # e.g., healthcare-analytics-eh
event_hub_conn_str = dbutils.secrets.get(scope="${KEY_VAULT_SCOPE}", key="eventhubconnection")

storage_account_name = "${STORAGE_ACCOUNT_NAME}"  # Update with your storage account name
# storage_account_key = dbutils.secrets.get(scope="healthcare-analytics", key="storageconnection")
 
# COMMAND ----------

kafka_options = {
    "kafka.bootstrap.servers": f"{event_hub_namespace}:9093",
    "subscribe": event_hub_name,
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": (
        'kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required '
        f'username="$ConnectionString" password="{event_hub_conn_str}";'
    ),
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "maxOffsetsPerTrigger": "1000"  # Control micro-batch size to manage costs on free tier
}

# Read from Event Hub
raw_df = (
    spark.readStream
    .format("kafka")
    .options(**kafka_options)
    .load()
)

bronze_df = (
    raw_df
    .select(
        col("key").cast("string").alias("event_key"),
        col("value").cast("string").alias("raw_json"),
        col("topic"),
        col("partition"),
        col("offset"),
        col("timestamp").alias("eventhub_timestamp"),
        current_timestamp().alias("ingested_at"),       # When Databricks picked it up
        lit("kafka-eventhub").alias("source_file")           # Source lineage
    )
)

# ADLS authentication
spark.conf.set(
    f"fs.azure.account.key.{storage_account_name}.dfs.core.windows.net",
    dbutils.secrets.get(scope="${KEY_VAULT_SCOPE}", key="storageconnection")
)

# Bronze layer path
bronze_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/patient_flow"
checkpoint_path = f"abfss://bronze@{storage_account_name}.dfs.core.windows.net/_checkpoints/patient_flow"

bronze_stream = (
    bronze_df
    .writeStream
    .format("delta")
    .outputMode("append")
    .option("checkpointLocation", checkpoint_path)
    .option("mergeSchema", "true")           # Handle schema evolution gracefully
    .trigger(processingTime="30 seconds")    # Cost-friendly micro-batch interval for free tier
    .queryName("bronze_patient_flow")        # Named query for monitoring in Spark UI
    .start(bronze_path)
)