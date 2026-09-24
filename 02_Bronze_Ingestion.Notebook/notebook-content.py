# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "bb30511c-a76c-43c8-a971-f2e1fcf07633",
# META       "default_lakehouse_name": "Healthcare_Lakehouse",
# META       "default_lakehouse_workspace_id": "344fd862-10de-4536-bd27-379ac23c8d6d",
# META       "known_lakehouses": [
# META         {
# META           "id": "bb30511c-a76c-43c8-a971-f2e1fcf07633"
# META         }
# META       ]
# META     }
# META   }
# META }

# MARKDOWN ********************

# # Bronze Ingestion

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

RAW_PATH = (
    "Files/raw_api"
)

SOURCE_FILES = {
    "patients.csv": "bronze_patients",
    "doctors.csv": "bronze_doctors",
    "departments.csv": "bronze_departments",
    "schedules.csv": "bronze_schedules",
    "slots.csv": "bronze_slots",
    "appointments.csv": "bronze_appointments"
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ingestion Function

# CELL ********************

from datetime import datetime, timezone

def ingest_csv_to_bronze(
    file_name,
    table_name
):

    path = (
        f"{RAW_PATH}/{file_name}"
    )

    print(
        f"Reading: {path}"
    )

    df = (
        spark.read
        .option("header", "true")
        .option("inferSchema", "true")
        .csv(path)
    )

    ingestion_time = (
        datetime.now(
            timezone.utc
        ).isoformat()
    )

    df = (
        df
        .withColumn(
            "_source",
            F.lit("Data_Generation")
        )
        .withColumn(
            "_source_file",
            F.lit(file_name)
        )
        .withColumn(
            "_ingestion_timestamp",
            F.lit(ingestion_time)
        )
    )

    (
        df.write
        .format("delta")
        .mode("overwrite")
        .option(
            "overwriteSchema",
            "true"
        )
        .saveAsTable(table_name)
    )

    print(
        f"Created: {table_name}"
    )

    print(
        f"Rows: {df.count()}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Ingestion

# CELL ********************

for file_name, table_name in SOURCE_FILES.items():

    ingest_csv_to_bronze(
        file_name,
        table_name
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table_name in SOURCE_FILES.values():

    count = spark.table(
        table_name
    ).count()

    print(
        f"{table_name}: {count:,}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
