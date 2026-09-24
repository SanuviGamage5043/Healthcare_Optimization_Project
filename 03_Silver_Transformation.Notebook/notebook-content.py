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

# # Silver Transformation

# CELL ********************

from pyspark.sql import functions as F

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Read Bronze

# CELL ********************

patients = spark.table(
    "bronze_patients"
)

doctors = spark.table(
    "bronze_doctors"
)

departments = spark.table(
    "bronze_departments"
)

schedules = spark.table(
    "bronze_schedules"
)

slots = spark.table(
    "bronze_slots"
)

appointments = spark.table(
    "bronze_appointments"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_patients = (
    patients
    .dropDuplicates(
        ["patient_id"]
    )
    .filter(
        F.col("patient_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_patients.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_patients"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_doctors = (
    doctors
    .dropDuplicates(
        ["doctor_id"]
    )
    .filter(
        F.col("doctor_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_doctors.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_doctors"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_departments = (
    departments
    .dropDuplicates(
        ["department_id"]
    )
    .filter(
        F.col("department_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_departments.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_departments"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_schedules = (
    schedules
    .dropDuplicates(
        ["schedule_id"]
    )
    .filter(
        F.col("schedule_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_schedules.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_schedules"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_slots = (
    slots
    .dropDuplicates(
        ["slot_id"]
    )
    .filter(
        F.col("slot_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_slots = (
    silver_slots
    .withColumn(
        "slot_date",
        F.to_date("slot_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_slots.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_slots"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    appointments
    .dropDuplicates(
        ["appointment_id"]
    )
    .filter(
        F.col("appointment_id").isNotNull()
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "booking_date",
        F.to_date("booking_date")
    )
    .withColumn(
        "appointment_date",
        F.to_date("appointment_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "appointment_hour",
        F.col("appointment_hour").cast("integer")
    )
    .withColumn(
        "booking_date",
        F.to_date("booking_date")
    )
    .withColumn(
        "appointment_date",
        F.to_date("appointment_date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "lead_time_days",
        F.datediff(
            "appointment_date",
            "booking_date"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "waiting_minutes",
        (
            F.unix_timestamp(
                "consultation_start",
                "HH:mm"
            )
            -
            F.unix_timestamp(
                "arrival_time",
                "HH:mm"
            )
        ) / 60
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "consultation_minutes",
        (
            F.unix_timestamp(
                "consultation_end",
                "HH:mm"
            )
            -
            F.unix_timestamp(
                "consultation_start",
                "HH:mm"
            )
        ) / 60
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

silver_appointments = (
    silver_appointments
    .withColumn(
        "status",
        F.lower(
            F.trim(
                F.col("status")
            )
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    silver_appointments.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "silver_appointments"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Invalid patient IDs:",
    silver_appointments
    .join(
        silver_patients.select(
            "patient_id"
        ),
        "patient_id",
        "left_anti"
    )
    .count()
)

print(
    "Invalid doctor IDs:",
    silver_appointments
    .join(
        silver_doctors.select(
            "doctor_id"
        ),
        "doctor_id",
        "left_anti"
    )
    .count()
)

print(
    "Invalid department IDs:",
    silver_appointments
    .join(
        silver_departments.select(
            "department_id"
        ),
        "department_id",
        "left_anti"
    )
    .count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
