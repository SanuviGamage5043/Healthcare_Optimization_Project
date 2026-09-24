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

# CELL ********************

from pyspark.sql import functions as F

appointments = spark.table(
    "silver_appointments"
)

patients = spark.table(
    "silver_patients"
)

doctors = spark.table(
    "silver_doctors"
)

departments = spark.table(
    "silver_departments"
)

slots = spark.table(
    "silver_slots"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_patient = (
    patients
    .select(
        "patient_id",
        "patient_name",
        "gender",
        "date_of_birth"
    )
    .dropDuplicates(
        ["patient_id"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_dim_patient.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_dim_patient"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_doctor = (
    doctors
    .select(
        "doctor_id",
        "doctor_name",
        "department_id",
        "specialty"
    )
    .dropDuplicates(
        ["doctor_id"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_dim_doctor.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_dim_doctor"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_department = (
    departments
    .select(
        "department_id",
        "department_name",
        "specialty",
        "room_count"
    )
    .dropDuplicates(
        ["department_id"]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_dim_department.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_dim_department"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

date_bounds = (
    appointments
    .select(
        F.min(
            "appointment_date"
        ).alias("min_date"),
        F.max(
            "appointment_date"
        ).alias("max_date")
    )
    .collect()[0]
)

min_date = date_bounds["min_date"]
max_date = date_bounds["max_date"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_date = (
    spark.range(1)
    .select(
        F.explode(
            F.sequence(
                F.lit(min_date),
                F.lit(max_date),
                F.expr(
                    "INTERVAL 1 DAY"
                )
            )
        ).alias("date")
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_dim_date = (
    gold_dim_date
    .withColumn(
        "year",
        F.year("date")
    )
    .withColumn(
        "month",
        F.month("date")
    )
    .withColumn(
        "month_name",
        F.date_format(
            "date",
            "MMMM"
        )
    )
    .withColumn(
        "quarter",
        F.quarter("date")
    )
    .withColumn(
        "day",
        F.dayofmonth("date")
    )
    .withColumn(
        "day_of_week",
        F.dayofweek("date")
    )
    .withColumn(
        "day_name",
        F.date_format(
            "date",
            "EEEE"
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
    gold_dim_date.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_dim_date"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_appointments = (
    appointments
    .select(
        "appointment_id",
        "patient_id",
        "doctor_id",
        "department_id",
        "slot_id",
        "appointment_date",
        "booking_date",
        "scheduled_time",
        "appointment_hour",
        "appointment_type",
        "arrival_time",
        "consultation_start",
        "consultation_end",
        "status",
        "lead_time_days",
        "waiting_minutes",
        "consultation_minutes"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_fact_appointments.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_fact_appointments"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_waiting = (
    appointments
    .filter(
        F.col(
            "waiting_minutes"
        ).isNotNull()
    )
    .select(
        "appointment_id",
        "patient_id",
        "doctor_id",
        "department_id",
        "appointment_date",
        "arrival_time",
        "consultation_start",
        "waiting_minutes",
        "consultation_minutes"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_fact_waiting.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_fact_waiting"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

booked = (
    appointments
    .groupBy(
        "appointment_date",
        "department_id"
    )
    .agg(
        F.count(
            "*"
        ).alias(
            "booked_slots"
        ),

        F.sum(
            F.when(
                F.col("status")
                == "completed",
                1
            ).otherwise(0)
        ).alias(
            "completed_appointments"
        ),

        F.sum(
            F.when(
                F.col("status")
                == "no_show",
                1
            ).otherwise(0)
        ).alias(
            "no_show_appointments"
        ),

        F.sum(
            F.when(
                F.col("status")
                == "cancelled",
                1
            ).otherwise(0)
        ).alias(
            "cancelled_appointments"
        ),

        F.avg(
            "waiting_minutes"
        ).alias(
            "average_waiting_minutes"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

available_capacity = (
    slots
    .groupBy(
        "slot_date",
        "department_id"
    )
    .agg(
        F.count(
            "*"
        ).alias(
            "available_slots"
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_capacity = (
    available_capacity
    .withColumnRenamed(
        "slot_date",
        "appointment_date"
    )
    .join(
        booked,
        [
            "appointment_date",
            "department_id"
        ],
        "left"
    )
    .fillna(
        0,
        [
            "booked_slots",
            "completed_appointments",
            "no_show_appointments",
            "cancelled_appointments",
            "average_waiting_minutes"
        ]
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

gold_fact_capacity = (
    gold_fact_capacity
    .withColumn(
        "unused_capacity",
        F.col("available_slots")
        - F.col("booked_slots")
    )
    .withColumn(
        "capacity_utilization",
        F.when(
            F.col("available_slots") > 0,
            F.col("booked_slots")
            /
            F.col("available_slots")
        ).otherwise(0)
    )
    .withColumn(
        "attendance_rate",
        F.when(
            F.col("booked_slots") > 0,
            F.col("completed_appointments")
            /
            F.col("booked_slots")
        ).otherwise(0)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    gold_fact_capacity.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_fact_capacity"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

GOLD_TABLES = [
    "gold_fact_appointments",
    "gold_fact_waiting",
    "gold_fact_capacity",
    "gold_dim_patient",
    "gold_dim_doctor",
    "gold_dim_department",
    "gold_dim_date"
]

for table in GOLD_TABLES:

    count = spark.table(
        table
    ).count()

    print(
        f"{table}: {count:,} rows"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for table in [
    "gold_fact_appointments",
    "gold_fact_waiting",
    "gold_fact_capacity",
    "gold_dim_patient",
    "gold_dim_doctor",
    "gold_dim_department",
    "gold_dim_date"
]:
    print(f"\n{table}")
    spark.table(table).show(5)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

appointments = spark.table("gold_fact_appointments")

appointments.groupBy("status").count().show()

appointments.select(
    "lead_time_days",
    "waiting_minutes",
    "consultation_minutes"
).describe().show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
