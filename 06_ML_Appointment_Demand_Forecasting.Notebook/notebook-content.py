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

import pandas as pd
import numpy as np

import matplotlib.pyplot as plt

print("Libraries loaded successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

appointments = spark.table("gold_fact_appointments")

display(appointments.limit(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

appointments.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

daily_demand = (
    appointments
    .groupBy("appointment_date")
    .agg(
        F.count("appointment_id").alias("appointment_count")
    )
    .orderBy("appointment_date")
)

display(daily_demand)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd = daily_demand.toPandas()

demand_pd["appointment_date"] = pd.to_datetime(
    demand_pd["appointment_date"]
)

demand_pd = demand_pd.sort_values(
    "appointment_date"
)

display(demand_pd.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Start:", demand_pd["appointment_date"].min())
print("End:", demand_pd["appointment_date"].max())
print("Days:", len(demand_pd))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(12, 5))

plt.plot(
    demand_pd["appointment_date"],
    demand_pd["appointment_count"]
)

plt.xlabel("Date")
plt.ylabel("Appointments")
plt.title("Daily Appointment Demand")

plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd["day_of_week"] = (
    demand_pd["appointment_date"].dt.dayofweek
)

demand_pd["day_of_month"] = (
    demand_pd["appointment_date"].dt.day
)

demand_pd["month"] = (
    demand_pd["appointment_date"].dt.month
)

demand_pd["week_of_year"] = (
    demand_pd["appointment_date"].dt.isocalendar().week.astype(int)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd["lag_1"] = (
    demand_pd["appointment_count"].shift(1)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd["lag_7"] = (
    demand_pd["appointment_count"].shift(7)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd["lag_14"] = (
    demand_pd["appointment_count"].shift(14)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_pd["rolling_7"] = (
    demand_pd["appointment_count"]
    .shift(1)
    .rolling(7)
    .mean()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

demand_ml = demand_pd.dropna().copy()

display(demand_ml.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_data = demand_ml[
    demand_ml["appointment_date"] < "2026-03-01"
].copy()

test_data = demand_ml[
    demand_ml["appointment_date"] >= "2026-03-01"
].copy()

print("Training rows:", len(train_data))
print("Testing rows:", len(test_data))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feature_columns = [
    "day_of_week",
    "day_of_month",
    "month",
    "week_of_year",
    "lag_1",
    "lag_7",
    "lag_14",
    "rolling_7"
]

X_train = train_data[feature_columns]
y_train = train_data["appointment_count"]

X_test = test_data[feature_columns]
y_test = test_data["appointment_count"]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import RandomForestRegressor

forecast_model = RandomForestRegressor(
    n_estimators=300,
    random_state=42
)

forecast_model.fit(
    X_train,
    y_train
)

print("Forecast model trained successfully")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

test_predictions = forecast_model.predict(X_test)

test_data["predicted_appointments"] = test_predictions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    test_data[
        [
            "appointment_date",
            "appointment_count",
            "predicted_appointments"
        ]
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.metrics import (
    mean_absolute_error,
    mean_squared_error
)

mae = mean_absolute_error(
    y_test,
    test_predictions
)

rmse = np.sqrt(
    mean_squared_error(
        y_test,
        test_predictions
    )
)

mape = np.mean(
    np.abs(
        (y_test - test_predictions) /
        y_test
    )
) * 100

print("MAE:", round(mae, 2))
print("RMSE:", round(rmse, 2))
print("MAPE:", round(mape, 2), "%")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

plt.figure(figsize=(12, 5))

plt.plot(
    test_data["appointment_date"],
    test_data["appointment_count"],
    label="Actual"
)

plt.plot(
    test_data["appointment_date"],
    test_data["predicted_appointments"],
    label="Predicted"
)

plt.xlabel("Date")
plt.ylabel("Appointments")
plt.title("Actual vs Predicted Appointment Demand")

plt.legend()
plt.xticks(rotation=45)
plt.tight_layout()

plt.show()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

final_X = demand_ml[feature_columns]
final_y = demand_ml["appointment_count"]

forecast_model.fit(
    final_X,
    final_y
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

last_date = demand_pd["appointment_date"].max()

future_dates = pd.date_range(
    start=last_date + pd.Timedelta(days=1),
    periods=14,
    freq="D"
)

future_df = pd.DataFrame({
    "appointment_date": future_dates
})

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

future_df["day_of_week"] = (
    future_df["appointment_date"].dt.dayofweek
)

future_df["day_of_month"] = (
    future_df["appointment_date"].dt.day
)

future_df["month"] = (
    future_df["appointment_date"].dt.month
)

future_df["week_of_year"] = (
    future_df["appointment_date"]
    .dt.isocalendar()
    .week
    .astype(int)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

history = demand_pd[
    ["appointment_date", "appointment_count"]
].copy()

future_predictions = []

for current_date in future_dates:

    day_of_week = current_date.dayofweek
    day_of_month = current_date.day
    month = current_date.month
    week_of_year = current_date.isocalendar().week

    lag_1 = history["appointment_count"].iloc[-1]

    lag_7 = history["appointment_count"].iloc[-7]

    lag_14 = history["appointment_count"].iloc[-14]

    rolling_7 = (
        history["appointment_count"]
        .iloc[-7:]
        .mean()
    )

    X_future = pd.DataFrame([{
        "day_of_week": day_of_week,
        "day_of_month": day_of_month,
        "month": month,
        "week_of_year": week_of_year,
        "lag_1": lag_1,
        "lag_7": lag_7,
        "lag_14": lag_14,
        "rolling_7": rolling_7
    }])

    prediction = forecast_model.predict(
        X_future
    )[0]

    prediction = max(0, round(prediction))

    future_predictions.append(prediction)

    history = pd.concat(
        [
            history,
            pd.DataFrame({
                "appointment_date": [current_date],
                "appointment_count": [prediction]
            })
        ],
        ignore_index=True
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_output = pd.DataFrame({
    "forecast_date": future_dates,
    "predicted_appointments": future_predictions
})

forecast_output["model_name"] = (
    "Random Forest"
)

forecast_output["model_version"] = "1.0"

forecast_output["prediction_date"] = (
    pd.Timestamp.now()
)

display(forecast_output)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

forecast_spark = spark.createDataFrame(
    forecast_output
)

(
    forecast_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("gold_ml_demand_forecast")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table(
        "gold_ml_demand_forecast"
    )
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
