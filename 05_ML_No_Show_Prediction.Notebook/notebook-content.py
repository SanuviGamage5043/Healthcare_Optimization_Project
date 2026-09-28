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
from pyspark.sql.window import Window

import pandas as pd
import numpy as np

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

display(
    appointments
    .groupBy("status")
    .count()
    .orderBy("status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    appointments.select(
        "appointment_id",
        "appointment_date",
        "appointment_hour",
        "appointment_type",
        "status",
        "lead_time_days"
    ).limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = (
    appointments
    .withColumn(
        "no_show",
        F.when(F.col("status") == "no_show", 1)
         .otherwise(0)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    ml_data
    .groupBy("status", "no_show")
    .count()
    .orderBy("status")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = ml_data.withColumn(
    "day_of_week",
    F.dayofweek("appointment_date")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

patient_window = (
    Window
    .partitionBy("patient_id")
    .orderBy("appointment_date")
    .rowsBetween(
        Window.unboundedPreceding,
        -1
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = (
    ml_data
    .withColumn(
        "previous_no_shows",
        F.sum(
            F.when(
                F.col("status") == "no_show",
                1
            ).otherwise(0)
        ).over(patient_window)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = (
    ml_data
    .withColumn(
        "previous_cancellations",
        F.sum(
            F.when(
                F.col("status") == "cancelled",
                1
            ).otherwise(0)
        ).over(patient_window)
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = (
    ml_data
    .fillna({
        "previous_no_shows": 0,
        "previous_cancellations": 0
    })
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    ml_data.select(
        "patient_id",
        "appointment_date",
        "status",
        "previous_no_shows",
        "previous_cancellations"
    )
    .orderBy(
        "patient_id",
        "appointment_date"
    )
    .limit(30)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_data = ml_data.select(
    "appointment_id",
    "patient_id",
    "appointment_date",
    "lead_time_days",
    "appointment_hour",
    "day_of_week",
    "department_id",
    "appointment_type",
    "previous_no_shows",
    "previous_cancellations",
    "no_show"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

for column in ml_data.columns:
    null_count = (
        ml_data
        .filter(F.col(column).isNull())
        .count()
    )

    print(
        f"{column}: {null_count} missing values"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    ml_data
    .groupBy("no_show")
    .count()
    .withColumn(
        "percentage",
        F.round(
            F.col("count") /
            ml_data.count() * 100,
            2
        )
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

np.random.choice(
    [
        "completed",
        "completed",
        "completed",
        "completed",
        "no_show",
        "cancelled"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_pd = ml_data.toPandas()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("Rows:", len(ml_pd))
print("Columns:", ml_pd.columns.tolist())

display(ml_pd.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

ml_pd["appointment_date"] = pd.to_datetime(
    ml_pd["appointment_date"]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    ml_pd["appointment_date"].min()
)

print(
    ml_pd["appointment_date"].max()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

train_data = ml_pd[
    ml_pd["appointment_date"]
    < "2026-03-01"
].copy()

test_data = ml_pd[
    ml_pd["appointment_date"]
    >= "2026-03-01"
].copy()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Training records:",
    len(train_data)
)

print(
    "Testing records:",
    len(test_data)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

feature_columns = [
    "lead_time_days",
    "appointment_hour",
    "day_of_week",
    "department_id",
    "appointment_type",
    "previous_no_shows",
    "previous_cancellations"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

X_train = train_data[
    feature_columns
]

y_train = train_data[
    "no_show"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

X_test = test_data[
    feature_columns
]

y_test = test_data[
    "no_show"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.compose import ColumnTransformer
from sklearn.preprocessing import OneHotEncoder
from sklearn.pipeline import Pipeline

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

numeric_features = [
    "lead_time_days",
    "appointment_hour",
    "day_of_week",
    "previous_no_shows",
    "previous_cancellations"
]

categorical_features = [
    "department_id",
    "appointment_type"
]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

preprocessor = ColumnTransformer(
    transformers=[
        (
            "num",
            "passthrough",
            numeric_features
        ),
        (
            "cat",
            OneHotEncoder(
                handle_unknown="ignore"
            ),
            categorical_features
        )
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.linear_model import LogisticRegression

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logistic_model = Pipeline(
    steps=[
        (
            "preprocessor",
            preprocessor
        ),
        (
            "model",
            LogisticRegression(
                max_iter=1000,
                class_weight="balanced"
            )
        )
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

logistic_model.fit(
    X_train,
    y_train
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.ensemble import RandomForestClassifier

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

random_forest_model = Pipeline(
    steps=[
        (
            "preprocessor",
            preprocessor
        ),
        (
            "model",
            RandomForestClassifier(
                n_estimators=200,
                random_state=42,
                class_weight="balanced"
            )
        )
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

random_forest_model.fit(
    X_train,
    y_train
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from xgboost import XGBClassifier

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

xgb_model = Pipeline(
    steps=[
        (
            "preprocessor",
            preprocessor
        ),
        (
            "model",
            XGBClassifier(
                n_estimators=200,
                max_depth=5,
                learning_rate=0.05,
                random_state=42,
                eval_metric="logloss"
            )
        )
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

xgb_model.fit(
    X_train,
    y_train
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from sklearn.metrics import (
    accuracy_score,
    precision_score,
    recall_score,
    f1_score,
    roc_auc_score,
    confusion_matrix,
    classification_report
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def evaluate_model(
    model,
    X_test,
    y_test,
    model_name
):

    predictions = model.predict(
        X_test
    )

    probabilities = model.predict_proba(
        X_test
    )[:, 1]

    print("=" * 50)
    print(model_name)
    print("=" * 50)

    print(
        "Accuracy:",
        round(
            accuracy_score(
                y_test,
                predictions
            ),
            4
        )
    )

    print(
        "Precision:",
        round(
            precision_score(
                y_test,
                predictions,
                zero_division=0
            ),
            4
        )
    )

    print(
        "Recall:",
        round(
            recall_score(
                y_test,
                predictions,
                zero_division=0
            ),
            4
        )
    )

    print(
        "F1 Score:",
        round(
            f1_score(
                y_test,
                predictions,
                zero_division=0
            ),
            4
        )
    )

    print(
        "ROC-AUC:",
        round(
            roc_auc_score(
                y_test,
                probabilities
            ),
            4
        )
    )

    print("\nConfusion Matrix:")
    print(
        confusion_matrix(
            y_test,
            predictions
        )
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

evaluate_model(
    logistic_model,
    X_test,
    y_test,
    "Logistic Regression"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

evaluate_model(
    random_forest_model,
    X_test,
    y_test,
    "Random Forest"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

evaluate_model(
    xgb_model,
    X_test,
    y_test,
    "XGBoost"
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

def get_metrics(
    model,
    X_test,
    y_test,
    model_name
):

    predictions = model.predict(
        X_test
    )

    probabilities = model.predict_proba(
        X_test
    )[:, 1]

    return {
        "Model": model_name,
        "Accuracy": accuracy_score(
            y_test,
            predictions
        ),
        "Precision": precision_score(
            y_test,
            predictions,
            zero_division=0
        ),
        "Recall": recall_score(
            y_test,
            predictions,
            zero_division=0
        ),
        "F1": f1_score(
            y_test,
            predictions,
            zero_division=0
        ),
        "ROC_AUC": roc_auc_score(
            y_test,
            probabilities
        )
    }

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results = []

results.append(
    get_metrics(
        logistic_model,
        X_test,
        y_test,
        "Logistic Regression"
    )
)

results.append(
    get_metrics(
        random_forest_model,
        X_test,
        y_test,
        "Random Forest"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results.append(
    get_metrics(
        xgb_model,
        X_test,
        y_test,
        "XGBoost"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

results_df = pd.DataFrame(
    results
)

display(results_df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

evaluation_spark = spark.createDataFrame(results_df)

evaluation_spark = evaluation_spark.select(
    F.col("Model").alias("model_name"),
    F.col("Accuracy").alias("accuracy"),
    F.col("Precision").alias("precision"),
    F.col("Recall").alias("recall"),
    F.col("F1").alias("f1_score"),
    F.col("ROC_AUC").alias("roc_auc")
)

display(evaluation_spark)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    evaluation_spark.write
    .format("delta")
    .mode("overwrite")
    .option("overwriteSchema", "true")
    .saveAsTable("ml_model_evaluation")
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    spark.table("ml_model_evaluation")
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

# CELL ********************

best_model = random_forest_model
best_model_name = "Random Forest"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions = best_model.predict(
    X_test
)

probabilities = best_model.predict_proba(
    X_test
)[:, 1]

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_output = test_data[
    [
        "appointment_id",
        "patient_id",
        "appointment_date",
        "department_id",
        "appointment_type",
        "appointment_hour"
    ]
].copy()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_output[
    "no_show_probability"
] = probabilities

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_output[
    "predicted_no_show"
] = predictions

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_output[
    "prediction_date"
] = pd.Timestamp.now()

prediction_output[
    "model_name"
] = best_model_name

prediction_output[
    "model_version"
] = "1.0"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    prediction_output.head(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_output[
    "risk_category"
] = pd.cut(
    prediction_output[
        "no_show_probability"
    ],
    bins=[
        -0.01,
        0.30,
        0.60,
        1.00
    ],
    labels=[
        "Low",
        "Medium",
        "High"
    ]
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_spark = spark.createDataFrame(
    prediction_output
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

prediction_spark.printSchema()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

(
    prediction_spark.write
    .format("delta")
    .mode("overwrite")
    .option(
        "overwriteSchema",
        "true"
    )
    .saveAsTable(
        "gold_ml_no_show_predictions"
    )
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

predictions_check = spark.table(
    "gold_ml_no_show_predictions"
)

display(
    predictions_check.limit(20)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    "Prediction records:",
    predictions_check.count()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(
    predictions_check
    .groupBy("risk_category")
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
