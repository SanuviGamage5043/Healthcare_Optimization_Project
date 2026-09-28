# HealthFlow — Healthcare Appointment & Hospital Capacity Optimization

## 📌 Project Overview

**HealthFlow** is an end-to-end healthcare data engineering and analytics solution designed to analyze hospital appointment operations, waiting times, resource utilization, appointment demand, and patient no-shows.

The project uses **Microsoft Fabric** to implement a modern data engineering architecture based on the **Medallion Architecture (Bronze, Silver, Gold)**. The processed data is modeled using a semantic model and visualized through **Power BI**.

Machine learning is also integrated into the platform to:

* Predict the probability of patient no-shows.
* Forecast future appointment demand.

The project demonstrates an end-to-end workflow from synthetic data generation and automated data processing to analytics, machine learning, and business intelligence.

---

## 🎯 Objectives

The main objectives of HealthFlow are to:

* Build a scalable healthcare data engineering pipeline.
* Implement Bronze, Silver, and Gold data layers.
* Clean, transform, and model healthcare appointment data.
* Analyze hospital appointment demand and operational performance.
* Analyze waiting times and resource utilization.
* Identify patterns in patient no-shows.
* Predict future patient no-show risk.
* Forecast future appointment demand.
* Build interactive Power BI dashboards.
* Automate the complete data processing workflow using Microsoft Fabric Pipelines.
* Maintain project notebooks and artifacts using GitHub integration.

---

## 🏗️ Solution Architecture

```text
                    Synthetic Healthcare Data
                         MedScheduler
                              │
                              ▼
                    Data Generation Notebook
                              │
                              ▼
                 Microsoft Fabric Data Pipeline
                              │
                              ▼
                       ┌─────────────┐
                       │    BRONZE   │
                       │ Raw Data    │
                       └──────┬──────┘
                              │
                              ▼
                       ┌─────────────┐
                       │    SILVER   │
                       │ Cleaned &   │
                       │ Transformed │
                       └──────┬──────┘
                              │
                              ▼
                       ┌─────────────┐
                       │     GOLD    │
                       │ Analytics-  │
                       │ Ready Data  │
                       └──────┬──────┘
                              │
                ┌─────────────┴─────────────┐
                │                           │
                ▼                           ▼
        Machine Learning              Semantic Model
                │                           │
        ┌───────┴────────┐                  │
        │                │                  ▼
        ▼                ▼             Power BI
   No-show ML       Demand Forecast       │
        │                │                 │
        └───────┬────────┘                 │
                ▼                          │
         ML Gold Tables ───────────────────┘
```

---

## 🛠️ Technologies Used

### Data Engineering

* **Microsoft Fabric**
* Fabric Lakehouse
* Fabric Data Pipelines
* Apache Spark / PySpark
* Delta Lake
* Python
* Pandas
* NumPy

### Analytics & Visualization

* Microsoft Power BI
* Power BI Semantic Model
* DAX

### Machine Learning

* Scikit-learn
* XGBoost
* Logistic Regression
* Random Forest
* Classification
* Regression / Demand Forecasting

### Version Control

* Git
* GitHub
* Microsoft Fabric Git Integration

---

# 🗄️ Data Architecture

The project follows the **Medallion Architecture**.

## 🥉 Bronze Layer

The Bronze layer stores the raw data generated from the source.

Tables include:

```text
bronze_patients
bronze_doctors
bronze_departments
bronze_appointments
bronze_slots
bronze_schedules
```

The Bronze layer also includes ingestion metadata such as:

* Source information
* Source file
* Ingestion timestamp

---

## 🥈 Silver Layer

The Silver layer contains cleaned and transformed data.

Tables include:

```text
silver_patients
silver_doctors
silver_departments
silver_appointments
silver_slots
silver_schedules
```

Transformations include:

* Duplicate removal
* Null handling
* Data type conversion
* Date conversion
* Status standardization
* Appointment lead-time calculation
* Waiting-time calculation
* Consultation-duration calculation
* Data validation

---

## 🥇 Gold Layer

The Gold layer contains analytics-ready fact and dimension tables.

### Fact Tables

```text
gold_fact_appointments
gold_fact_waiting
gold_fact_capacity
gold_fact_doctor_capacity
```

### Dimension Tables

```text
gold_dim_patient
gold_dim_doctor
gold_dim_department
gold_dim_date
```

### Machine Learning Tables

```text
gold_ml_no_show_predictions
gold_ml_demand_forecast
ml_model_evaluation
```

---

# 📊 Data Model

The project uses a dimensional modeling approach.

```text
                    gold_dim_patient
                           │
                           │
gold_dim_doctor ─── gold_fact_appointments ─── gold_dim_department
                           │
                           │
                     gold_dim_date


gold_dim_department ─── gold_fact_capacity ─── gold_dim_date


gold_dim_doctor ─── gold_fact_doctor_capacity
                           │
gold_dim_department ───────┤
                           │
gold_dim_date ─────────────┘


gold_dim_patient ─── gold_fact_waiting
gold_dim_doctor ───────────┤
gold_dim_department ───────┤
gold_dim_date ─────────────┘
```

The model avoids direct fact-to-fact relationships and uses shared dimension tables for filtering and analysis.

---

# ⚙️ Data Pipeline

The complete workflow is orchestrated using a Microsoft Fabric Data Pipeline called:

```text
Healthcare_Data_Pipeline
```

The pipeline contains the following notebook activities:

```text
Data Generation
      ↓
Bronze
      ↓
Silver
      ↓
Gold
      ↓
 ┌────┴────┐
 ↓         ↓
No-show   Demand
ML        Forecast ML
```

### Pipeline Notebooks

```text
01_Data_Generation
02_Bronze
03_Silver
04_Gold
05_ML_No_Show_Prediction
06_ML_Appointment_Demand_Forecasting
```

The pipeline allows the entire data processing and machine learning workflow to be executed in sequence.

---

# 🤖 Machine Learning

HealthFlow contains two machine learning use cases.

## 1. Patient No-show Prediction

### Objective

Predict whether a scheduled appointment is likely to result in a patient no-show.

### Target

```text
no_show = 1 → No-show
no_show = 0 → Not a no-show
```

### Features

The model uses features available before the appointment takes place:

```text
lead_time_days
appointment_hour
day_of_week
department_id
appointment_type
previous_no_shows
previous_cancellations
```

Post-appointment information such as waiting time and consultation duration is not used as prediction features to avoid data leakage.

### Models

Three classification models are evaluated:

```text
Logistic Regression
Random Forest Classifier
XGBoost Classifier
```

### Evaluation Metrics

Models are evaluated using:

* Accuracy
* Precision
* Recall
* F1-score
* ROC-AUC

The final model is selected based on the evaluation results.

### Prediction Output

Predictions are stored in:

```text
gold_ml_no_show_predictions
```

The table contains information such as:

```text
appointment_id
patient_id
appointment_date
department_id
appointment_type
appointment_hour
no_show_probability
predicted_no_show
risk_category
model_name
model_version
prediction_date
```

Risk categories are used to group predicted probabilities into:

```text
Low
Medium
High
```

---

# 📈 2. Appointment Demand Forecasting

### Objective

Predict the expected number of appointments on future days.

The model works with daily appointment demand.

### Input

Historical daily appointment counts are generated from:

```text
gold_fact_appointments
```

### Features

The forecasting models use:

```text
day_of_week
day_of_month
month
week_of_year
lag_1
lag_7
lag_14
rolling_7
```

Lag features represent previous appointment demand, while the rolling average represents recent demand patterns.

### Models

Two regression models are evaluated:

```text
Random Forest Regressor
XGBoost Regressor
```

### Evaluation Metrics

The models are evaluated using:

* MAE
* RMSE
* MAPE

The model with the appropriate evaluation performance is selected for future forecasting.

### Forecast

The system generates a future appointment demand forecast, currently covering the next 14 days.

Predictions are stored in:

```text
gold_ml_demand_forecast
```

The table contains:

```text
forecast_date
predicted_appointments
model_name
model_version
prediction_date
```

---

# 📊 Power BI Dashboards

The project contains multiple Power BI report pages for operational and predictive analytics.

## 1. Executive Overview

Provides a high-level view of hospital operations.

Key metrics include:

* Total Appointments
* Average Waiting Time
* No-show Rate
* Cancellation Rate
* Doctor Utilization

Visualizations include:

* Appointment trend
* Department performance
* Waiting-time trend

---

## 2. Waiting Time Analysis

Analyzes patient waiting and consultation times.

Key metrics:

* Average Waiting Time
* Maximum Waiting Time
* Patients Waiting Over 30 Minutes
* Average Consultation Time

Analysis includes:

* Waiting time by department
* Waiting time by doctor
* Waiting time by appointment hour
* Waiting-time trend

---

## 3. Capacity & Resource Utilization

Analyzes hospital resource usage.

Key metrics:

* Available Slots
* Booked Slots
* Capacity Utilization
* Unused Slots
* Doctor Utilization

Analysis includes:

* Capacity utilization by department
* Doctor utilization
* Available vs booked slots
* Capacity utilization trend

---

## 4. Appointment Demand Analysis

Analyzes historical appointment demand.

Visualizations include:

* Appointment demand trend
* Appointments by department
* Appointments by appointment type
* Appointments by hour
* Appointment types by department

---

## 5. No-show Analytics

Analyzes historical patient no-show behavior.

Visualizations include:

* No-shows by department
* No-show rate by department
* No-shows by appointment type
* No-show rate by appointment type
* No-shows by appointment hour
* No-show rate trend

---

## 6. No-show Prediction

Displays machine learning predictions.

Key metrics include:

* Total Predictions
* High Risk Appointments
* Average Predicted Risk
* Historical No-show Rate
* Selected ML Model

Visualizations include:

* Predicted risk distribution
* Risk by department
* Risk by appointment type
* Predicted risk by appointment hour
* High-risk appointment table
* Model performance comparison

---

## 7. Appointment Demand Forecast

Displays future appointment demand predictions.

Key metrics include:

* Forecasted Appointments
* Average Daily Forecast
* Maximum Forecasted Demand
* Forecast Days
* Forecast Model

Visualizations include:

* Historical appointment demand
* Predicted appointment demand
* Highest forecasted demand days
* Daily forecast table

Slicers include:

* Forecast Date
* Forecast Model

---

# 📁 Repository Structure

```text
healthcare-data-engineering/
│
├── notebooks/
│   ├── 01_Data_Generation.ipynb
│   ├── 02_Bronze.ipynb
│   ├── 03_Silver.ipynb
│   ├── 04_Gold.ipynb
│   ├── 05_ML_No_Show_Prediction.ipynb
│   └── 06_ML_Appointment_Demand_Forecasting.ipynb
│
├── api/
│   └── README.md
│
├── data/
│   └── README.md
│
├── powerbi/
│   └── README.md
│
├── README.md
│
└── .gitignore
```

Large generated datasets and unnecessary temporary files should not be committed to the repository.

---

# 🔄 End-to-End Workflow

The complete HealthFlow workflow is:

```text
1. Generate healthcare data
            ↓
2. Store raw data
            ↓
3. Bronze ingestion
            ↓
4. Silver transformation
            ↓
5. Gold data modeling
            ↓
6. Build semantic model
            ↓
7. Generate ML features
            ↓
8. Train and evaluate ML models
            ↓
9. Generate predictions
            ↓
10. Store ML results in Gold
            ↓
11. Visualize results in Power BI
            ↓
12. Automate workflow using Fabric Pipeline
```

---

# 🔐 Data & Privacy

This project uses **synthetic healthcare data** for educational and demonstration purposes.

No real patient medical records or personally identifiable healthcare information are used.

The project is intended to demonstrate data engineering, analytics, machine learning, and business intelligence concepts.

---

# 🚀 Future Improvements

Possible future enhancements include:

* Real-time appointment data ingestion
* REST API integration
* Real-time hospital capacity monitoring
* Department-level demand forecasting
* More advanced time-series models
* Automated model retraining
* ML model monitoring
* Data quality monitoring
* Real-time Power BI dashboards
* Automated alerting for high-risk appointments
* Integration with hospital scheduling systems

---

# 🎓 Project Scope

This project demonstrates practical implementation of:

* Data ingestion
* Data transformation
* Medallion Architecture
* Lakehouse architecture
* Delta tables
* Dimensional modeling
* Data pipelines
* Machine learning
* Predictive analytics
* Demand forecasting
* Power BI
* DAX
* GitHub version control
* End-to-end data engineering

---



## ⭐ Summary

**HealthFlow** demonstrates how a healthcare organization can transform raw appointment data into actionable analytics and machine learning insights using Microsoft Fabric.

The solution combines:

**Data Engineering + Lakehouse + Medallion Architecture + Machine Learning + Power BI + Pipeline Automation**

to create an end-to-end healthcare analytics platform.
