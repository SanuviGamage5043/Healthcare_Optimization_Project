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

# # **Data Generation**

# MARKDOWN ********************

# ## Imports

# CELL ********************

import pandas as pd
import numpy as np

from datetime import datetime, timedelta, date, time

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Configuration

# CELL ********************

# ==============================
# HOSPITAL CONFIGURATION
# ==============================

NUM_PATIENTS = 5000
NUM_DOCTORS = 50

START_DATE = date(2026, 1, 1)
END_DATE = date(2026, 3, 31)

WORK_START_HOUR = 8
WORK_END_HOUR = 16

SLOT_DURATION_MINUTES = 30

SEED = 42

np.random.seed(SEED)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Departments

# CELL ********************

departments = pd.DataFrame([
    {
        "department_id": "D001",
        "department_name": "Cardiology",
        "specialty": "Cardiology",
        "room_count": 5
    },
    {
        "department_id": "D002",
        "department_name": "Orthopedics",
        "specialty": "Orthopedics",
        "room_count": 4
    },
    {
        "department_id": "D003",
        "department_name": "Pediatrics",
        "specialty": "Pediatrics",
        "room_count": 5
    },
    {
        "department_id": "D004",
        "department_name": "Dermatology",
        "specialty": "Dermatology",
        "room_count": 3
    },
    {
        "department_id": "D005",
        "department_name": "Neurology",
        "specialty": "Neurology",
        "room_count": 3
    },
    {
        "department_id": "D006",
        "department_name": "General Medicine",
        "specialty": "General Medicine",
        "room_count": 6
    },
    {
        "department_id": "D007",
        "department_name": "ENT",
        "specialty": "ENT",
        "room_count": 3
    },
    {
        "department_id": "D008",
        "department_name": "Ophthalmology",
        "specialty": "Ophthalmology",
        "room_count": 3
    },
    {
        "department_id": "D009",
        "department_name": "Gynecology",
        "specialty": "Gynecology",
        "room_count": 4
    },
    {
        "department_id": "D010",
        "department_name": "Psychiatry",
        "specialty": "Psychiatry",
        "room_count": 2
    }
])

display(departments)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Doctors

# CELL ********************

doctor_names = [
    "Silva", "Perera", "Fernando", "De Silva",
    "Bandara", "Jayasinghe", "Gunawardena",
    "Wijesinghe", "Senanayake", "Rathnayake"
]

doctors = []

for i in range(NUM_DOCTORS):

    doctor_id = f"DOC{i+1:03d}"

    department = departments.iloc[
        i % len(departments)
    ]

    name = (
        "Dr. "
        + doctor_names[i % len(doctor_names)]
        + f" {i+1}"
    )

    doctors.append({
        "doctor_id": doctor_id,
        "doctor_name": name,
        "department_id": department["department_id"],
        "specialty": department["specialty"]
    })

doctors = pd.DataFrame(doctors)

display(doctors.head(10))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Patients

# CELL ********************

first_names = [
    "Kasun", "Nimal", "Amal", "Saman",
    "Kavindu", "Dilshan", "Tharindu",
    "Chamari", "Nadeesha", "Sanduni",
    "Ishara", "Sachini"
]

last_names = [
    "Perera", "Silva", "Fernando",
    "Bandara", "Jayasinghe",
    "Gunawardena", "Rathnayake"
]

patients = []

for i in range(NUM_PATIENTS):

    patient_id = f"PAT{i+1:05d}"

    gender = np.random.choice(
        ["Male", "Female"]
    )

    birth_year = np.random.randint(
        1945,
        2010
    )

    birth_date = date(
        birth_year,
        np.random.randint(1, 13),
        np.random.randint(1, 28)
    )

    patients.append({
        "patient_id": patient_id,
        "patient_name": (
            np.random.choice(first_names)
            + " "
            + np.random.choice(last_names)
        ),
        "gender": gender,
        "date_of_birth": birth_date
    })

patients = pd.DataFrame(patients)

display(patients.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate Dates

# CELL ********************

date_range = pd.date_range(
    START_DATE,
    END_DATE,
    freq="D"
)

working_dates = [
    d.date()
    for d in date_range
    if d.weekday() < 5
]

print(
    "Working days:",
    len(working_dates)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate Schedules

# CELL ********************

schedules = []

schedule_id = 1

for work_date in working_dates:

    for _, doctor in doctors.iterrows():

        schedules.append({
            "schedule_id": f"SCH{schedule_id:06d}",
            "doctor_id": doctor["doctor_id"],
            "department_id": doctor["department_id"],
            "schedule_date": work_date,
            "start_time": f"{WORK_START_HOUR:02d}:00",
            "end_time": f"{WORK_END_HOUR:02d}:00",
            "status": "available"
        })

        schedule_id += 1

schedules = pd.DataFrame(schedules)

display(schedules.head())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate appointment slots

# CELL ********************

slots = []

slot_id = 1

current_date = START_DATE

while current_date <= END_DATE:

    if current_date.weekday() < 5:

        for _, doctor in doctors.iterrows():

            current_time = WORK_START_HOUR * 60
            end_time = WORK_END_HOUR * 60

            while current_time < end_time:

                hour = current_time // 60
                minute = current_time % 60

                slots.append({
                    "slot_id": f"SLOT{slot_id:07d}",
                    "doctor_id": doctor["doctor_id"],
                    "department_id": doctor["department_id"],
                    "slot_date": current_date,
                    "slot_time": (
                        f"{hour:02d}:{minute:02d}"
                    ),
                    "duration_minutes": SLOT_DURATION_MINUTES,
                    "slot_status": "available"
                })

                slot_id += 1

                current_time += SLOT_DURATION_MINUTES

    current_date += timedelta(days=1)

slots = pd.DataFrame(slots)

display(slots.head())

print(
    "Total slots:",
    len(slots)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Generate appointments

# CELL ********************

FILL_RATE = 0.70

appointment_slots = slots[
    np.random.rand(len(slots)) < FILL_RATE
].copy()

appointment_slots = appointment_slots.reset_index(
    drop=True
)

# Appointment types for the single hospital
APPOINTMENT_TYPES = [
    "General Consultation",
    "Specialist Consultation",
    "Follow-up",
    "Routine Check-up"
]

# Probability of each appointment type
APPOINTMENT_TYPE_PROBS = [
    0.40,   # General Consultation
    0.25,   # Specialist Consultation
    0.25,   # Follow-up
    0.10    # Routine Check-up
]

appointments = []

for i, slot in appointment_slots.iterrows():

    patient = patients.sample(
        1
    ).iloc[0]

    booking_days_before = np.random.randint(
        1,
        31
    )

    appointment_date = slot["slot_date"]

    booking_date = (
        appointment_date
        - timedelta(
            days=int(booking_days_before)
        )
    )

    status = np.random.choice(
        [
            "completed",
            "completed",
            "completed",
            "completed",
            "no_show",
            "cancelled"
        ]
    )

    # Generate appointment type
    appointment_type = np.random.choice(
        APPOINTMENT_TYPES,
        p=APPOINTMENT_TYPE_PROBS
    )

    # Extract hour from scheduled time
    appointment_hour = pd.to_datetime(
        slot["slot_time"]
    ).hour

    appointments.append({

        "appointment_id":
            f"APT{i+1:07d}",

        "patient_id":
            patient["patient_id"],

        "doctor_id":
            slot["doctor_id"],

        "department_id":
            slot["department_id"],

        "slot_id":
            slot["slot_id"],

        "booking_date":
            booking_date,

        "appointment_date":
            appointment_date,

        "scheduled_time":
            slot["slot_time"],

        "appointment_hour":
            appointment_hour,

        "appointment_type":
            appointment_type,

        "status":
            status
    })

appointments = pd.DataFrame(
    appointments
)

display(appointments.head())

print(
    "Total appointments:",
    len(appointments)
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Add arrival and consultation times

# CELL ********************

def create_time(
    time_string,
    extra_minutes=0
):

    hour, minute = map(
        int,
        time_string.split(":")
    )

    total_minutes = (
        hour * 60
        + minute
        + extra_minutes
    )

    new_hour = total_minutes // 60
    new_minute = total_minutes % 60

    return (
        f"{new_hour:02d}:{new_minute:02d}"
    )


arrival_times = []
consultation_starts = []
consultation_ends = []

for _, row in appointments.iterrows():

    if row["status"] == "completed":

        # Patient arrives between
        # 5 and 45 minutes before/after scheduled time
        arrival_offset = np.random.randint(
            -10,
            31
        )

        waiting_time = np.random.randint(
            5,
            91
        )

        consultation_duration = np.random.randint(
            15,
            61
        )

        arrival = create_time(
            row["scheduled_time"],
            arrival_offset
        )

        consultation_start = create_time(
            row["scheduled_time"],
            arrival_offset + waiting_time
        )

        consultation_end = create_time(
            row["scheduled_time"],
            arrival_offset
            + waiting_time
            + consultation_duration
        )

    else:

        arrival = None
        consultation_start = None
        consultation_end = None

    arrival_times.append(arrival)
    consultation_starts.append(
        consultation_start
    )
    consultation_ends.append(
        consultation_end
    )


appointments["arrival_time"] = arrival_times

appointments[
    "consultation_start"
] = consultation_starts

appointments[
    "consultation_end"
] = consultation_ends

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ## Save Generated data

# CELL ********************

tables = {
    "patients": patients,
    "doctors": doctors,
    "departments": departments,
    "schedules": schedules,
    "slots": slots,
    "appointments": appointments
}

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************


# CELL ********************

for name, df in tables.items():

    path = (
        f"/lakehouse/default/Files/"
        f"raw_api/{name}.csv"
    )

    df.to_csv(
        path,
        index=False
    )

    print(
        f"Saved: {path}"
    )

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print("PATIENTS:", len(patients))
print("DOCTORS:", len(doctors))
print("DEPARTMENTS:", len(departments))
print("SCHEDULES:", len(schedules))
print("SLOTS:", len(slots))
print("APPOINTMENTS:", len(appointments))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

print(
    appointments["status"]
    .value_counts()
)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
