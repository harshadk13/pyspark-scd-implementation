# pyspark-scd-implementation
PySpark implementations of SCD Type-1, Type-2, and Type-3

PySpark SCD Implementation
This repository contains PySpark implementations of Slowly Changing Dimension (SCD) Type-1, Type-2, and Type-3 for managing dimension table updates in a data warehouse.
Overview
Slowly Changing Dimensions (SCDs) handle data that changes infrequently, such as customer profiles. This repository demonstrates:

SCD Type-1: Overwrites existing data without retaining history.
SCD Type-2: Maintains full history by creating new records with validity dates and active flags.
SCD Type-3: Tracks limited history by adding columns for previous values.

Repository Structure
pyspark-scd-implementation/
├── src/
│   ├── scd_type1.py   # SCD Type-1 implementation
│   ├── scd_type2.py   # SCD Type-2 implementation
│   ├── scd_type3.py   # SCD Type-3 implementation
├── README.md          # Project documentation

Prerequisites

Python 3.8+
PySpark 3.5.0+
A Spark environment (e.g., Databricks, local Spark setup, or Azure Synapse)

Install PySpark:
pip install pyspark

Usage

Clone the Repository:
git clone https://github.com/harshadk13/pyspark-scd-implementation.git
cd pyspark-scd-implementation


Run the Scripts:Each script (scd_type1.py, scd_type2.py, scd_type3.py) can be executed using:
spark-submit src/scd_type1.py


Input Data:The scripts use sample employee data with columns id, name, location, and salary. Modify the data_full and data_daily lists in each script to use your own data.


SCD Implementations
SCD Type-1

Overwrites existing records with new data.
No history is maintained.
Example: Updates to location and salary replace old values.

SCD Type-2

Creates new records for changes, preserving history.
Uses Active_Flag, From_date, and To_date to track record validity.
Example: A change in location adds a new row with the updated value, marking the old row as inactive.

SCD Type-3

Tracks limited history by adding columns for previous values (e.g., Previous_Location, Previous_Salary).
Suitable for scenarios where only the most recent change needs to be tracked.
Example: A change in location updates the current value and stores the old value in Previous_Location.


LinkedIn: @Devikrishna R

License
This project is licensed under the MIT License.

