from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, to_date, current_date, hash

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type2").getOrCreate()

# Full dataset
data_full = [
    ["1", "Flynn", "Alberta", "42000"],
    ["2", "Max", "Alberta", "45565"],
    ["3", "Ana", "Alberta", "52000"],
    ["4", "Elsa", "Alberta", "35000"]
]
columns = ["id", "name", "location", "salary"]
df_full = spark.createDataFrame(data_full, columns)

# Daily updates
data_daily = [
    ["3", "Ana", "Calgary", "65000"],
    ["4", "Elsa", "Winnipeg", None],
    ["5", "Olaf", "Toronto", "80000"],
    ["2", "Max", "Alberta", "45565"]
]
df_daily = spark.createDataFrame(data_daily, columns)

# Add control columns
df_full = df_full.withColumn("Active_Flag", lit("Y")) \
                 .withColumn("From_date", to_date(current_date())) \
                 .withColumn("To_date", lit(None))
df_daily = df_daily.withColumn("Active_Flag", lit("Y")) \
                   .withColumn("From_date", to_date(current_date())) \
                   .withColumn("To_date", lit(None))

# Display input DataFrames
print("Full data with control columns:")
df_full.show()
print("Daily updates with control columns:")
df_daily.show()

# Identify changed records
update_ds = df_full.join(
    df_daily,
    (df_full.id == df_daily.id) & (df_full.Active_Flag == "Y"),
    "inner"
).filter(
    hash(df_full.name, df_full.location, df_full.salary) != hash(df_daily.name, df_daily.location, df_daily.salary)
).select(
    df_full.id,
    df_full.name,
    df_full.location,
    df_full.salary,
    lit("N").alias("Active_Flag"),
    df_full.From_date,
    to_date(current_date()).alias("To_date")
)

# Identify unchanged records
no_change = df_full.join(
    update_ds,
    (df_full.id == update_ds.id) & (df_full.Active_Flag == "Y"),
    "left_anti"
)

# Identify new records
new_records = df_daily.join(
    df_full,
    df_daily.id == df_full.id,
    "left_anti"
)

# Combine all records
final_result = no_change.union(update_ds).union(new_records)

# Display result
print("SCD Type-2 Result:")
final_result.show()

# Stop Spark session
spark.stop()
