from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type1").getOrCreate()

# Full dataset (historical data)
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
    ["4", "Elsa", "Winnipeg", "10000"],
    ["5", "Olaf", "Toronto", "80000"],
    ["2", "Max", "Alberta", "45565"]
]
df_daily_update = spark.createDataFrame(data_daily, columns)

# Display input DataFrames
print("Full data:")
df_full.show()
print("Daily updates:")
df_daily_update.show()

# Perform SCD Type-1: Overwrite with new data
result = df_full.join(df_daily_update, "id", "full_outer").select(
    coalesce(df_full.id, df_daily_update.id).alias("ID"),
    coalesce(df_daily_update.name, df_full.name).alias("Name"),
    coalesce(df_daily_update.location, df_full.location).alias("Location"),
    coalesce(df_daily_update.salary, df_full.salary).alias("Salary")
)

# Display result
print("SCD Type-1 Result:")
result.show()

# Stop Spark session
spark.stop()
