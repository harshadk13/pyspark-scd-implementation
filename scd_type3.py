from pyspark.sql import SparkSession
from pyspark.sql.functions import coalesce, col

# Initialize Spark session
spark = SparkSession.builder.appName("SCD_Type3").getOrCreate()

# Full dataset (historical data with previous columns)
data_full = [
    ["1", "Flynn", "Alberta", None, "42000", None],
    ["2", "Max", "Alberta", None, "45565", None],
    ["3", "Ana", "Alberta", None, "52000", None],
    ["4", "Elsa", "Alberta", None, "35000", None]
]
columns = ["id", "name", "location", "previous_location", "salary", "previous_salary"]
df_full = spark.createDataFrame(data_full, columns)

# Daily updates
data_daily = [
    ["3", "Ana", "Calgary", "65000"],
    ["4", "Elsa", "Winnipeg", "10000"],
    ["5", "Olaf", "Toronto", "80000"],
    ["2", "Max", "Alberta", "45565"]
]
update_columns = ["id", "name", "location", "salary"]
df_daily = spark.createDataFrame(data_daily, update_columns)

# Display input DataFrames
print("Full data:")
df_full.show()
print("Daily updates:")
df_daily.show()

# Perform SCD Type-3: Update current values and move old values to previous columns
result = df_full.join(df_daily, "id", "full_outer").select(
    coalesce(df_full.id, df_daily.id).alias("id"),
    coalesce(df_daily.name, df_full.name).alias("name"),
    coalesce(df_daily.location, df_full.location).alias("location"),
    df_full.location.alias("previous_location"),
    coalesce(df_daily.salary, df_full.salary).alias("salary"),
    df_full.salary.alias("previous_salary")
)

# Display result
print("SCD Type-3 Result:")
result.show()

# Stop Spark session
spark.stop()
