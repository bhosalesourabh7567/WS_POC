# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "8ee1f78d-64d2-4317-9095-79c2ec4bfc14",
# META       "default_lakehouse_name": "Bronze_AdventureworksOLTP",
# META       "default_lakehouse_workspace_id": "026f5f82-6f1a-4490-af92-058246f9e89a",
# META       "known_lakehouses": [
# META         {
# META           "id": "8ee1f78d-64d2-4317-9095-79c2ec4bfc14"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Step 1: Define the file path
file_path = "abfss://POC@onelake.dfs.fabric.microsoft.com/Bronze_AdventureworksOLTP.Lakehouse/Files/DimAccount.csv"

# Step 2: Read the pipe-delimited CSV
df_dim_account = (
    spark.read
    .option("header", True)
    .option("inferSchema", True)
    .option("delimiter", "|")  # ← pipe delimiter
    .csv(file_path)
)

# Step 3: Optional preview
display(df_dim_account)

# Step 4: Write
df_dim_account.write.format("delta").mode("overwrite").saveAsTable("Gold.DimAccount")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Gold.dimaccount LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
