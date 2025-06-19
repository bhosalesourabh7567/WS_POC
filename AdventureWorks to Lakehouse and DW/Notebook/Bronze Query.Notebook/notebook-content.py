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

# Read the table correctly
df = spark.read.table("Bronze.Product")

# Get an empty DataFrame with the same schema
empty_df = df.limit(0)

# Overwrite the table with the empty DataFrame (truncate)
empty_df.write.mode("overwrite").format("delta").saveAsTable("Bronze.Product")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Bronze.Product LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
