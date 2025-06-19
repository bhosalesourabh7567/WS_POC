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
# META         },
# META         {
# META           "id": "06e8f7f5-344d-4d52-a9f6-e4ab90f529f8"
# META         },
# META         {
# META           "id": "3043a3e8-531c-4fa3-ae6f-f5cf54804b4f"
# META         }
# META       ]
# META     }
# META   }
# META }

# CELL ********************

# Welcome to your new notebook
# Type here in the cell editor to add code!
# Set file path (adjust if using workspace paths)
file_path = "abfss://POC@onelake.dfs.fabric.microsoft.com/Bronze_AdventureworksOLTP.Lakehouse/Files/DimAccount.csv"

# Read CSV file into DataFrame
df_dim_account = spark.read.option("header", True).option("inferSchema", True).csv(file_path)

# Display preview (optional)
display(df_dim_account)

# Write to Delta table in Lakehouse (replace mode options: "overwrite", "append")
df_dim_account.write.format("delta").mode("overwrite").saveAsTable("Gold.DimAccount")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.env.getJobId()

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.ls("abfss://SourabhWorkspace@onelake.dfs.fabric.microsoft.com/LH_AdventureWorks.Lakehouse/Files/AdventureWorks")

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

mssparkutils.fs.head('abfss://SourabhWorkspace@onelake.dfs.fabric.microsoft.com/LH_AdventureWorks.Lakehouse/Files/AdventureWorks/Sales.CurrencyRate.csv')

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
