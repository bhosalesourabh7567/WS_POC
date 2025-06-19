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

# Welcome to your new notebook
# Type here in the cell editor to add code!
df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.SalesTerritory")
deduped = remove_duplicates(df, ["TerritoryID", "Name", "CountryRegionCode", "Group"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.SalesTerritory")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
