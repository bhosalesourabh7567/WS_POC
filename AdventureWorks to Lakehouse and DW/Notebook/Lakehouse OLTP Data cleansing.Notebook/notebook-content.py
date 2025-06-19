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

from pyspark.sql.window import Window
from pyspark.sql.functions import row_number

def remove_duplicates(df, subset_cols):
    window_spec = Window.partitionBy(*subset_cols).orderBy("rowguid" if "rowguid" in df.columns else subset_cols[0])
    return df.withColumn("row_num", row_number().over(window_spec)) \
             .filter("row_num = 1") \
             .drop("row_num")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
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

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Person")
deduped = remove_duplicates(df, ["BusinessEntityID", "FirstName", "LastName"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.Person")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.BusinessEntityAddress")
deduped = remove_duplicates(df, ["BusinessEntityID", "AddressID", "AddressTypeID"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.BusinessEntityAddress")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Address")
deduped = remove_duplicates(df, ["AddressLine1", "City", "PostalCode"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.Address")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.StateProvince")
deduped = remove_duplicates(df, ["StateProvinceID", "Name", "CountryRegionCode"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.StateProvince")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.CountryRegion")
deduped = remove_duplicates(df, ["CountryRegionCode", "Name"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.CountryRegion")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Product")
deduped = remove_duplicates(df, ["ProductID", "ProductNumber"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.Product")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.read.table("Bronze_AdventureworksOLTP.Bronze.ProductCategory")
deduped = remove_duplicates(df, ["ProductCategoryID", "Name"])
deduped.write.mode("overwrite").saveAsTable("Bronze_AdventureworksOLTP.Bronze.ProductCategory")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
