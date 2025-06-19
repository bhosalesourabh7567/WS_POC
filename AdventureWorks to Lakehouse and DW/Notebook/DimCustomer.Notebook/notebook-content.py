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
# Load from Bronze
customer = spark.read.format("delta").load("abfss://POC@onelake.dfs.fabric.microsoft.com/StagingAdventureworksOLTP.Lakehouse/Tables/Sales/Customer")



# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Silver.vw_dimcustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM StagingAdventureworksOLTP.Person.Person LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

# Step 1: Read from Lakehouse view
vw_dimcustomer_df = spark.read.table("Bronze_AdventureworksOLTP.Silver.VW_DimCustomer")

# Optional: Inspect schema and preview data
vw_dimcustomer_df.printSchema()
vw_dimcustomer_df.show(5)

# Step 2: Overwrite the target Warehouse table
vw_dimcustomer_df.write \
    .format("sqlDW") \
    .option("url", "DW_AdventureWorks") \
    .option("dbtable", "Gold.DimCustomer") \
    .mode("overwrite") \
    .save()


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

spark.sql("""
    CREATE OR REPLACE MATERIALIZED VIEW Silver.DimCustomer
    AS
    SELECT
        c.CustomerID AS CustomerKey,
        CONCAT(
            p.FirstName, ' ',
            COALESCE(CONCAT(p.MiddleName, ' '), ''),
            p.LastName
        ) AS FullName,
        p.Title,
        p.FirstName,
        p.MiddleName,
        p.LastName,
        c.PersonID,
        a.AddressLine1,
        a.AddressLine2,
        a.City,
        sp.Name AS StateProvince,
        cr.Name AS CountryRegion,
        a.PostalCode,
        sp.CountryRegionCode,
        c.StoreID,
        c.TerritoryID,
        CURRENT_TIMESTAMP() AS CreatedDate
    FROM Bronze.Customer c
    INNER JOIN Bronze.Person p ON c.PersonID = p.BusinessEntityID
    INNER JOIN Bronze.BusinessEntityAddress bea ON bea.BusinessEntityID = p.BusinessEntityID
    INNER JOIN Bronze.Address a ON a.AddressID = bea.AddressID
    INNER JOIN Bronze.StateProvince sp ON a.StateProvinceID = sp.StateProvinceID
    INNER JOIN Bronze.CountryRegion cr ON sp.CountryRegionCode = cr.CountryRegionCode
    WHERE c.PersonID IS NOT NULL
""")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Silver.dimcustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Silver.vw_dimcustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

from pyspark.sql.functions import col, concat_ws, current_timestamp, when

# Load Bronze tables
customer = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Customer")
person = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Person")
bea = spark.read.table("Bronze_AdventureworksOLTP.Bronze.BusinessEntityAddress")
address = spark.read.table("Bronze_AdventureworksOLTP.Bronze.Address")
state = spark.read.table("Bronze_AdventureworksOLTP.Bronze.StateProvince")
country = spark.read.table("Bronze_AdventureworksOLTP.Bronze.CountryRegion")

# Join tables and apply transformations
dim_customer_df = (
    customer
    .join(person, customer["PersonID"] == person["BusinessEntityID"])
    .join(bea, person["BusinessEntityID"] == bea["BusinessEntityID"])
    .join(address, bea["AddressID"] == address["AddressID"])
    .join(state, address["StateProvinceID"] == state["StateProvinceID"])
    .join(country, state["CountryRegionCode"] == country["CountryRegionCode"])
    .filter(customer["PersonID"].isNotNull())
    .select(
        customer["CustomerID"].alias("CustomerKey"),
        concat_ws(" ", person["FirstName"], 
                  when(person["MiddleName"].isNotNull(), person["MiddleName"]).otherwise(""), 
                  person["LastName"]).alias("FullName"),
        person["Title"],
        person["FirstName"],
        person["MiddleName"],
        person["LastName"],
        customer["PersonID"],
        address["AddressLine1"],
        address["AddressLine2"],
        address["City"],
        state["Name"].alias("StateProvince"),
        country["Name"].alias("CountryRegion"),
        address["PostalCode"],
        state["CountryRegionCode"],
        customer["StoreID"],
        customer["TerritoryID"],
        current_timestamp().alias("CreatedDate")
    )
)

# Option 1: Create a temporary view
dim_customer_df.createOrReplaceTempView("Silver_VW_DimCustomer")

# Option 2: Save as Delta table if you want it persisted
dim_customer_df.write.mode("overwrite").format("delta").saveAsTable("Silver.VW_DimCustomer")


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

df = spark.sql("SELECT * FROM Bronze_AdventureworksOLTP.Silver.vw_dimcustomer LIMIT 1000")
display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
