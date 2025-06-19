# Fabric notebook source

# METADATA ********************

# META {
# META   "kernel_info": {
# META     "name": "synapse_pyspark"
# META   },
# META   "dependencies": {
# META     "lakehouse": {
# META       "default_lakehouse": "be691835-4f90-46b3-99c0-6aede2a3fa95",
# META       "default_lakehouse_name": "LH_Silver",
# META       "default_lakehouse_workspace_id": "7d17f09b-3b26-4d54-aa9d-058c6077ab28"
# META     }
# META   }
# META }

# MARKDOWN ********************

# ### 03. Silver Transformations

# MARKDOWN ********************

# #### Data Cleaning

# CELL ********************

today_date = '2024-09-17'
workspace = "fabric_DEV"

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

fabric_bronze_path = f"abfss://{workspace}@onelake.dfs.fabric.microsoft.com/LH_Bronze.Lakehouse/Tables/bronze_data"

from pyspark.sql.functions import col
df = spark.read.format('delta').load(fabric_bronze_path).filter(col('Processing_Date') == str(today_date))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 01. Handling Duplicates

# CELL ********************

print('Count of rows before deleting duplicates :' , df.count())

df_nodups = df.dropDuplicates()

print('Count of rows before deleting duplicates :' , df_nodups.count())

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 02 - Handle missing or NULL values

# MARKDOWN ********************

# ##### 02. a . Drop rows with missing critical values

# CELL ********************

print('Count before dropping missing criticial data rows : ', df_nodups.count() )

df_dropped = df_nodups.dropna(subset=['Student_ID','Course_ID','Enrollment_Date'])

print('Count After dropping missing criticial data rows : ', df_dropped.count() )


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ##### 02. b. Fill rows with default values

# CELL ********************

df_filled = df_dropped.fillna({
                "Age": 0,
                "Gender": "Unknown",
                "Status": "In-progress",
                "Final_Grade": "N/A",
                "Attendance_Rate": 0.0,
                "Time_Spent_on_Course_hrs": 0.0,
                "Assignments_Completed": 0,
                "Quizzes_Completed": 0,
                "Forum_Posts": 0,
                "Messages_Sent": 0,
                "Quiz_Average_Score": 0.0,
                "Assignment_Average_Score": 0.0,
                "Project_Score": 0.0,
                "Extra_Credit": 0.0,
                "Overall_Performance": 0.0,
                "Feedback_Score": 0.0,
                "Parent_Involvement": "Unknown",
                "Demographic_Group": "Unknown",
                "Internet_Access": "Unknown",
                "Learning_Disabilities": "Unknown",
                "Preferred_Learning_Style": "Unknown",
                "Language_Proficiency": "Unknown",
                "Participation_Rate": "Unknown",
                "Completion_Time_Days": 0,
                "Performance_Score": 0.0,
                "Course_Completion_Rate": 0.0,
                "Completion_Date": '12/31/9999'

            })

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 03. Standardize Date Formats

# CELL ********************

from pyspark.sql.functions import to_date, col

df_format = df_filled.withColumn("Enrollment_Date", to_date(col("Enrollment_Date"), "M/d/yyyy"))\
         .withColumn("Completion_Date",to_date(col('Completion_Date'), "M/d/yyyy"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# CELL ********************

display(df_format)

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# #### 04. Check for Logical consistency
# ##### Completion_Date > Enrollment_Date

# CELL ********************

df_consistent = df_format.filter(col("Completion_Date") >= col("Enrollment_Date"))

# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }

# MARKDOWN ********************

# ### Business Transformations

# CELL ********************


# METADATA ********************

# META {
# META   "language": "python",
# META   "language_group": "synapse_pyspark"
# META }
