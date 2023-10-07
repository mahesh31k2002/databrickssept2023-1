# Databricks notebook source
# MAGIC %md
# MAGIC ### databricks doesnt need below  
# MAGIC !pip install pyspark
# MAGIC
# MAGIC from pyspark.sql import SparkSession
# MAGIC
# MAGIC # Create a SparkSession
# MAGIC spark = SparkSession.builder.appName("example").getOrCreate()
# MAGIC

# COMMAND ----------

data = [
    ("1","Alice",20),
    ("2","Bob",20),
    ("3","Charlie",20)
]
columns = ["Id","Name","Age"]

# COMMAND ----------

df = spark.createDataFrame(data,columns)

# COMMAND ----------

df.show()

# COMMAND ----------

display(df)

# COMMAND ----------

# access_key='cxoHaqi+a8RUCSTh0dNB+3dBet6938xPhQdGTHW6BzXH3eqKWPdBrdo/W5sbYeDVL4jDWzOSc2Xl+ASt/EcGew=='
# dbutils.fs.mount(
#     source = 'wasbs://practicecontainer@practicestore2023.blob.core.windows.net/',
#     mount_point = '/mnt/',
#     extra_configs = {'fs.azure.account.key.practicestore2023.blob.core.windows.net':access_key}
# )


# COMMAND ----------

# MAGIC %fs
# MAGIC ls 'mnt/'

# COMMAND ----------

df = spark.read.options(header=True).csv('/mnt/Inbound/empdata.csv')

# COMMAND ----------

df.show()

# COMMAND ----------

df.select("ID","Name").show()

# COMMAND ----------

df_single_partition = df.coalesce(1)

# COMMAND ----------

df_single_partition.write.csv('/mnt/Inbound/empdataout.csv', header=True, mode='overwrite')

# COMMAND ----------

df.select(df["ID"]).show()

# COMMAND ----------

from pyspark.sql.functions import *
df.select(col("ID").alias("Idname")).show()

# COMMAND ----------

from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# COMMAND ----------

empdata =[
    (1,"ename1",1000,10),
    (2,"ename2",14000,20),
    (3,"ename3",5000,10),
    (4,"ename4",10000,30)
]

# COMMAND ----------

empschema = StructType(
    [
        StructField("ID",IntegerType(),True),
        StructField("Name",StringType(),True),
        StructField("Salary",IntegerType(),True),
        StructField("Deptno",IntegerType(),True)
    ]
)

# COMMAND ----------

df = spark.createDataFrame(empdata,empschema)

# COMMAND ----------

df.show()

# COMMAND ----------

# Filter data
df.select("ID").filter("Salary>500").show()

# COMMAND ----------

df.filter("Salary>500").select("ID","Name","Salary").show()

# COMMAND ----------

df.filter(col("Salary")>5000).select("ID","Name","Salary").show()

# COMMAND ----------

df.filter(df["Salary"]>5000).select("ID","Name","Salary").show()

# COMMAND ----------

df.filter(df.Salary>5000).select("ID","Name","Salary").show()

# COMMAND ----------

df.where(df.Salary>5000).select("ID","Name","Salary").show()

# COMMAND ----------

df.where(expr("Salary>5000 and ID=2")).select("ID","Name","Salary").show()

# COMMAND ----------

df.filter(expr("Salary>5000 and ID=2")).select("ID","Name","Salary").show()

# COMMAND ----------

# add a new column
df.withColumn("City",lit("Hyderabad")).show()

# COMMAND ----------

df2 = df.withColumnRenamed("ID","Empid")

# COMMAND ----------

df2.show()

# COMMAND ----------

df2.drop("Deptno").show()

# COMMAND ----------

df.show()

# COMMAND ----------

df.withColumn("TypeofSal",when(df.Salary>5000,"highpaid") \
    .when((df.Salary>3500) & (df.Salary<=5000),"MediumPaid") \
    .otherwise("lowpaid")).show()

# COMMAND ----------

df.withColumn("TypeofSal",when(df.Salary>5000,"highpaid") \
    .when(df.Salary>3500,"MediumPaid") \
    .otherwise("lowpaid")).show()

# COMMAND ----------

df.withColumn("TypeofSal",expr("CASE WHEN Salary >5000 THEN 'highpaid' WHEN Salary>3500 THEN 'Medium Paid' ELSE 'Low Paid' END")).show()

# COMMAND ----------

df.withColumn("concateidname",concat(df.ID,lit('-'),df.Name)).show()

# COMMAND ----------

# Use Aggregate Functions

# COMMAND ----------

df.select(sum("Salary")).show()

# COMMAND ----------

df.where(expr("Salary>5000")).select(sum(df.Salary)).show()

# COMMAND ----------

df.where(expr("Salary>5000")).select(sum("Salary")).show()

# COMMAND ----------

df1 = df.where(expr("Salary>5000")).select(sum("Salary"))
df1.show()

# COMMAND ----------

var1 = df.where(expr("Salary>5000")).select(sum("Salary")).collect()[0][0]
print(f"the total salary of all employees whose salary greater than 5000 are : {var1}")

# COMMAND ----------

# use of Group By functions

# COMMAND ----------

df.show()

# COMMAND ----------

df.groupBy("Deptno") \
    .agg(sum("Salary")) \
    .show()

# COMMAND ----------

df.groupBy("Deptno") \
    .agg(sum("Salary").alias("Total_Salary")) \
    .show()

# COMMAND ----------

df.groupBy("Deptno") \
    .agg(sum("Salary").alias("TotalSal"),min("Salary").alias("minsal"),max("Salary").alias("maxsal"),avg("Salary").alias("avgSal")) \
    .show()

# COMMAND ----------

df.groupBy("Deptno") \
    .agg(sum("Salary").alias("TotalSal"),min("Salary").alias("minsal"),max("Salary").alias("maxsal"),avg("Salary").alias("avgSal")) \
    .filter(col("Deptno") != 20) \
    .show()

# COMMAND ----------

df.groupBy("Deptno") \
    .avg("Salary") \
    .filter(col("Deptno") != 20) \
    .show()
# Note : we can't write more than one aggregagate function without agg(function1,function2,function3)

# COMMAND ----------

df.dtypes

# COMMAND ----------

display(df.dtypes)

# COMMAND ----------

df.describe()

# COMMAND ----------

display(df.describe())

# COMMAND ----------

empdf = df

# COMMAND ----------

empdf.show()

# COMMAND ----------

type(empdf)

# COMMAND ----------

deptdata = [
    (10,"Sales"),
    (20,"HR"),
    (30,"IT"),
    (40,"Finance")
]
deptcolumns = ["Deptid","Depname"]

deptdf = spark.createDataFrame(deptdata,deptcolumns)
deptdf.show()

# COMMAND ----------

type(deptdf)

# COMMAND ----------

empdf.show()

# COMMAND ----------

deptdf.show()

# COMMAND ----------

empdf.printSchema()

# COMMAND ----------

deptdf.printSchema()

# COMMAND ----------

# convert long to integer
deptdf = deptdf.withColumn("Deptid",col("Deptid").cast("int"))
deptdf.printSchema()

# COMMAND ----------

deptdf = deptdf.withColumnRenamed("DeptId","Deptno")

# COMMAND ----------

empdf.show()

# COMMAND ----------

deptdf.show()

# COMMAND ----------

# Joins

# COMMAND ----------

empdf.join(deptdf,empdf.Deptno==deptdf.Deptno,"inner").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="inner").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="left").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="right").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="right").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="outer").show()

# COMMAND ----------

empdf.join(deptdf,on="Deptno",how="outer").fillna({"ID":0,"Name":"None","Salary":0}).show()

# COMMAND ----------

#Write big sql query with all possible combination of DML operations

# COMMAND ----------

# Sample DataFrames
employee_data = [("Alice", "Engineering", 75000),
                 ("Bob", "HR", 60000),
                 ("Charlie", "Engineering", 80000),
                 ("David", "Finance", 90000),
                 ("Eve", "HR", 55000)]

department_data = [("Engineering", "San Francisco"),
                   ("HR", "New York"),
                   ("Finance", "Los Angeles")]

employee_columns = ["Name", "Department", "Salary"]
department_columns = ["Department", "Location"]

# COMMAND ----------

employee_df = spark.createDataFrame(employee_data, employee_columns)
department_df = spark.createDataFrame(department_data, department_columns)

# COMMAND ----------

employee_df.show()
department_df.show()

# COMMAND ----------

# Combine join, group by, order by, select, filter, and conditional logic into a single query

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner").show()

# COMMAND ----------

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner") \
    .groupBy("Location") \
    .agg(sum(employee_df.Salary).alias("TotalSalary")) \
    .show()

# COMMAND ----------

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner") \
    .where(expr("Salary>70000")) \
    .groupBy("Location") \
    .agg(sum(employee_df.Salary).alias("TotalSalary")) \
    .show()

# COMMAND ----------

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner") \
    .where(expr("Salary>70000")) \
    .groupBy("Location") \
    .agg(sum(employee_df.Salary).alias("TotalSalary")) \
    .filter(col("TotalSalary")>90000) \
    .show()

# COMMAND ----------

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner") \
    .where(expr("Salary>70000")) \
    .groupBy("Location") \
    .agg(sum(employee_df.Salary).alias("TotalSalary")) \
    .orderBy(col("TotalSalary").desc()) \
    .show()

# COMMAND ----------

employee_df.join(department_df,employee_df.Department == department_df.Department,"inner") \
    .where(expr("Salary>70000")) \
    .groupBy("Location") \
    .agg(sum(employee_df.Salary).alias("TotalSalary")) \
    .orderBy(col("TotalSalary").desc()) \
    .withColumn("SalaryTyper",when(col("TotalSalary")>90000,"HighlyPaid").otherwise("LowPiad")) \
    .show()

# COMMAND ----------

result = (employee_df
          .join(department_df, "Department", "left")
          .groupBy("Location", "Department")
          .agg(sum(col("Salary")).alias("TotalSalary"))
          .filter(col("TotalSalary") > 70000)
          .orderBy(col("TotalSalary").desc())
          .select("Location", "Department", "TotalSalary",
                  when(col("TotalSalary") > 50000, "High Paid")
                  .when((col("TotalSalary") >= 10000) & (col("TotalSalary") <= 50000), "Medium Paid")
                  .otherwise("Low Paid").alias("SalaryCategory")))

# COMMAND ----------

result.show()

# COMMAND ----------

df_rightjoin = empdf.join(deptdf,on="Deptno",how="outer")
df_rightjoin.show()

# COMMAND ----------

df_rightjoin.dropna().show()
#When you apply .dropna(), it will drop any row that contains at least one null value in any of its columns.

# COMMAND ----------

# Sample DataFrame with missing values
data = [("Alice", 30, None),
        ("Bob", None, "New York"),
        ("Charlie", 25, "Los Angeles"),
        ("David", 28, "Chicago"),
        (None, 22, "San Francisco")]

columns = ["Name", "Age", "City"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.show()

# COMMAND ----------

df.fillna({"Age":0,"City":"Unknown","Name":"Unknown"}).show()

# COMMAND ----------

df.dropna().show()

# COMMAND ----------

null_check_df = df.select([col(c).isNull().alias(c) for c in df.columns])
null_check_df.show()

# COMMAND ----------

data = [("Alice", 30, None),
        ("Bob", None, "New York"),
        ("Charlie", 25, "Los Angeles"),
        ("David", 28, "Chicago"),
        (None, 22, "San Francisco")]

columns = ["Name", "Age", "City"]
df = spark.createDataFrame(data, columns)

# COMMAND ----------

df.withColumn('agemissing',df['Age'].isNull()).show()

# COMMAND ----------

person = ("John","Doe",100)

firstname = person[0]

lastname = person[1]

age = person[2]

print(f"Name: {firstname} {lastname} ,Age:{age}")

# COMMAND ----------

shopping_list = ["Apples", "Bananas", "Milk", "Bread"]

shopping_list[0]


# COMMAND ----------

shopping_list.append("Eggs")

shopping_list

# COMMAND ----------

shopping_list.remove("Bananas")

# COMMAND ----------

shopping_list

# COMMAND ----------

for item in shopping_list:
  if "Milk" in item:
    print("Milk exist")
  else:
    print("Milk Not exist")

# COMMAND ----------

if "Milks" in shopping_list:
  print("Milk exist")
else:
  print("Not exist")

# COMMAND ----------

data = ["   Apple   ", "Banana", "   Cherry", "  Date   "]

print(data)

# COMMAND ----------

newdata=[]
newdata = [i.strip() for i in data]

# COMMAND ----------

newdata

# COMMAND ----------

newlist=[]
for item in data:
  newlist.append(item.strip())
newlist

# COMMAND ----------

csv_data = "John,Doe,30,New York"
print(csv_data)
type(csv_data)
csv_data.split(",")


# COMMAND ----------

splitoutput = csv_data.split(",")
splitoutput

# COMMAND ----------

type(splitoutput)

# COMMAND ----------

sentence = "This is a sample sentence."
sentence.split(" ")

# COMMAND ----------

a = "2023-09-15 14:30:00 - User logged     in"
a.strip()

# COMMAND ----------

log_lines = [
    "2023-09-15 14:30:00 - User logged in",
    "2023-09-15 15:00:12 - Data processing started",
    "2023-09-15 15:45:30 - Error occurred"
]
timestamp=[]
log=[]



# COMMAND ----------

for item in log_lines:
    parts = item.strip().split(" - ")
    if len(parts) ==2 :
        timestamp.append(parts[0])
        log.append(parts[1])

timestamp


# COMMAND ----------

log

# COMMAND ----------

from pyspark.sql.functions import split
# Given data
data = [
    "2023-09-15 14:30:00 - User logged in",
    "2023-09-15 15:00:12 - Data processing started",
    "2023-09-15 15:45:30 - Error occurred"
]

# Create a DataFrame with the given data
df = spark.createDataFrame(data, StringType())

# Split the data based on " - " and add to DataFrame with columns "data" and "error"
split_data = split(df[0]," - ")
df = df.withColumn("data", split_data[0])
df = df.withColumn("error", split_data[1])
display(df)


# COMMAND ----------

# # Print extracted data
# for i in range(len(timestamps)):
#     print(f"Timestamp: {timestamps[i]}, Message: {messages[i]}")

# COMMAND ----------

text_data = [
    "   This is an example   ",
    "of text data with     irregular     spacing.",
    "AND DIFFERENT CAPITALIZATION.",
    "   ",
    "Another example."
]

# Clean and normalize the text data
cleaned_data = []

for text in text_data:
  cleaned_text = text.strip().lower()
  print(cleaned_text)

# COMMAND ----------


for text in text_data:
    cleaned_text = text.strip().lower()
    cleaned_data.append(cleaned_text)

# COMMAND ----------

# Sample data with timestamps
data = [("2023-09-15 10:30:00", 120),
        ("2023-09-15 12:45:30", 180),
        ("2023-09-15 15:15:15", 90)]

columns = ["event_time", "duration"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)
df.printSchema()

# Convert "event_time" to a timestamp column

df = df.withColumn("event_time",to_timestamp(col("event_time"),"yyyy-MM-dd HH:mm:ss"))
df.show()

# COMMAND ----------

df = df.withColumn("CurrenDate",current_date()) \
      .withColumn("PreviousDate",current_date()-2) \
      .withColumn("CurrenTime",current_timestamp())
df.show()

# COMMAND ----------

df.select(col("CurrenDate"),col("PreviousDate")).show()

df.withColumn("datediff",datediff(col("CurrenDate"),col("PreviousDate"))).show()

# COMMAND ----------

df.withColumn("months_between",months_between(col("CurrenDate"),col("PreviousDate"))).show()

df.withColumn("dateadd",date_add(col("CurrenDate"),10)).show()

# COMMAND ----------

# PIVOT # Sample data
data = [("Alice", "Math", 90),
        ("Alice", "Physics", 85),
        ("Bob", "Math", 78),
        ("Bob", "Physics", 92)]

columns = ["Name", "Subject", "Score"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)
df.show()


# COMMAND ----------

df.groupBy("Name").pivot("Subject").agg(sum("Score")).show()

# COMMAND ----------

#UNPIVOT
# Sample pivoted data
data = [("Alice", 90, 85, 90),
        ("Bob", 78, 92, 90),
        ("Bob1", 78, 92, 90),]

columns = ["Name", "Math", "Physics", "Chemistry"]

# Create a DataFrame
df = spark.createDataFrame(data, columns)

df.show()

# COMMAND ----------

unpivot_df = df.selectExpr("Name", "stack(3, 'Math', Math, 'Physics', Physics, 'CHEM', Chemistry) as (Subject, Score)")

# Show the unpivoted DataFrame
unpivot_df.show()

# COMMAND ----------

# write function to add two columns to dataframes when passing paramter as a dataframe and return the dataframe by the caller

from pyspark.sql.functions import lit, current_date


# Define a function to add columns to the DataFrame
def add_columns_to_df(input_df):
    # Add a 'currentdate' column with the current date
    df_with_date = input_df.withColumn('currentdate', current_date())
    
    # Add a 'lit' column with a constant value (e.g., 42 for emp and 99 for dept)
    if 'emp_id' in input_df.columns:
        df_with_lit = df_with_date.withColumn('lit', lit(42))
    elif 'dept_id' in input_df.columns:
        df_with_lit = df_with_date.withColumn('lit', lit(99))
    else:
        raise ValueError("DataFrame must have 'emp' or 'dept' columns.")
    
    return df_with_lit

# Sample DataFrames (you can replace these with your own DataFrames)
emp_data = [(1, 'John'), (2, 'Alice'), (3, 'Bob')]
dept_data = [(101, 'HR'), (102, 'IT'), (103, 'Finance')]

emp_columns = ['emp_id', 'emp_name']
dept_columns = ['dept_id', 'dept_name']

emp_df = spark.createDataFrame(emp_data, emp_columns)
dept_df = spark.createDataFrame(dept_data, dept_columns)

# Call the function on emp and dept DataFrames
emp_df_with_columns = add_columns_to_df(emp_df)
dept_df_with_columns = add_columns_to_df(dept_df)

# Show the modified DataFrames
emp_df_with_columns.show()
dept_df_with_columns.show()



# COMMAND ----------

# DIfference between dataframe and data set
# DataFrames are a more general-purpose choice for most use cases, offering ease of use and SQL-like operations. Datasets, on the other hand, are recommended when you require strict type safety, custom data structures, or object-oriented programming capabilities. The choice between them depends on your specific data processing needs and the level of type-safety you require in your code.

# COMMAND ----------

str = "welcome"

def strfun(str):
    str1=''
    length = len(str)
    for i in range(length):
     str1 = str1 + str[length-1]
     length = length - 1
    return str1

print(strfun(str))
	

# COMMAND ----------

str = "welcome"
str1 = str[::-1]
print(str1)

# COMMAND ----------

str = "welcome"
revstr = ''
for i in str:
    revstr = i + revstr
print(revstr)


# COMMAND ----------


