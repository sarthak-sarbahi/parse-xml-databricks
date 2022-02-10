# Databricks notebook source
# DBTITLE 1,Importing libraries
from pyspark.sql.functions import *
from pyspark.sql.types import *

# COMMAND ----------

# DBTITLE 1,Downloading XML file
# MAGIC %sh 
# MAGIC wget https://www.w3schools.com/xml/simple.xml -P /dbfs/xml/

# COMMAND ----------

# DBTITLE 1,Verifying downloaded file
# MAGIC %fs ls dbfs:/xml/

# COMMAND ----------

# DBTITLE 1,Reading XML file using PySpark
# manually defined schema
custom_schema = StructType(
    [StructField('calories',LongType(),True),
     StructField('description',StringType(),True),
     StructField('name',StringType(),True),
     StructField('price',StringType(),True)
    ]
)

# reading xml and passing manually defined schema
food_menu_df = (
    spark
    .read
    .format("com.databricks.spark.xml")
    .schema(custom_schema)
    .option("rowTag","food") # row tag to treat as a row
    .option("inferSchema",False) # if true, spark tries to infer an appropriate type for each resulting dataframe column
    .load("dbfs:/xml/simple.xml")
)

display(food_menu_df)

# COMMAND ----------

# DBTITLE 1,XML with Spark SQL and Hive table
# MAGIC %sql
# MAGIC CREATE TABLE food_menu (
# MAGIC calories long,
# MAGIC description string,
# MAGIC name string,
# MAGIC price string
# MAGIC )
# MAGIC USING xml
# MAGIC OPTIONS (path "dbfs:/xml/simple.xml", rowTag "food");

# COMMAND ----------

# DBTITLE 1,Querying Hive table
# MAGIC %sql
# MAGIC SELECT * FROM food_menu;

# COMMAND ----------

# DBTITLE 1,Performing a visualization
# performing simple transformations
food_menu_df = (food_menu_df
 .withColumn("price",regexp_replace(col('price'),"\$",""))
 .withColumn("price",col("price").cast("float"))
)

display(food_menu_df)

# COMMAND ----------

# MAGIC %md
# MAGIC ### References
# MAGIC - https://www.w3schools.com/xml/xml_examples.asp
# MAGIC - https://docs.microsoft.com/en-us/azure/databricks/data/data-sources/xml
