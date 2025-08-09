# Databricks notebook source
# MAGIC %md
# MAGIC # Silver Layer Script

# COMMAND ----------

# MAGIC %md
# MAGIC #### Importing Pyspark Functions and Types

# COMMAND ----------


from pyspark.sql.functions import *
from pyspark.sql.types import *


# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Access Using App

# COMMAND ----------

https://learn.microsoft.com/en-us/azure/databricks/connect/storage/azure-storage#–oauth-20

# Credentials needed:
# Application (client) ID        --> <APPLICATION_CLIENT_ID>
# Tenant ID                      --> <TENANT_ID>
# Storage Account (Storage Name) --> <STORAGE_ACCOUNT_NAME>
# Secret value                    --> <CLIENT_SECRET_VALUE>

spark.conf.set("fs.azure.account.auth.type.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net", "OAuth")
spark.conf.set("fs.azure.account.oauth.provider.type.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set("fs.azure.account.oauth2.client.id.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net", "<APPLICATION_CLIENT_ID>")
spark.conf.set("fs.azure.account.oauth2.client.secret.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net", "<CLIENT_SECRET_VALUE>")
spark.conf.set("fs.azure.account.oauth2.client.endpoint.<STORAGE_ACCOUNT_NAME>.dfs.core.windows.net", "https://login.microsoftonline.com/<TENANT_ID>/oauth2/token")

# COMMAND ----------

# MAGIC %md
# MAGIC ### Data Loading

# COMMAND ----------

# MAGIC %md
# MAGIC #### Reading Data

# COMMAND ----------

# Load the data
df_cal = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Calendar')

# COMMAND ----------

# Load the data
df_cust = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Customers')

# COMMAND ----------

# Load the data
df_pcat = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Product_Categories')

# COMMAND ----------

# Load the data
df_psbcat = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Product_Subcategories')

# COMMAND ----------

# Load the data
df_prod = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Products')

# COMMAND ----------

# Load the data
df_ret = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Returns')

# COMMAND ----------

# Load the data
# The * is a wildcard for all files in the directory. In this case, the name convention is the same so the only difference is the year. So we can just use the wildcard to get all years.
df_sales = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Sales/AdventureWorks_Sales_*.csv')

# COMMAND ----------

# Load the data
df_terr = spark.read.format('csv').option("header", True).option('InferSchema', True).load('abfss://bronze-raw@adventureworksdl.dfs.core.windows.net/AdventureWorks_Territories')

# COMMAND ----------

# MAGIC %md
# MAGIC ### Transformations

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Calendar

# COMMAND ----------

df_cal.display()

# COMMAND ----------

# Adding month & year columns to Calendar csv 
df_cal = df.withColumn("Month", month(col('Date')))\
           .withColumn("Year", year(col('Date')))
df_cal.display()

# COMMAND ----------

# Push data to the silver layer in Parquet format (stores data in a columnar format rather than row based format)
df_cal.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Calendar')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Customers

# COMMAND ----------

df_cust.display()

# COMMAND ----------

df_cust.withColumn('fullName', concat(col("Prefix"),lit(' '),col("FirstName"),lit(' '),col("LastName"))).display()



# COMMAND ----------

# Creating a full name column using the Prefix, FirstName and LastName
df_cust = df_cust.withColumn('fullName', concat_ws(' ', col("Prefix"),col("FirstName"),col("LastName")))

# COMMAND ----------

df_cust.display()


# COMMAND ----------

# Push data to the silver layer in Parquet format (stores data in a columnar format rather than row based format)
df_cust.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Customers')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Product Categories

# COMMAND ----------

df_pcat.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Product_Categories')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Product Subcategories

# COMMAND ----------

df_psbcat.display()

# COMMAND ----------

df_psbcat.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Product_Subcategories')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Products

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod = df_prod.withColumn('ProductSKU', split(col('ProductSKU'), '-')[0])\
                 .withColumn('ProductName', split(col('ProductName'), ' ')[0])
    

# COMMAND ----------

df_prod.display()

# COMMAND ----------

df_prod.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Products')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Returns

# COMMAND ----------

df_ret.display()

# COMMAND ----------

df_ret.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Returns')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Territories

# COMMAND ----------

df_terr.display()

# COMMAND ----------

df_terr.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Territories')\
    .save()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Sales

# COMMAND ----------

df_sales.display()

# COMMAND ----------

#Changing Stock Date to timestamp
df_sales = df_sales.withColumn('StockDate', to_timestamp('StockDate'))

# COMMAND ----------

# Utilizing REGEXP_REPLACE to change OrderNumber
df_sales = df_sales.withColumn('OrderNumber', regexp_replace(col('OrderNumber'), 'S', 'T'))

# COMMAND ----------

df_sales = df_sales.withColumn('Multiply',col('OrderLineItem')*col('OrderQuantity'))

# COMMAND ----------

df_sales.display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Analyses

# COMMAND ----------

# Analyzing the number of orders received per day
df_sales.groupBy('OrderDate').agg(count('OrderNumber').alias('Total Daily Orders')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Where can we find opportunity to increase share of wallet?

# COMMAND ----------

# Display different category tables and their relationship to sales
df_pcat.display()
df_psbcat.display()
df_prod.display()

# COMMAND ----------

# Inner join products table to obtain ProductSubcategoryKey
df_wshare = df_sales.join(df_prod, on = 'ProductKey', how='inner')


# COMMAND ----------

# Inner join subcategory table to obtain ProductCategoryKey
df_wshare = df_wshare.join(df_psbcat, on = 'ProductSubcategoryKey', how='inner')

# COMMAND ----------

#Inner join category table to obtain CategoryName 
df_wshare = df_wshare.join(df_pcat, on = 'ProductCategoryKey', how='inner')

# COMMAND ----------

df_wshare.select('OrderDate', 'OrderNumber', 'CategoryName','ProductKey', 'ProductSubcategoryKey', 'ProductCategoryKey').display()

# COMMAND ----------

# Aggregrate orders by category 
df_wshare.groupBy('CategoryName').agg(count('OrderNumber').alias('Total Orders Per Category')).display()

# COMMAND ----------

# MAGIC %md
# MAGIC An area for exploration would be Clothing.

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Where can we pilot the new clothing strategy?

# COMMAND ----------

df_wshare.display()
df_terr.display()

# COMMAND ----------

df_wshare = df_wshare.join(df_terr, on = df_wshare['TerritoryKey'] == df_terr['SalesTerritoryKey'], how='inner')

# COMMAND ----------

df_wshare.select('OrderDate', 'OrderNumber', 'CategoryName','ProductKey', 'ProductSubcategoryKey', 'ProductCategoryKey', 'Region', 'Country').display()

# COMMAND ----------

df_wshare.filter(col('CategoryName') == 'Clothing')\
    .groupBy('Region')\
    .agg(count('OrderNumber').alias('Clothing Orders Per Region'))\
    .display()

# COMMAND ----------

# MAGIC %md
# MAGIC The two largest markets are Australia and Southwest, US. These are two good areas for pilot testing new strategies as they represent two vastly different market makeups. This would allow the company to stress test strategies in different environments with opportunity for great learnings.
# MAGIC
# MAGIC Another area for exploration would be why is there such a vast difference in sales in Southwest US, vs all other US regions? Is it lack of market presence via physical locations or lack of brand awareness? 

# COMMAND ----------

# MAGIC %md
# MAGIC ##### Write sales to sliver layer

# COMMAND ----------


df_sales.write.format('parquet')\
    .mode('append')\
    .option('path', 'abfss://silver-transform@adventureworksdl.dfs.core.windows.net/AdventureWorks_Sales')\
    .save()