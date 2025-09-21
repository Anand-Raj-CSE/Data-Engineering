# Databricks notebook source
spark


# COMMAND ----------

storage_account = "sajchashdcvahdcbadchjb"
application_id = "adojcndocvcvnd0chvducvb"
directory_id = "wivnw0vhwruvbwruvbwoeivnv"

spark.conf.set(f"fs.azure.account.auth.type.{storage_account}.dfs.core.windows.net", "OAuth")
spark.conf.set(f"fs.azure.account.oauth.provider.type.{storage_account}.dfs.core.windows.net", "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider")
spark.conf.set(f"fs.azure.account.oauth2.client.id.{storage_account}.dfs.core.windows.net", application_id)
spark.conf.set(f"fs.azure.account.oauth2.client.secret.{storage_account}.dfs.core.windows.net","&wd wcbw9eubweu9fgw7egf86vwevf")
spark.conf.set(f"fs.azure.account.oauth2.client.endpoint.{storage_account}.dfs.core.windows.net", f"https://login.microsoftonline.com/{directory_id}/oauth2/token")

# COMMAND ----------

csv_path = "abfss://audcbcaidba@adoaodubad.dfs.core.windows.net/bronze/customers_dataset.csv"

df = spark.read.option("header", "true").option("inferSchema", "true").csv(csv_path)

display(df)

# COMMAND ----------


path = "abfss://audcbcaidba@adoaodubad.dfs.core.windows.net/bronze/"
customer_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_customers_dataset.csv")
customer_df.show(20)

# COMMAND ----------

display(customer_df)

# COMMAND ----------

geolocation_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_geolocation_dataset.csv")
sellers_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_sellers_dataset.csv")
items_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_order_items_dataset.csv")
payments_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_order_payments_dataset.csv")
reviews_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_order_reviews_dataset.csv")
products_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_products_dataset.csv")
orders_df = spark.read.option('header','true').option('inferschema','true').csv(path+"olist_orders_dataset.csv")


# COMMAND ----------

import pymongo

# COMMAND ----------

# MAGIC %md
# MAGIC ### Since mongo is not being imported so for data Enrichment we are going to get the data using python conection , get the data in pandas dataframe and then we are going to convert it into spark dataframe for more optimization and parallel processing. We can install libraries in our compute , go to libraries and then pypl , put the library name and its version after == , it would install.
# MAGIC

# COMMAND ----------

import pymongo

# COMMAND ----------

# importing module
from pymongo import MongoClient

hostname = "psdch sdhcv dvh sdio"
database = "dcbwdicvwbeivbwevife"
port = "27018"
username = "wejvvewjbewvuvnwecnife"
password = "euwbc9ec8bwe9ucwe9cuba"

uri = "mongodb://" + username + ":" + password + "@" + hostname + ":" + port + "/" + database

# Connect with the portnumber and host
client = MongoClient(uri)

# Access database
mydatabase = client[database]
mydatabase


# COMMAND ----------

display(products_df.head(5))

# COMMAND ----------

# MAGIC %md
# MAGIC #### Here product_category_name is in another language so we need to adjust it according to correct language data which is in our Mondo DB

# COMMAND ----------

import pandas as pd
collection = mydatabase['product_categories']
mongo_data = pd.DataFrame(list(collection.find()))
mongo_data.head(5)

# COMMAND ----------

from pyspark.sql.functions import col,to_date,datediff,current_date,when

# COMMAND ----------

def cleandataframe(df,name):
    print(f"Cleanind dataFrame : {name}")
    return df.dropDuplicates().na.drop('all')

orders_df = cleandataframe(orders_df,'orders')
display(orders_df)


# COMMAND ----------

# MAGIC %md
# MAGIC Converting Date columns to date 

# COMMAND ----------

orders_df = orders_df.withColumn("order_purchase_timestamp",to_date(col("order_purchase_timestamp")))\
                     .withColumn("order_approved_at",to_date(col("order_approved_at")))\
                     .withColumn("order_delivered_carrier_date",to_date(col("order_delivered_carrier_date")))\
                     .withColumn("order_delivered_customer_date",to_date(col("order_delivered_customer_date")))\
                     .withColumn("order_estimated_delivery_date",to_date(col("order_estimated_delivery_date")))
display(orders_df)

# COMMAND ----------

# calculating Delivery and time delays
orders_df = orders_df.withColumn("Delivery time",when(col("order_status") == "delivered",  
                                                          datediff("order_delivered_customer_date","order_purchase_timestamp")))
display(orders_df.head(20))

# COMMAND ----------

orders_df = orders_df.withColumn(
    "actual_delivery_time", 
    datediff(col("order_delivered_customer_date"), col("order_purchase_timestamp"))
)

orders_df = orders_df.withColumn(
    "estimated_delivery_time",
    datediff(col("order_estimated_delivery_date"), col("order_purchase_timestamp"))
)

orders_df = orders_df.withColumn(
    "delay",
    when(
        (col("actual_delivery_time").isNotNull()) & (col("estimated_delivery_time").isNotNull()),
        col("actual_delivery_time") > col("estimated_delivery_time")
    ).otherwise(None)
)

display(orders_df)

# COMMAND ----------

orders_df = orders_df.withColumn("delay_time",col("actual_delivery_time")-col("estimated_delivery_time"))
display(orders_df.tail(5))

# COMMAND ----------

# MAGIC %md
# MAGIC Joining

# COMMAND ----------

display(items_df.head(5))
display(products_df.head(5))
display(customer_df.head(5))

# COMMAND ----------

orders_customer_df = orders_df.join(customer_df,orders_df.customer_id == customer_df.customer_id,"left")
orders_payment_df = orders_customer_df.join(payments_df,orders_customer_df.order_id == payments_df.order_id,"left")
orders_items_df = orders_payment_df.join(items_df,"order_id","left")
orders_item_products_df = orders_items_df.join(products_df,orders_items_df.product_id == products_df.product_id,"left")
final_df = orders_item_products_df.join(sellers_df,orders_item_products_df.seller_id == sellers_df.seller_id,"left")


# COMMAND ----------

display(final_df)

# COMMAND ----------

mongo_data.drop("_id",axis=1,inplace=True)
mongo_spark_df = spark.createDataFrame(mongo_data)
display(mongo_spark_df)

# COMMAND ----------

final_df = final_df.join(mongo_spark_df,"product_category_name","left")
display(final_df)

# COMMAND ----------

display(final_df)

# COMMAND ----------

# MAGIC %md
# MAGIC Removing duplicate columns from our df

# COMMAND ----------

def remove_duplicate_columns(df):
    columns = df.columns
    seen_columns = set()
    columns_to_be_removed = []
    for col in columns:
        if col in seen_columns:
            columns_to_be_removed.append(col)
        else:
            seen_columns.add(col)
    returning_df = df.drop(*columns_to_be_removed)
    return returning_df
final_df = remove_duplicate_columns(final_df)
display(final_df)

# COMMAND ----------

final_df.write.mode("overwrite").parquet("abfss://audcbcaidba@adoaodubad.dfs.core.windows.net/silver")

# COMMAND ----------

