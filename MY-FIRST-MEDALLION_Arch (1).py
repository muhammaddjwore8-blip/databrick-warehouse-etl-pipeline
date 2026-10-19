# Databricks notebook source
from pyspark.sql import functions as F
from pyspark.sql.functions import trim, to_date, try_to_date

file_path = "/Workspace/Users/muhammeddarboe21@gmail.com/warehouse_messy_data.csv"


bronze_df = spark.read.csv(file_path, header = True, inferSchema=False)


# COMMAND ----------

#bronze_df.withColumn('ingested_at', F.current_timestamp())
 
#df.show()
for col in bronze_df.columns:
  new_name = col.replace(" ", "_").replace("/","_") 
  bronze_df = bronze_df.withColumnRenamed(col, new_name)

bronze_df.withColumn('ingested_at', F.current_timestamp())
bronze_df.printSchema()
bronze_df.show(5)
 





#bronze_df.write.format("delta").mode("overwrite").save("/Workspace/Users/muhammeddarboe21@gmail.com/bronze/order")


# COMMAND ----------

bronze_df.write.format("delta").mode("overwrite").option('overwriteSchema', 'true').saveAsTable("Bronze_order")

# COMMAND ----------

spark.sql("select * from Bronze_order limit 5").show()

# COMMAND ----------

bronz = spark.table("Bronze_order")

# COMMAND ----------

silver_df = bronz.withColumn("Date", F.coalesce(F.try_to_date("Last_Restocked", "dd/MM/yyyy"), F.try_to_date("Last_Restocked", "dd-MM-yyyy"))).withColumn("Last_Restocked", F.regexp_replace("Last_Restocked", "NaN", "0"))


display(silver_df)

# COMMAND ----------

silver_df= silver_df.dropDuplicates(['Product_ID'])

# COMMAND ----------

silver_df = silver_df.withColumn("Product_Name", trim(F.regexp_replace("Product_Name", " ", "_"))).withColumn("Warehouse", F.regexp_replace("Warehouse", " ", "_")).withColumn("Location", F.regexp_replace("Location", " ", "_"))




# COMMAND ----------

silver_df.show(5)

# COMMAND ----------

from pyspark.sql.functions import trim

silver_df = silver_df.withColumn("Quantity", trim(F.col('Quantity')))



# COMMAND ----------

silver_df = silver_df.replace("two hundred","200", subset=['Quantity'])



# COMMAND ----------

silver_df = silver_df.replace("NaN","0", subset=['Quantity'])

# COMMAND ----------

silver_df.show()

# COMMAND ----------

silver_df = silver_df.withColumn("Supplier", F.regexp_replace("Supplier", " ", "_"))

# COMMAND ----------

silver_df.show(5)

# COMMAND ----------

silver_df = silver_df.withColumn('Price', trim(F.col('Price')))


silver_df = silver_df.replace("NaN", "0", subset=["Price"])
#silver_df.withColumn("Price", F.regexp_replace("Price", "NaN", "0").cast("double"))


# COMMAND ----------

silver_df.show(20)

# COMMAND ----------

silver_df.select('Product_ID','Product_Name','Price').show(7,truncate=False)

# COMMAND ----------

silver_df= silver_df.withColumn("Price", F.col("Price").cast("double"))

# COMMAND ----------

silver_df = silver_df.withColumn("Last_Restocked", F.when((F.col("Last_Restocked") == "0") | (F.col("Last_Restocked") =="NaN" ) | F.col("Last_Restocked").isNull(), None).otherwise(F.col("Last_Restocked")))

# COMMAND ----------

silver_df= silver_df.withColumn("Last_Restocked", try_to_date(F.col("Last_Restocked"), "dd/MM/yyyy"))

# COMMAND ----------

silver_df.show(8)

# COMMAND ----------

silver_df.filter(F.col("Price") > 1).filter(F.col("Last_Restocked").isNotNull()).show()
silver_df = silver_df


# COMMAND ----------

silver_df.write.mode("overwrite").format("delta").option("overWriteSchema", "true").saveAsTable("silver_order")

# COMMAND ----------

spark.table("silver_order").count()

# COMMAND ----------

gold_df = spark.table('silver_order')


# COMMAND ----------

gold_quantity_df = gold_df.groupBy("Warehouse").agg(F.sum(F.col("Quantity") * F.col('Price')).alias("total_warehorse_value"))

gold_quantity_df.show()

# COMMAND ----------

gold_quantity_df.write.mode("overwrite").format("delta").option("overWriteSchema", "true").saveAsTable("gold_quantity")

# COMMAND ----------



# COMMAND ----------

gold_df = gold_df.withColumn("Product_name", trim(F.regexp_replace("Product_Name", "_", " ")))


gold_df.show()

# COMMAND ----------

gold_quantity_df.orderBy("Warehouse", ascending=True).show()

# COMMAND ----------

gold_stock_df = gold_df.filter(F.lower("Status").like("%out%") | F.lower("Status").like("%low%")).select("Product_ID", "Product_Name","Quantity", "Status").orderBy("Status", ascending=True)

# COMMAND ----------

gold_stock = gold_df.filter(F.lower("Status").like("%out%") | F.lower("Status").like("%low%")).select("Product_ID", "Product_Name","Quantity", "Status").orderBy("Status", ascending=True)


# COMMAND ----------

gold_stock.write.mode("overwrite").format("delta").option("overWriteSchema", "true").saveAsTable("gold_stock")

# COMMAND ----------

