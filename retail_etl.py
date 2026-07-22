from pyspark.sql.functions import col, sum

customers_df = spark.read.option("header","true").csv("customers.csv")
products_df = spark.read.option("header","true").csv("products.csv")
orders_df = spark.read.option("header","true").csv("orders.csv")

orders_customers = orders_df.join(customers_df,"customer_id")

final_df = orders_customers.join(products_df,"product_id")

final_df = final_df.withColumn(
    "revenue",
    col("quantity")*col("price")
)

revenue_df = final_df.groupBy("product_name")\
    .agg(sum("revenue").alias("total_revenue"))

print("ETL Completed Successfully")