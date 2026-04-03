# ==============================
# 1. CONFIGURATION (ADLS MOUNT)
# ==============================

storage_account_name = "yourstorageaccount"
container_name = "datalake"
sas_token = "your_sas_token"

configs = {
  f"fs.azure.account.auth.type.{storage_account_name}.dfs.core.windows.net": "SAS",
  f"fs.azure.sas.token.provider.type.{storage_account_name}.dfs.core.windows.net":
    "org.apache.hadoop.fs.azurebfs.sas.FixedSASTokenProvider",
  f"fs.azure.sas.fixed.token.{storage_account_name}.dfs.core.windows.net": sas_token
}

# Mount ADLS
try:
    dbutils.fs.mount(
      source=f"abfss://{container_name}@{storage_account_name}.dfs.core.windows.net/",
      mount_point="/mnt/datalake",
      extra_configs=configs
    )
except:
    print("Already mounted")


# ==============================
# 2. LOAD BRONZE DATA
# ==============================

df_raw = spark.read.csv(
    "/mnt/datalake/bronze/sales/sales_raw.csv",
    header=True,
    inferSchema=True
)

print("Raw Data")
df_raw.show()


# Save Bronze as Delta
df_raw.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/datalake/bronze/sales_delta")


# ==============================
# 3. LOAD DIRTY DATA
# ==============================

df_dirty = spark.read.csv(
    "/mnt/datalake/bronze/sales/sales_dirty.csv",
    header=True,
    inferSchema=True
)

print("Dirty Data")
df_dirty.show()


# ==============================
# 4. CLEAN DATA (SILVER LAYER)
# ==============================

from pyspark.sql.functions import col, to_date

df_clean = df_dirty \
    .dropDuplicates() \
    .filter(col("price") > 0) \
    .fillna({
        "category": "Unknown",
        "quantity": 1
    }) \
    .withColumn("order_date", to_date(col("order_date"), "yyyy-MM-dd"))

print("Clean Data")
df_clean.show()


# ==============================
# 5. LOAD REFERENCE DATA
# ==============================

df_customers = spark.read.csv(
    "/mnt/datalake/reference/customers.csv",
    header=True,
    inferSchema=True
)

df_products = spark.read.csv(
    "/mnt/datalake/reference/products.csv",
    header=True,
    inferSchema=True
)


# ==============================
# 6. ENRICH DATA (JOIN)
# ==============================

df_enriched = df_clean \
    .join(df_customers, "customer_id", "left") \
    .join(df_products, "product", "left")

print("Enriched Data")
df_enriched.show()


# Save Silver
df_enriched.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/datalake/silver/sales_enriched")


# ==============================
# 7. GOLD LAYER TRANSFORMATION
# ==============================

df_gold = df_enriched.withColumn(
    "total_amount",
    col("price") * col("quantity")
)


# ==============================
# 8. AGGREGATIONS
# ==============================

# Sales by City
df_city = df_gold.groupBy("city") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "total_sales")

print("Sales by City")
df_city.show()


# Sales by Category
df_category = df_gold.groupBy("category") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "total_revenue")

print("Sales by Category")
df_category.show()


# Top Customers
df_top_customers = df_gold.groupBy("customer_name") \
    .sum("total_amount") \
    .withColumnRenamed("sum(total_amount)", "total_spent") \
    .orderBy(col("total_spent").desc())

print("Top Customers")
df_top_customers.show()


# ==============================
# 9. SAVE GOLD LAYER
# ==============================

df_city.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/datalake/gold/sales_by_city")

df_category.write.format("delta") \
    .mode("overwrite") \
    .save("/mnt/datalake/gold/sales_by_category")


# ==============================
# 10. CREATE SQL TABLES
# ==============================

spark.sql("""
CREATE TABLE IF NOT EXISTS sales_gold_city
USING DELTA
LOCATION '/mnt/datalake/gold/sales_by_city'
""")

spark.sql("""
CREATE TABLE IF NOT EXISTS sales_gold_category
USING DELTA
LOCATION '/mnt/datalake/gold/sales_by_category'
""")


# ==============================
# 11. PERFORMANCE OPTIMIZATION
# ==============================

# Repartition
df_gold = df_gold.repartition(4)

# Cache
df_gold.cache()


# ==============================
# 12. PARTITIONED WRITE
# ==============================

df_gold.write.format("delta") \
    .partitionBy("category") \
    .mode("overwrite") \
    .save("/mnt/datalake/gold/sales_partitioned")


# ==============================
# 13. SAMPLE SQL QUERIES
# ==============================

spark.sql("SELECT * FROM sales_gold_city ORDER BY total_sales DESC").show()
spark.sql("SELECT * FROM sales_gold_category ORDER BY total_revenue DESC").show()


print("Pipeline Execution Completed Successfully")