from pyspark.sql import SparkSession
from pyspark.sql.functions import (
     sum as spark_sum,
    col,
    round as spark_round,
    sum as spark_sum,
    to_timestamp,
    trim,
    upper,
    when,
)

# Step 1: Create Spark session
spark = (
    SparkSession.builder
    .appName("Retail Sales ETL")
    .master("local[*]")
    .getOrCreate()
)

spark.sparkContext.setLogLevel("WARN")


# Step 2: Read sales data
sales_df = (
    spark.read
    .option("header", True)
    .csv("data/sales.csv")
)


# Step 3: Convert data types
typed_sales_df = (
    sales_df
    .withColumn("quantity", col("quantity").cast("integer"))
    .withColumn("unit_price", col("unit_price").cast("double"))
    .withColumn(
        "transaction_timestamp",
        to_timestamp(
            col("transaction_timestamp"),
            "yyyy-MM-dd HH:mm:ss"
        )
    )
)


# Step 4: Clean sales ID columns
cleaned_sales_df = (
    typed_sales_df
    .withColumn(
        "transaction_id",
        upper(trim(col("transaction_id")))
    )
    .withColumn(
        "customer_id",
        upper(trim(col("customer_id")))
    )
    .withColumn(
        "product_id",
        upper(trim(col("product_id")))
    )
    .withColumn(
        "store_id",
        upper(trim(col("store_id")))
    )
)


# Step 5: Calculate sales amount
transformed_sales_df = cleaned_sales_df.withColumn(
    "sales_amount",
    spark_round(
        col("quantity") * col("unit_price"),
        2
    )
)


# Step 6: Validate records
validated_sales_df = transformed_sales_df.withColumn(
    "validation_error",
    when(
        col("customer_id").isNull()
        | (trim(col("customer_id")) == ""),
        "MISSING_CUSTOMER_ID",
    )
    .when(
        col("product_id").isNull()
        | (trim(col("product_id")) == ""),
        "MISSING_PRODUCT_ID",
    )
    .when(
        col("quantity").isNull()
        | (col("quantity") <= 0),
        "INVALID_QUANTITY",
    )
    .when(
        col("unit_price").isNull()
        | (col("unit_price") < 0),
        "INVALID_UNIT_PRICE",
    )
    .otherwise(None)
)


# Step 7: Separate valid and rejected data
valid_sales_df = (
    validated_sales_df
    .filter(col("validation_error").isNull())
    .drop("validation_error")
    .dropDuplicates(["transaction_id"])
)

rejected_sales_df = validated_sales_df.filter(
    col("validation_error").isNotNull()
)


# Step 8: Display validation results
print("\n========== VALID SALES ==========")
valid_sales_df.show(truncate=False)

print("\n========== REJECTED SALES ==========")
rejected_sales_df.show(truncate=False)

print("\n========== COUNTS ==========")
print("Source records:", sales_df.count())
print("Valid records:", valid_sales_df.count())
print("Rejected records:", rejected_sales_df.count())


# Step 9: Write current outputs
(
    valid_sales_df.write
    .mode("overwrite")
    .parquet("output/curated_sales")
)

(
    rejected_sales_df.write
    .mode("overwrite")
    .parquet("output/rejected_sales")
)

print("\nParquet files written successfully.")


# Step 10: Read customer data
customers_df = (
    spark.read
    .option("header", True)
    .csv("data/customer.csv")
)

customers_df = customers_df.withColumn(
    "customer_id",
    upper(trim(col("customer_id")))
)


# Step 11: Read product data
products_df = (
    spark.read
    .option("header", True)
    .csv("data/products.csv")
)

products_df = products_df.withColumn(
    "product_id",
    upper(trim(col("product_id")))
)


# Step 12: Display customer and product data
print("\n========== CUSTOMER DATA ==========")
customers_df.show(truncate=False)

print("\n========== PRODUCT DATA ==========")
products_df.show(truncate=False)

# Step 14: Join sales with customer data
sales_customer_df = valid_sales_df.join(
    customers_df,
    on="customer_id",
    how="left"
)

# Step 15: Join with product data
fact_sales_df = sales_customer_df.join(
    products_df,
    on="product_id",
    how="left"
)

# Convert product prices from string to double
fact_sales_df = (
    fact_sales_df
    .withColumn(
        "cost_price",
        col("cost_price").cast("double")
    )
    .withColumn(
        "list_price",
        col("list_price").cast("double")
    )
)


# Calculate total cost and profit
# Convert price columns to numeric data types
fact_sales_df = (
    fact_sales_df
    .withColumn(
        "cost_price",
        col("cost_price").cast("double")
    )
    .withColumn(
        "list_price",
        col("list_price").cast("double")
    )
)

# Calculate total cost
fact_sales_df = fact_sales_df.withColumn(
    "total_cost",
    spark_round(
        col("quantity").cast("double") * col("cost_price"),
        2
    )
)

# Calculate profit
fact_sales_df = fact_sales_df.withColumn(
    "profit_amount",
    spark_round(
        col("sales_amount") - col("total_cost"),
        2
    )
)

print("\n========== FACT SALES WITH PROFIT ==========")
fact_sales_df.show(truncate=False)
(
    fact_sales_df.write
    .mode("overwrite")
    .parquet("output/fact_sales")
)

print("\nFact sales Parquet written successfully.")
from pyspark.sql.functions import sum as spark_sum

sales_summary_df = (
    fact_sales_df
    .groupBy("category")
    .agg(
        spark_round(
            spark_sum("sales_amount"),
            2
        ).alias("total_sales"),
        spark_round(
            spark_sum("profit_amount"),
            2
        ).alias("total_profit")
    )
)

print("\n========== SALES SUMMARY BY CATEGORY ==========")
sales_summary_df.show(truncate=False)
# Step: Write final fact sales table
(
    fact_sales_df.write
    .mode("overwrite")
    .parquet("output/fact_sales")
)

print("\nFact sales Parquet written successfully.")


# Step: Create sales summary by category
sales_summary_df = (
    fact_sales_df
    .groupBy("category")
    .agg(
        spark_round(
            spark_sum("sales_amount"),
            2
        ).alias("total_sales"),
        spark_round(
            spark_sum("profit_amount"),
            2
        ).alias("total_profit")
    )
)

print("\n========== SALES SUMMARY BY CATEGORY ==========")
sales_summary_df.show(truncate=False)


# Write summary to Parquet
(
    sales_summary_df.write
    .mode("overwrite")
    .parquet("output/sales_summary")
)

print("\nSales summary Parquet written successfully.")



# Step 13: Stop Spark
spark.stop()