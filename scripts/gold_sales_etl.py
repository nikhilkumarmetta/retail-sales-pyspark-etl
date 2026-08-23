from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Create Spark session
spark = (
    SparkSession.builder
    .appName("RetailGoldSalesETL")
    .getOrCreate()
)

print("Spark session created successfully")
# ---------------------------------------------------------
# 2. Load source datasets
# ---------------------------------------------------------

sales_df = spark.read.parquet("output/curated_sales")

customers_df = (
    spark.read
    .option("header", True)
    .csv("data/customer.csv")
)

products_df = (
    spark.read
    .option("header", True)
    .csv("data/products.csv")
)

print("Curated sales:")
sales_df.show()

print("Customers:")
customers_df.show()

print("Products:")
products_df.show()
# ---------------------------------------------
# 3. Join sales with customer and product data
# ---------------------------------------------

gold_df = (
    sales_df
    .join(customers_df, on="customer_id", how="left")
    .join(products_df, on="product_id", how="left")
)

print("Gold joined dataset:")
gold_df.show(truncate=False)
# ---------------------------------------------
# 4. Build analytics-ready Gold dataset
# ---------------------------------------------

final_gold_df = (
    gold_df
    .withColumn("cost_price", col("cost_price").cast("double"))
    .withColumn(
        "total_cost",
        col("quantity") * col("cost_price")
    )
    .withColumn(
        "profit",
        col("sales_amount") - col("total_cost")
    )
    .select(
        "transaction_id",
        "customer_id",
        "first_name",
        "last_name",
        "state",
        "product_id",
        "product_name",
        "category",
        "brand",
        "quantity",
        "unit_price",
        "cost_price",
        "sales_amount",
        "total_cost",
        "profit",
        "transaction_timestamp",
        "store_id"
    )
)

print("Final Gold dataset:")
final_gold_df.show(truncate=False)
# ---------------------------------------------
# 5. Write Gold dataset to Parquet
# ---------------------------------------------

gold_output_path = "output/gold_sales"

final_gold_df.write \
    .mode("overwrite") \
    .parquet(gold_output_path)

print(f"Gold dataset written successfully to: {gold_output_path}")