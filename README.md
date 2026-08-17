# Retail Sales PySpark ETL Pipeline

## Project Overview

This project demonstrates an end-to-end retail sales data engineering pipeline built with Python and PySpark.

The pipeline reads raw CSV data, performs schema conversion and data cleansing, applies data-quality validations, removes duplicate transactions, enriches sales records with customer and product information, calculates sales and profit metrics, creates analytical summaries, and writes curated outputs in Parquet format.

## Architecture

```text
Raw CSV Files
    ↓
PySpark Ingestion
    ↓
Schema Conversion
    ↓
Data Cleaning
    ↓
Data Quality Validation
    ↓
Duplicate Removal
    ↓
Valid / Rejected Records
    ↓
Customer + Product Joins
    ↓
Sales / Cost / Profit Calculations
    ↓
Category-Level Aggregation
    ↓
Parquet Outputs
```

## Technologies Used

* Python 3.12
* PySpark 4.2.0
* Apache Spark
* Spark DataFrames
* Parquet
* Git
* GitHub

## Source Files

The project uses three source datasets:

### `sales.csv`

Contains transaction-level data such as:

* transaction ID
* customer ID
* product ID
* quantity
* unit price
* transaction timestamp
* store ID

### `customer.csv`

Contains customer details such as:

* customer ID
* first name
* last name
* email
* state
* created date

### `products.csv`

Contains product details such as:

* product ID
* product name
* category
* brand
* cost price
* list price

## Data Cleaning

The pipeline standardizes identifiers by:

* trimming leading and trailing spaces
* converting IDs to uppercase
* converting quantity to integer
* converting unit price to double
* converting transaction timestamp to Spark timestamp
* converting product cost and list prices to numeric data types

## Data Quality Rules

The pipeline checks for:

* missing customer IDs
* missing product IDs
* invalid or negative quantities
* invalid unit prices
* duplicate transaction IDs

Invalid records are separated into a rejected dataset with a `validation_error` column.

## Business Transformations

### Sales Amount

```text
sales_amount = quantity × unit_price
```

### Total Cost

```text
total_cost = quantity × cost_price
```

### Profit Amount

```text
profit_amount = sales_amount - total_cost
```

## Data Enrichment

The pipeline performs left joins between:

```text
valid_sales
    +
customers
    +
products
```

to create an enriched `fact_sales` dataset.

A left join is used so valid sales records are preserved even when matching customer reference data is unavailable.

## Sample Processing Results

The sample pipeline run produced:

```text
Source records:   8
Valid records:    5
Rejected records: 2
Duplicate removed: 1
```

Rejected records included:

* missing customer ID
* invalid negative quantity

## Category Sales Summary

| Category    | Total Sales | Total Profit |
| ----------- | ----------: | -----------: |
| Electronics |      111.00 |        17.00 |
| Furniture   |      200.00 |        70.00 |
| Accessories |       45.00 |        27.00 |

## Output Datasets

The ETL pipeline generates:

```text
output/
├── curated_sales/
├── rejected_sales/
├── fact_sales/
└── sales_summary/
```

Outputs are written in Snappy-compressed Parquet format.

## Project Structure

```text
retail-sales-pyspark-etl/
├── data/
│   ├── sales.csv
│   ├── customer.csv
│   └── products.csv
├── scripts/
│   └── local_sales_etl.py
├── output/
├── .gitignore
├── requirements.txt
└── README.md
```

## Run the Project Locally

Activate the Python virtual environment:

```bash
source .venv/bin/activate
```

Run the ETL pipeline:

```bash
python scripts/local_sales_etl.py
```

## Key PySpark Concepts Used

* `SparkSession`
* Spark DataFrames
* `withColumn()`
* `cast()`
* `trim()`
* `upper()`
* `when()`
* `filter()`
* `isNull()`
* `dropDuplicates()`
* `join()`
* `groupBy()`
* `sum()`
* Parquet writes

## Future Enhancements

The next phase of this project will move the local ETL pipeline to AWS using:

* Amazon S3
* AWS Glue
* AWS Glue Data Catalog
* AWS Glue Crawlers
* Amazon Athena
* CloudWatch
* AWS Glue Job Bookmarks

Additional enhancements may include:

* incremental processing
* partitioned Parquet output
* automated pipeline orchestration
* Amazon Redshift integration
* dashboarding with Amazon QuickSight
