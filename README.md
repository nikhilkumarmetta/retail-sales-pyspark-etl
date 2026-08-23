# AWS Retail Data Engineering Pipeline

An end-to-end data engineering project that processes retail sales data using Python, PySpark, Amazon S3, AWS Glue Data Catalog, and Amazon Athena.

The project demonstrates a complete data pipeline from raw CSV files through data cleaning, validation, transformation, enrichment, Parquet storage, cataloging, and SQL-based business analytics.

## Project Architecture

```text
Raw CSV Data
     |
     v
Python / PySpark ETL
     |
     +---- Invalid Records ---> Rejected Data
     |
     v
Curated Sales Data
     |
     v
Customer + Product Enrichment
     |
     v
Gold Sales Dataset
     |
     v
Amazon S3 (Parquet)
     |
     v
AWS Glue Data Catalog
     |
     v
Amazon Athena
     |
     v
Business Analytics / SQL
```

## Technologies Used

- Python
- PySpark / Apache Spark
- SQL
- Amazon S3
- AWS Glue Data Catalog
- Amazon Athena
- Parquet
- AWS CLI
- Git / GitHub
- VS Code

## Dataset

The project uses three source datasets:

- `sales.csv` - retail transaction data
- `customer.csv` - customer information
- `products.csv` - product and pricing information

## ETL Pipeline

### 1. Data Ingestion

Retail CSV files are loaded into Spark DataFrames for processing.

### 2. Data Cleaning

The pipeline performs transformations including:

- Data type casting
- Trimming whitespace
- Standardizing IDs
- Converting identifiers to uppercase
- Removing duplicate transactions
- Handling null and invalid values

### 3. Data Quality Validation

Transactions are validated using business rules.

Examples of rejected conditions include:

- Missing customer ID
- Missing product ID
- Invalid quantity
- Invalid unit price

Valid and rejected records are separated so bad data can be investigated without stopping the pipeline.

### 4. Curated Layer

Valid sales records are written in Parquet format to create a clean analytics-ready dataset.

A derived sales metric is calculated:

```text
sales_amount = quantity * unit_price
```

### 5. Gold Layer

The curated sales dataset is enriched with customer and product information.

The Gold dataset contains business-ready fields such as:

- Transaction ID
- Customer
- State
- Product
- Category
- Brand
- Quantity
- Sales amount
- Cost
- Profit

The Gold dataset is stored in Amazon S3 using Parquet format.

### 6. AWS Glue Data Catalog

AWS Glue is used to catalog the Parquet datasets stored in S3.

A clean Athena view named:

```text
retail_data_db.gold_sales
```

is used as the business analytics layer.

### 7. Amazon Athena Analytics

Athena SQL queries are used to analyze:

- Revenue by product category
- Revenue by state
- Product profitability
- Overall business KPIs
- Profit margin by category

The SQL queries are available in:

```text
sql/athena_queries.sql
```

## Business Results

The Gold dataset produced the following sample KPIs:

| Metric | Result |
|---|---:|
| Total Transactions | 5 |
| Total Revenue | $356.00 |
| Total Profit | $114.00 |
| Average Transaction Value | $71.20 |

### Category Performance

| Category | Revenue | Profit | Profit Margin |
|---|---:|---:|---:|
| Furniture | $200.00 | $70.00 | 35.00% |
| Accessories | $45.00 | $27.00 | 60.00% |
| Electronics | $111.00 | $17.00 | 15.32% |

### Key Insights

- Furniture generated the highest revenue and total profit.
- Accessories achieved the highest profit margin at 60%.
- Office Chair was the most profitable product with $70 total profit.
- Mechanical Keyboard generated revenue but produced a $10 loss.
- One Gold record contained a missing state, demonstrating a downstream data-quality issue that can be monitored and improved in future pipeline versions.

## Project Structure

```text
aws-retail-data-project/
|
├── data/
│   ├── sales.csv
│   ├── customer.csv
│   └── products.csv
|
├── scripts/
│   ├── local_sales_etl.py
│   └── gold_sales_etl.py
|
├── sql/
│   └── athena_queries.sql
|
├── .gitignore
├── README.md
└── requirements.txt
```

## Running the Project Locally

Create and activate a Python virtual environment:

```bash
python3 -m venv .venv
source .venv/bin/activate
```

Install dependencies:

```bash
pip install -r requirements.txt
```

Run the local sales ETL:

```bash
python scripts/local_sales_etl.py
```

Run the Gold-layer ETL:

```bash
python scripts/gold_sales_etl.py
```

## Data Engineering Concepts Demonstrated

This project demonstrates practical experience with:

- ETL pipeline development
- PySpark transformations
- Data quality validation
- Data cleansing
- Deduplication
- Data enrichment and joins
- Medallion-style curated and Gold layers
- Columnar Parquet storage
- Amazon S3 data lakes
- AWS Glue metadata cataloging
- Amazon Athena analytics
- SQL aggregation and business analysis

## Future Improvements

Future versions could include:

- AWS Glue ETL jobs
- Automated pipeline orchestration
- S3 event-driven processing
- Data partitioning
- Incremental processing
- Automated data-quality checks
- CloudWatch monitoring
- CI/CD deployment
- Larger production-scale datasets

## Purpose

This project was built as a hands-on demonstration of an end-to-end AWS data engineering workflow, combining local PySpark development with cloud storage, metadata cataloging, and serverless SQL analytics.