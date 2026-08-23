-- =====================================================
-- AWS Retail Data Engineering Project
-- Amazon Athena Business Analytics Queries
-- Gold Layer: retail_data_db.gold_sales
-- =====================================================


-- 1. Verify Gold Layer
SELECT *
FROM retail_data_db.gold_sales
LIMIT 10;


-- 2. Total Sales by Product Category
SELECT
    category,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    COUNT(transaction_id) AS transaction_count
FROM retail_data_db.gold_sales
GROUP BY category
ORDER BY total_sales DESC;


-- 3. Total Sales by State
SELECT
    state,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    COUNT(transaction_id) AS transaction_count
FROM retail_data_db.gold_sales
GROUP BY state
ORDER BY total_sales DESC;


-- 4. Product Profitability
SELECT
    product_name,
    ROUND(SUM(sales_amount), 2) AS total_sales,
    ROUND(SUM(profit), 2) AS total_profit,
    COUNT(transaction_id) AS transaction_count
FROM retail_data_db.gold_sales
GROUP BY product_name
ORDER BY total_profit DESC;


-- 5. Overall Business KPIs
SELECT
    COUNT(transaction_id) AS total_transactions,
    ROUND(SUM(sales_amount), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(AVG(sales_amount), 2) AS avg_transaction_value
FROM retail_data_db.gold_sales;


-- 6. Profit Margin by Category
SELECT
    category,
    ROUND(SUM(sales_amount), 2) AS total_revenue,
    ROUND(SUM(profit), 2) AS total_profit,
    ROUND(
        (SUM(profit) / SUM(sales_amount)) * 100,
        2
    ) AS profit_margin_pct
FROM retail_data_db.gold_sales
GROUP BY category
ORDER BY total_profit DESC;