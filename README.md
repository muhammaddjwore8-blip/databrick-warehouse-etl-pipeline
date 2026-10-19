# databrick-warehouse-etl-pipeline medallion architecture
end to end ETL pipe-line built in databricks using pyspark and delta late
to ingest and clean messy data then save in a clean warehouse for analysis and insight purpose.

1- CSV (messy) -> Bronze (raw + ingest) -> Silver (cleaned) -> Gold (business metrics)

### Tech Stack
- PySpark, Databricks Community, Delta Lake, Spark SQL

### What I Built

**Bronze Layer:**
- Ingested CSV with `spark.read.csv(header=True)`
- Cleaned column names (`" "` and `"/"` -> `"_"`)
- Added `ingested_at` with `current_timestamp()` 
- Saved as Delta Table: `Bronze_order`

**Silver Layer (Data Cleaning):**
- Dates: Handled multiple formats `dd/MM/yyyy` and `dd-MM-yyyy` using `try_to_date` + `coalesce`
- Nulls: Replaced `NaN` and `"two hundred"` -> `0`/`200` and cast `Quantity` and `Price` to double
- Strings: `trim` + `regexp_replace` for Product_Name, Warehouse, Location, Supplier
- Deduplication: `dropDuplicates(['Product_ID'])` (next: window function with row_number)

**Gold Layer (Business Value):**
- `gold_quantity`: Total inventory value per warehouse = `SUM(Quantity * Price)`
- `gold_stock`: Low / Out-of-stock alerts filtered with `lower(Status) LIKE '%out%' OR '%low%'`

### Data Quality Checks
- Filtered valid prices `Price > 1` and non-null dates
- Count verification: `spark.table("silver_order").count()`

### How to Run
1. Upload CSV to `/Workspace/Users/.../warehouse_messy_data.csv`
2. Run notebook cells top to bottom in Databricks

### Next Improvement
- Add row_number window for latest Product_ID
- Add SCD Type 2, job scheduling with Databricks Workflows
