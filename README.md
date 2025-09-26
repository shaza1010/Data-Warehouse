# Data Warehouse ELT Project 

## 📊 Project Overview  
This repository implements a **three-layer data warehouse** architecture (Bronze → Silver → Gold) using T-SQL on SQL Server.  
Raw CRM and ERP CSV data are ingested into bronze tables, cleaned and normalized in silver, and finally exposed as analytical views (dimension + fact) in gold for analytics and reporting.

---

## ✨ Features  
- End-to-end pipeline from CSV → analytics-ready views
- Ingestion of CRM and ERP data from CSV files  
- Three-layer architecture:  
  - **Bronze:** Raw staging of source files  
  - **Silver:** Data cleaning, normalization, deduplication  
  - **Gold:** Final dimensional models (facts & dimensions)  
- Surrogate key generation for dimension tables  
- Ready for BI tools (Power BI, Tableau, etc.)

---

## 🛠 Tools & Technologies  
- **SQL Server** for schema, transformations, and views  
- **T-SQL** for data cleansing and modeling  
- **CSV files** as input sources (CRM + ERP)  

---
 
## 📂 Repository Structure  
```
dataset/
│   ├── source_crm/
│   │   ├── cust_info.csv
│   │   ├── prd_info.csv
│   │   └── sales_details.csv
│   └── source_erp/
│       ├── loc_a101.csv
│       ├── cust_az12.csv
│       └── px_cat_g1v2.csv
scripts/
│   ├── Bronze_layer.sql
│   ├── Silver_layer.sql
│   └── Gold_layer.sql
README.md
```
---

- `dataset/source_crm` — Input CSVs from CRM  
- `dataset/source_erp` — Input CSVs from ERP  
- `scripts/` — SQL scripts for each layer  
- `README.md` — This documentation  

---

## 🧱 Layers & Transformation Logic

### Bronze Layer (`Bronze_layer.sql`)  
- Creates the `bronze` schema and raw staging tables  
- Defines a stored procedure `bronze.load_bronze` that truncates and bulk-inserts the CSV data into staging tables  
- Minimal transformations or cleansing — preserves raw values and structure  

### Silver Layer (`Silver_layer.sql`)  
- Creates the `silver` schema and cleaned/normalized tables  
- For each domain (customers, products, sales, ERP tables), apply:  
  - Data trimming/whitespace cleanup  
  - Normalization (e.g. marital status codes → “Single” / “Married”, gender codes → “Male” / “Female”)  
  - Conversion of types (e.g. integer dates → `DATE`)  
  - Null handling and default substitutions  
  - Deduplication (via `ROW_NUMBER()` partitioning)  
  - Inserts into silver tables with a metadata column `dwh_create_date` to capture load timestamp  

### Gold Layer (`Gold_layer.sql`)  
- Creates the `gold` schema and defines views for analytics:  
  - `gold.dim_customers` — customer dimension combining CRM + ERP enrichments  
  - `gold.dim_products` — product dimension enriched with category metadata  
  - `gold.fact_sales` — fact table joining sales with dimensions  
- Implements surrogate key generation (with `ROW_NUMBER()`) and filters to exclude historical or invalid data  

---

## 📋 Summary Table  

| Layer     | Purpose / Role                               | Key Logic & Transformations                              |
|-----------|----------------------------------------------|-----------------------------------------------------------|
| **Bronze** | Raw ingestion/staging of source CSVs       | Bulk load via `BULK INSERT`, preserve original values     |
| **Silver** | Clean / normalize / dedupe / validate        | Trim, code normalization, type conversion, deduplication  |
| **Gold**   | Analytics models / dimension & fact exposure | Create views, surrogate keys, enrich and join data       |

---

## ⚡ Getting Started / Execution Order

1. **Prepare database environment**  
   - Create a database (e.g. `DataWarehouse`)  
   - Ensure that the server has access to the CSV file paths used in your `BULK INSERT` statements in `Bronze_layer.sql`.

2. **Run SQL scripts in order**  
   - `Bronze_layer.sql` → create bronze schema/tables + load raw CSVs  
   - `Silver_layer.sql` → transform, clean, and insert into silver schema  
   - `Gold_layer.sql` → build the analytical views  

3. **Verify results**  
   - Query views: `SELECT * FROM gold.dim_customers;`, `SELECT * FROM gold.fact_sales;`, etc.  
   - Optionally compare row counts between layers or run data quality checks.

---

## 🔮 Future Enhancements  
- Integrate with **BI/dashboard tools** (Power BI, Tableau)  
- Containerize or automate environment setup (e.g. Docker, CI/CD pipelines)  

---

## 👩 Author & Credits  
- **Author:**  *[Shaza Osman](https://www.linkedin.com/in/shaza-ag-osman/)*  
- **Date:** *Sep. 2025* 
