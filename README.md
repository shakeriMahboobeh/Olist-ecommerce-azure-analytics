# Olist E-Commerce Sales Analytics — End-to-End Azure Synapse Platform

🎯 **Business Question:** Can Olist's marketplace performance — revenue trends, top-selling categories, and geographic demand across Brazil — be tracked in a self-service report, built on a fully automated, re-runnable data pipeline?

Built on **Azure Synapse Analytics** — from raw Olist CSV exports to an interactive Power BI dashboard — using the **Medallion architecture** (Bronze → Silver → Gold), a serverless SQL star schema, and a Synapse Pipeline that rebuilds the warehouse end to end.

**Why this project exists:** a re-implementation of the same medallion-architecture pattern used in an earlier Microsoft Fabric project — Bronze → Silver → Gold, orchestrated pipeline, Power BI on top — but rebuilt on **Azure Synapse Analytics** instead of Fabric, with **Git version control** (GitHub, integrated into Synapse Studio) from day one.

## 📊 Dashboard

### Olist Sales Overview

![Olist Sales Overview](docs/screenshots/dashboard.png)

**Revenue, orders, average order value** — KPI cards at the top of the report.

**Revenue trend across the year** — monthly revenue line chart, showing the real November 2017 Black Friday spike present in the source data.

**Top categories and states by revenue** — top 10 product categories (English names) and revenue by customer state (full state names), as horizontal bar charts.

## 🏗️ Architecture

**Dataset:** Olist Brazilian E-Commerce Public Dataset — orders, order items, customers, sellers, products, payments, reviews, geolocation, category-name translation. 9 source files, ~100K orders.

**Tech Stack:** Azure Data Lake Storage Gen2 · Synapse Spark Pool (PySpark, Delta Lake) · Synapse Serverless SQL Pool (CETAS, external tables) · Synapse Pipelines · Power BI (Import mode, DAX) · Git (GitHub-integrated Synapse Studio)

```
Raw CSV Exports (Olist Brazilian E-Commerce Dataset)
  |
  v

BRONZE                 SILVER                  GOLD
ADLS container   -->    Delta tables    -->     Serverless SQL
(raw CSVs)              (cleaned,                (star schema:
                         deduplicated,            4 dimensions
                         typed, Delta              + fact_order_items,
                         format)                   via CETAS)
                                                        |
                                                        v
                                                  Power BI Dashboard
                                                  (Import mode, SQL Auth
                                                   + managed identity
                                                   credential for storage)
```

One Synapse Pipeline orchestrates all three layers: a Notebook activity cleans Bronze into Silver, a second Notebook activity clears the Gold storage folders, and two Script activities rebuild the Gold dimensions and fact table via CETAS. The Gold clear step exists because CETAS refuses to write into a non-empty folder (see Synapse Serverless SQL Notes below).

## 🗂️ Data Model (Star Schema)

One fact table (`fact_order_items`) at order-item grain, four dimensions:

| Table | Grain / Surrogate Key |
|---|---|
| `fact_order_items` | One row per order item; keys join to all four dimensions plus aggregated payment total and average review score per order |
| `dim_customer` | `customer_key` — `ROW_NUMBER()` over distinct `customer_id` |
| `dim_seller` | `seller_key` — `ROW_NUMBER()` over distinct `seller_id` |
| `dim_product` | `product_key` — `ROW_NUMBER()` over distinct `product_id`, joined to the English category-name translation |
| `dim_date` | `date_key` (`yyyyMMdd` integer) — full calendar generated in PySpark, 2016-01-01 to 2018-12-31, doubling as its own surrogate key |

Every dimension is built with CETAS against the cleaned Silver Delta tables; the fact table joins all four dimensions plus two aggregated subqueries (payment total, average review score per order).

## 🔍 Data Engineering & Quality Challenges

**Pipeline login pointed at the wrong endpoint.** Every Script activity failed with `Login failed for user '<token-identified principal>'`, even after granting the workspace's managed identity Synapse Administrator and adding it as `db_owner` in `db_olist_gold`. Actual cause: the default linked service pointed at the **dedicated pool** endpoint (`<workspace>.sql.azuresynapse.net`) instead of the **serverless** endpoint (`<workspace>-ondemand.sql.azuresynapse.net`). Fixed with a new linked service using the correct `-ondemand` FQDN.

**A Delete activity removed an entire storage container, not just its contents.** A root-level wildcard path was intended to clear only the files inside the Gold container, but `*` was matched as a literal folder name rather than a pattern for this dataset/activity combination, and the container itself was deleted. Replaced with a Notebook activity running `mssparkutils.fs.rm()` per table — an approach already proven reliable.

**SQL Authentication has no identity to pass through to storage.** Power BI connected to the Gold layer with SQL Authentication, but reading data failed with "content of directory cannot be listed" — a SQL-auth login has no Azure AD identity to hand off to storage. Fixed with a database-scoped credential (`CREATE DATABASE SCOPED CREDENTIAL ... WITH IDENTITY = 'Managed Identity'`) attached to the `gold_lake` external data source.

## ⚡ Synapse Serverless SQL Notes

**Every external table is a pointer, not storage.** No `INSERT`/`UPDATE`/`MERGE` in serverless SQL — the only way to persist a query result is CETAS (`CREATE EXTERNAL TABLE ... AS SELECT`), and `DROP EXTERNAL TABLE` only removes the metadata pointer, never the underlying files.

**`DROP EXTERNAL TABLE` doesn't support `IF EXISTS`.** Workaround used in every rebuild script:
```sql
IF EXISTS (SELECT * FROM sys.external_tables WHERE name = 'dim_customer')
    EXEC('DROP EXTERNAL TABLE dim_customer');
```

**`dim_date` is generated, not derived from orders.** Built in PySpark with `explode(sequence(start, end, interval 1 day))` to cover every calendar day, not just dates with an order.

## 🔄 Pipeline Automation

```
Notebook (Bronze -> Silver)  --(on success)-->  Notebook (Clear Gold folders)
       --(on success)-->  Script (Rebuild Gold dimensions)
       --(on success)-->  Script (Rebuild Gold fact table)
```

A weekly Schedule trigger (`tr_nightly_rebuild`) is defined against the pipeline and kept stopped by default, since the source dataset is a static historical export and re-running the full rebuild on an unchanging schedule would consume compute for no benefit. It can be started from Manage → Triggers whenever a live scheduled run needs to be demonstrated; the pipeline itself is otherwise run on demand via Trigger now.

## 📁 Repo Structure

```
├── credential/                # Synapse-managed: database-scoped credential definitions
├── dataset/                   # Synapse-managed: pipeline dataset definitions
├── docs/
│   └── screenshots/           # Report and architecture screenshots referenced in this README
├── integrationRuntime/        # Synapse-managed: integration runtime definitions
├── linkedService/             # Synapse-managed: linked service definitions (incl. ls_synapse_serverless_gold)
├── notebook/                  # Synapse-managed: PySpark notebooks
│   ├── nb_bronze_to_silver    # Cleans and writes all 9 source tables to Silver Delta, + dim_date generation
│   └── nb_clear_gold          # Clears Gold storage folders before each CETAS rebuild
├── pipeline/                  # Synapse-managed: orchestration pipeline definition
│   └── pl_olist_bronze_to_gold  # Bronze -> Silver -> Gold rebuild, chained by success dependencies
├── reports/
│   └── olist_sales_overview.pbix  # Power BI report
├── sqlscript/                 # Synapse-managed: Gold layer SQL scripts
│   ├── DWH-Dimension          # Builds dim_customer, dim_seller, dim_product, dim_date
│   ├── DWH-Fact               # Builds fact_order_items
│   ├── External Data Source   # Master key, database-scoped credential, silver_lake/gold_lake external data sources
│   ├── Grant Access           # Creates the SQL user for the workspace's managed identity, grants db_owner
│   └── Sanity Check           # Row-count and cross-table join verification queries
├── trigger/                   # Synapse-managed: trigger definitions
│   └── tr_nightly_rebuild     # Weekly Schedule trigger, kept stopped (see Pipeline Automation)
├── publish_config.json        # Synapse Git-integration config (auto-generated)
└── README.md
```
