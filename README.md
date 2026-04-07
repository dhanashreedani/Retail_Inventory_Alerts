
# Retail Inventory Pipeline

A data pipeline that automatically tracks product stock and sends alerts when something needs attention.

---

## Overview

This project builds a complete data pipeline for retail inventory management. It pulls product data from a free API every day, processes it in three stages using Databricks, and stores the results in Azure Data Lake. When stock issues are detected, automatic email alerts are sent. All insights are visible through a Databricks dashboard. The pipeline runs on a daily schedule without any manual work.

---

## Architecture

    Step 1 - Data Ingestion
        Source        : DummyJSON REST API (https://dummyjson.com/products)
        Tool          : Azure Data Factory
        Action        : Fetch paginated product data and store in Azure Data Lake

    Step 2 - Bronze Layer (Raw)
        Storage       : Azure Data Lake - Bronze Container
        Tool          : Databricks Notebook
        Action        : Save raw data exactly as received, no changes made

    Step 3 - Silver Layer (Clean)
        Storage       : Azure Data Lake - Silver Container
        Tool          : Databricks + PySpark
        Action        : Fix data types, remove duplicates, handle missing values

    Step 4 - Gold Layer (Business Ready)
        Storage       : Azure Data Lake - Gold Container
        Tool          : Databricks + PySpark
        Action        : Create summary tables for low stock, overstock, demand alerts

    Step 5 - Alerting
        Tool          : Databricks SQL Alerts
        Action        : Run SQL queries on Gold tables, send email if issues found

    Step 6 - Dashboard
        Tool          : Databricks Dashboard
        Action        : Display KPIs, charts, and inventory insights

---

## What It Does

- Fetches product data daily from an API (https://dummyjson.com/products)
- Cleans and organizes the data
- Sends email alerts for stock issues
- Shows results in a Databricks dashboard

---

## Tools Used

| Tool | Purpose |
|---|---|
| Azure Data Lake | Stores all data |
| Azure Data Factory | Runs the pipeline daily |
| Databricks + PySpark | Cleans and transforms data |
| Databricks Dashboard | Shows charts and inventory insights |

---

## How Data Flows

    API -> Bronze -> Silver -> Gold -> Alerts + Dashboard

- Bronze - saves data exactly as received
- Silver - fixes errors, removes duplicates, corrects data types
- Gold - creates summary tables ready for business decisions

---

## Alerts (Auto Email)

| Alert | Condition |
|---|---|
| Low Stock | Stock less than 10 |
| Overstock | Stock more than 2x minimum order |
| Demand Alert | Unusual demand signals |

---

## Files

- data/ -> Sample API response
- notebooks/ -> Bronze, Silver, Gold, Logging scripts
- pipelines/ -> ADF pipeline definition
- dashboards/ -> Databricks dashboard config

---

## Quick Setup

1. Create ADLS storage with bronze, silver, gold containers
2. Run ext_loc_catalog notebook to set up the database
3. Import Project_pipeline.json into Azure Data Factory
4. Set a daily schedule trigger in ADF
5. Add alert queries in Databricks SQL with your email
6. Open the Databricks Dashboard and connect to the Gold layer tables

---

## Author

Dhanashree Dani

- GitHub    : https://github.com/dhanashreedani
- LinkedIn  : https://www.linkedin.com/in/dhanashree-dani-075a48253/
- Email     : dhanashree.dani26@gmail.com
