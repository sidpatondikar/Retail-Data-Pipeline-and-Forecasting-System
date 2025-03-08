
# CandyBatchProcessor

## Project Overview

CandyBatchProcessor is a batch data processing system designed to handle daily online transactions for Tiger's Candy, a rapidly growing candy store that originated on the RIT campus. This system processes raw orders, validates data, updates inventory, generates analytical reports, and integrates with a time series forecasting model to predict future sales and profits.

The project demonstrates a complete ETL pipeline using **MySQL**, **MongoDB**, **Apache Spark**, and **Apache Airflow** to process data batches and ensure timely reporting and forecasting.

---

## Dataset Description

The project uses a dataset containing:

### 1. Customers Data (MySQL)
- Customer profiles including name, contact details, and addresses.
- Initial data is loaded from a CSV file.

### 2. Products Data (MySQL)
- Product catalog with product categories, prices, and inventory levels.
- Initial data is loaded from a CSV file.

### 3. Transaction Data (MongoDB)
- Daily JSON files (10 days) containing raw order transactions.
- Each transaction includes customer ID, product details, quantities, and timestamps.

---

## Key Features

- **Data Ingestion**: Load initial data from CSV to MySQL and JSON to MongoDB.
- **Batch Processing ETL**: 
    - Process daily transactions into `orders` and `order_line_items` tables.
    - Perform inventory checks and cancel unavailable items.
    - Output processed data to CSV files.
- **Data Analytics**:
    - Generate daily sales summaries.
    - Update product inventory levels based on processed orders.
- **Forecasting**:
    - Predict future sales and profits using a time series model.
    - Calculate MAE (Mean Absolute Error) and MSE (Mean Squared Error) to evaluate forecasting performance.
- **Apache Airflow Integration**:
    - Orchestrate the entire batch process using a custom Airflow DAG.
    - Automate data loading, processing, and forecasting tasks.

---

## Tech Stack

- **Python 3.8+**
- **MySQL**
- **MongoDB**
- **Apache Spark (PySpark)**
- **Apache Airflow**
- **Time Series Forecasting Model (Pre-provided)**

---

## License and Usage Restrictions

Copyright © 2025 Zimeng Lyu. All rights reserved.
This project was developed by Zimeng Lyu for the RIT DSCI-644 course, originally designed for GitLab CI/CD integration. It is shared publicly for portfolio and learning purposes.

🚫 Usage Restriction:

This project may not be used as teaching material in any form (including lectures, assignments, or coursework) without explicit written permission from the author.

📩 For inquiries regarding usage permissions, please contact Zimeng Lyu at zimenglyu@mail.rit.edu.

