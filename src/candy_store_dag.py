from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime, timedelta
from data_processor import DataProcessor
from pyspark.sql import SparkSession
from typing import Dict
from dotenv import load_dotenv
import os

# Load environment variables (Airflow workers need this to load .env in case)
load_dotenv()

# Default args for the DAG
default_args = {
    "owner": "airflow",
    "depends_on_past": False,
    "start_date": datetime(2024, 3, 1),
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# Define the DAG
dag = DAG(
    "candy_store_etl",
    default_args=default_args,
    description="Candy Store Order Processing and Forecasting",
    schedule_interval=None,
    catchup=False,
    tags=["candy_store"],
)


# Load Configuration (shared across tasks)
def load_config() -> Dict:
    return {
        "mongodb_uri": os.getenv("MONGODB_URI"),
        "mongodb_db": os.getenv("MONGO_DB"),
        "mongodb_collection_prefix": os.getenv("MONGO_COLLECTION_PREFIX"),
        "mongo_start_date": os.getenv("MONGO_START_DATE"),
        "mongo_end_date": os.getenv("MONGO_END_DATE"),
        "mysql_url": os.getenv("MYSQL_URL"),
        "mysql_user": os.getenv("MYSQL_USER"),
        "mysql_password": os.getenv("MYSQL_PASSWORD"),
        "mysql_db": os.getenv("MYSQL_DB"),
        "customers_table": os.getenv("CUSTOMERS_TABLE"),
        "products_table": os.getenv("PRODUCTS_TABLE"),
        "output_path": os.getenv("OUTPUT_PATH"),
        "reload_inventory_daily": os.getenv("RELOAD_INVENTORY_DAILY", "false").lower()
        == "true",
        "mysql_connector_path": os.getenv("MYSQL_CONNECTOR_PATH"),
        "app_name": "CandyStoreETL",
    }


# Create Spark Session
def create_spark_session(config: Dict) -> SparkSession:
    return (
        SparkSession.builder.appName(config["app_name"])
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", config["mysql_connector_path"])
        .config("spark.mongodb.input.uri", config["mongodb_uri"])
        .config("spark.driver.memory", "4g")
        .getOrCreate()
    )


# Global dictionary to hold data across tasks
DATA_PROCESSOR_STATE = {"data_processor": None}


# Task 1: Load Data from MySQL & MongoDB
def load_data():
    config = load_config()
    spark = create_spark_session(config)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    data_processor.configure(config)

    data_processor.customers_df = data_processor.load_mysql_data_to_spark(
        config["customers_table"]
    )
    data_processor.products_df = data_processor.load_mysql_data_to_spark(
        config["products_table"]
    )
    data_processor.original_products_df = data_processor.products_df

    data_processor.transactions_df = data_processor.load_mongo_data_to_spark()

    for collection_name, df in data_processor.transactions_df.items():
        data_processor.transactions_df[collection_name] = (
            data_processor.transform_mongo_data(df)
        )

    os.makedirs(config["output_path"], exist_ok=True)

    # Save DataFrames to Parquet for next task
    data_processor.customers_df.write.mode("overwrite").parquet(
        f'{config["output_path"]}/customers.parquet'
    )
    data_processor.products_df.write.mode("overwrite").parquet(
        f'{config["output_path"]}/products.parquet'
    )

    for date, df in data_processor.transactions_df.items():
        df.write.mode("overwrite").parquet(
            f'{config["output_path"]}/transactions_{date}.parquet'
        )

    print("✅ DataFrames saved to disk successfully.")

    spark.stop()


# Task 2: Batch Processing
def batch_processing():
    config = load_config()
    spark = create_spark_session(config)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    data_processor.configure(config)

    # Load DataFrames
    data_processor.customers_df = spark.read.parquet(
        f'{config["output_path"]}/customers.parquet'
    )
    data_processor.products_df = spark.read.parquet(
        f'{config["output_path"]}/products.parquet'
    )
    data_processor.original_products_df = data_processor.products_df

    # Load transactions in sorted order
    transactions_df = {}

    transaction_files = [
        file
        for file in os.listdir(config["output_path"])
        if file.startswith("transactions_") and file.endswith(".parquet")
    ]

    # Sort files by date (YYYYMMDD is naturally sortable)
    transaction_files = sorted(
        transaction_files,
        key=lambda f: f.replace("transactions_", "").replace(".parquet", ""),
    )

    for file in transaction_files:
        date = file.replace("transactions_", "").replace(".parquet", "")
        transactions_df[date] = spark.read.parquet(f'{config["output_path"]}/{file}')

    data_processor.transactions_df = transactions_df

    print("\n🔹 Starting batch processing for all dates...")

    data_processor.batch_processing(data_processor.transactions_df)

    data_processor.orders_df = data_processor.orders_df.orderBy("order_id")
    data_processor.order_line_items_df = data_processor.order_line_items_df.orderBy(
        "order_id", "product_id"
    )

    # Save processed data for next step
    data_processor.orders_df.write.mode("overwrite").parquet(
        f'{config["output_path"]}/orders.parquet'
    )
    data_processor.order_line_items_df.write.mode("overwrite").parquet(
        f'{config["output_path"]}/order_line_items.parquet'
    )
    data_processor.daily_summary_df.write.mode("overwrite").parquet(
        f'{config["output_path"]}/daily_summary.parquet'
    )

    print("✅ Batch processing completed and data saved to disk.")

    spark.stop()


# Task 3: Forecasting & CSV Generation
def generate_forecasts_and_output():
    config = load_config()
    spark = create_spark_session(config)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    data_processor.configure(config)

    # ✅ Reload all data including products_df
    data_processor.orders_df = spark.read.parquet(
        f'{config["output_path"]}/orders.parquet'
    )
    data_processor.order_line_items_df = spark.read.parquet(
        f'{config["output_path"]}/order_line_items.parquet'
    )
    data_processor.daily_summary_df = spark.read.parquet(
        f'{config["output_path"]}/daily_summary.parquet'
    )
    data_processor.products_df = spark.read.parquet(
        f'{config["output_path"]}/products.parquet'
    )

    # Generate final CSVs
    data_processor.generate_csv(
        data_processor.daily_summary_df,
        data_processor.orders_df,
        data_processor.order_line_items_df,
        data_processor.products_df,  # Now this won't be None
    )

    forecast_df = data_processor.forecast_sales_and_profits(
        data_processor.daily_summary_df
    )

    if forecast_df is not None:
        data_processor.save_to_csv(
            forecast_df, config["output_path"], "sales_profit_forecast.csv"
        )

    print("✅ Forecasting and final output generation completed.")

    spark.stop()


# Airflow Tasks
load_data_task = PythonOperator(
    task_id="load_data",
    python_callable=load_data,
    dag=dag,
)

batch_processing_task = PythonOperator(
    task_id="batch_processing",
    python_callable=batch_processing,
    dag=dag,
)

forecasting_task = PythonOperator(
    task_id="forecasting_and_output",
    python_callable=generate_forecasts_and_output,
    dag=dag,
)

# Define Task Dependencies
load_data_task >> batch_processing_task >> forecasting_task
