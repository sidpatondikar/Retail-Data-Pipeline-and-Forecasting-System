from pyspark.sql import SparkSession, DataFrame
from data_processor import DataProcessor
from dotenv import load_dotenv
from datetime import datetime, timedelta
import os
from pyspark.sql.functions import col, sum as spark_sum
from typing import Dict, Tuple
import traceback
import shutil
import glob


def create_spark_session(app_name: str = "CandyStoreAnalytics") -> SparkSession:
    """
    Create and configure Spark session with MongoDB and MySQL connectors
    """
    return (
        SparkSession.builder.appName(app_name)
        .config(
            "spark.jars.packages", "org.mongodb.spark:mongo-spark-connector_2.12:3.0.1"
        )
        .config("spark.jars", os.getenv("MYSQL_CONNECTOR_PATH"))
        .config("spark.mongodb.input.uri", os.getenv("MONGODB_URI"))
        .config("spark.driver.memory", "12g")
        .getOrCreate()
    )


def get_date_range(start_date: str, end_date: str) -> list[str]:
    """Generate a list of dates between start and end date"""
    start = datetime.strptime(start_date, "%Y%m%d")
    end = datetime.strptime(end_date, "%Y%m%d")
    date_list = []

    current = start
    while current <= end:
        date_list.append(current.strftime("%Y%m%d"))
        current += timedelta(days=1)

    return date_list


def print_header():
    print("*" * 80)
    print("                        CANDY STORE DATA PROCESSING SYSTEM")
    print("                               Analysis Pipeline")
    print("*" * 80)


def print_processing_period(date_range: list):
    print("\n" + "=" * 80)
    print("PROCESSING PERIOD")
    print("-" * 80)
    print(f"Start Date: {date_range[0]}")
    print(f"End Date:   {date_range[-1]}")
    print("=" * 80)


def setup_configuration() -> Tuple[Dict, list]:
    """Setup application configuration"""
    load_dotenv()
    config = load_config()
    date_range = get_date_range(
        os.getenv("MONGO_START_DATE"), os.getenv("MONGO_END_DATE")
    )
    return config, date_range


def load_config() -> Dict:
    """Load configuration from environment variables"""
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
    }


def initialize_data_processor(spark: SparkSession, config: Dict) -> DataProcessor:
    """Initialize and configure the DataProcessor"""
    print("\nINITIALIZING DATA SOURCES")
    print("-" * 80)

    data_processor = DataProcessor(spark)
    data_processor.config = config
    return data_processor


def print_processing_complete(total_cancelled_items: int) -> None:
    """Print processing completion message"""
    print("\nPROCESSING COMPLETE")
    print("=" * 80)
    print(f"Total Cancelled Items: {total_cancelled_items}")


def print_daily_summary(orders_df, order_items_df, cancelled_count):
    """Print summary of daily processing"""
    processed_items = order_items_df.filter(col("quantity") > 0).count()
    print("\nDAILY PROCESSING SUMMARY")
    print("-" * 40)
    print(f"• Successfully Processed Orders: {orders_df.count()}")
    print(f"• Successfully Processed Items: {processed_items}")
    print(f"• Items Cancelled (Inventory): {cancelled_count}")
    print("-" * 40)


def generate_forecasts(
    data_processor: DataProcessor, final_daily_summary, output_path: str
):
    """Generate and save sales forecasts"""
    print("\nGENERATING FORECASTS")
    print("-" * 80)

    try:
        if final_daily_summary is not None and final_daily_summary.count() > 0:
            print("Schema before forecasting:", final_daily_summary.printSchema())
            forecast_df = data_processor.forecast_sales_and_profits(final_daily_summary)
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, output_path, "sales_profit_forecast.csv"
                )
        else:
            print("Warning: No daily summary data available for forecasting")
    except Exception as e:
        print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")
        print("Stack trace:", traceback.format_exc())


def main():
    print_header()

    # Setup
    config, date_range = setup_configuration()
    print_processing_period(date_range)

    # Initialize processor
    spark = create_spark_session()
    data_processor = DataProcessor(spark)

    data_processor.config = config

    try:
        # Configure and load data
        data_processor.configure(data_processor.config)

        # print("Start batch processing for project 2!")

        # Configure and load data
        data_processor.configure(data_processor.config)

        # print("\n🔹 Loading initial data into MySQL...")
        # data_processor.load_csv_to_mysql("data/dataset_15/customers.csv", config["customers_table"])
        # data_processor.load_csv_to_mysql("data/dataset_15/products.csv", config["products_table"])

        # #print("\n🔹 Loading initial transaction data into MongoDB...")
        # data_processor.load_json_to_mongo()

        # print("\n🔹 Loading MySQL data into Spark...")
        data_processor.customers_df = data_processor.load_mysql_data_to_spark(
            data_processor.config["customers_table"]
        )
        data_processor.products_df = data_processor.load_mysql_data_to_spark(
            data_processor.config["products_table"]
        )

        data_processor.original_products_df = data_processor.products_df
        # print("\nMySQL Customers Table Preview:")
        # customers_df.show(5)
        # print(f"Dimensions: {customers_df.count()} rows, {len(customers_df.columns)} columns")

        # print("\nMySQL Products Table Preview:")
        # products_df.show(5)
        # print(f"Dimensions: {products_df.count()} rows, {len(products_df.columns)} columns")

        print("\n🔹 Loading MongoDB transaction data into Spark...")
        data_processor.transactions_df = data_processor.load_mongo_data_to_spark()

        for collection_name, df in data_processor.transactions_df.items():
            data_processor.transactions_df[collection_name] = (
                data_processor.transform_mongo_data(df)
            )

        print("\nStart batch processing for project 2!\n")

        data_processor.batch_processing(data_processor.transactions_df)

        # Final sorting
        data_processor.orders_df = data_processor.orders_df.orderBy("order_id")
        data_processor.order_line_items_df = data_processor.order_line_items_df.orderBy(
            "order_id", "product_id"
        )
        data_processor.products_df = data_processor.products_df.select(
            col("product_id"), col("product_name"), col("stock").alias("current_stock")
        )

        data_processor.generate_csv(
            data_processor.daily_summary_df,
            data_processor.orders_df,
            data_processor.order_line_items_df,
            data_processor.products_df,
        )

        # Generate forecasts
        try:
            # daily_summary_df follows the same schema as the daily_summary that you save to csv
            # schema:
            # - date: date - The business date
            # - num_orders: integer - Total number of orders for the day
            # - total_sales: decimal(10,2) - Total sales amount for the day
            # - total_profit: decimal(10,2) - Total profit for the day
            forecast_df = data_processor.forecast_sales_and_profits(
                data_processor.daily_summary_df
            )
            if forecast_df is not None:
                data_processor.save_to_csv(
                    forecast_df, config["output_path"], "sales_profit_forecast.csv"
                )
        except Exception as e:
            print(f"⚠️  Warning: Could not generate forecasts: {str(e)}")

    except Exception as e:
        print(f"\n❌ Error occurred: {str(e)}")
        raise
    finally:
        print("\nCleaning up...")
        spark.stop()


if __name__ == "__main__":
    main()
