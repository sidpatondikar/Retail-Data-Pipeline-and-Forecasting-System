from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import (
    explode,
    col,
    round as spark_round,
    sum as spark_sum,
    max,
    count,
    abs as spark_abs,
    countDistinct,
    when,
    broadcast,
    lit,
    to_date,
)
from typing import Dict, Tuple
from collections import defaultdict
import mysql.connector
from pymongo import MongoClient
import os
import glob
import shutil
import decimal
import numpy as np
import csv
import json
from time_series import ProphetForecaster
from datetime import datetime, timedelta
from pyspark.sql.types import DoubleType, DecimalType


class DataProcessor:
    def __init__(self, spark: SparkSession):
        self.spark = spark
        # Initialize all class properties
        self.config = None
        self.current_inventory = None
        self.inventory_initialized = False
        self.original_products_df = None  # Store original products data
        self.reload_inventory_daily = False  # New flag for inventory reload
        self.order_items = None
        self.products_df = None
        self.customers_df = None
        self.transactions_df = None
        self.orders_df = None
        self.order_line_items_df = None
        self.daily_summary_df = None
        self.total_cancelled_items = 0

    def configure(self, config: Dict) -> None:
        """Configure the data processor with environment settings"""
        self.config = config
        self.reload_inventory_daily = config.get("reload_inventory_daily", False)
        print("\nINITIALIZING DATA SOURCES")
        print("-" * 80)
        if self.reload_inventory_daily:
            print("Daily inventory reload: ENABLED")
        else:
            print("Daily inventory reload: DISABLED")

    def save_to_csv(self, df: DataFrame, output_path: str, filename: str) -> None:
        """
        Save DataFrame to a single CSV file.

        :param df: DataFrame to save
        :param output_path: Base directory path
        :param filename: Name of the CSV file
        """
        # Ensure output directory exists
        os.makedirs(output_path, exist_ok=True)

        # Create full path for the output file
        full_path = os.path.join(output_path, filename)
        print(f"Saving to: {full_path}")  # Debugging output

        # Create a temporary directory in the correct output path
        temp_dir = os.path.join(output_path, "_temp")
        print(f"Temporary directory: {temp_dir}")  # Debugging output

        # Save to temporary directory
        df.coalesce(1).write.mode("overwrite").option("header", "true").csv(temp_dir)

        # Find the generated part file
        csv_file = glob.glob(f"{temp_dir}/part-*.csv")[0]

        # Move and rename it to the desired output path
        shutil.move(csv_file, full_path)

        # Clean up - remove the temporary directory
        shutil.rmtree(temp_dir)

    def finalize_processing(self) -> None:
        """Finalize processing and create summary"""
        print("\nPROCESSING COMPLETE")
        print("=" * 80)
        print(f"Total Cancelled Items: {self.total_cancelled_items}")

    def load_csv_to_mysql(self, file_path: str, table_name: str):
        """
        Load CSV file data into MySQL, overwriting existing data.
        """
        connection = mysql.connector.connect(
            host="localhost",
            user=self.config["mysql_user"],
            password=self.config["mysql_password"],
            database=self.config["mysql_db"],
        )
        cursor = connection.cursor()

        cursor.execute(f"TRUNCATE TABLE {table_name};")
        print(f"🗑️ Cleared existing data from {table_name}")

        with open(file_path, "r", encoding="utf-8") as file:
            reader = csv.reader(file)
            header = next(reader)
            values_placeholder = ", ".join(["%s"] * len(header))

            insert_query = f"INSERT INTO {table_name} ({', '.join(header)}) VALUES ({values_placeholder})"

            data = []
            for row in reader:
                if len(row) == len(header):
                    data.append(tuple(row))

        if not data:
            print(f"No valid data found in {file_path}, check the file format!")

        if data:
            try:
                cursor.executemany(insert_query, data)  # Batch insert
                connection.commit()
                print(
                    f"Overwritten {table_name} with {len(data)} new rows from {file_path}."
                )
            except mysql.connector.Error as err:
                print(f"Error inserting data into {table_name}: {err}")

        cursor.close()
        connection.close()

    def load_json_to_mongo(self):
        """
        Load JSON transaction files into MongoDB and overwrite existing data.
        """
        client = MongoClient(self.config["mongodb_uri"])
        db = client[self.config["mongodb_db"]]

        for date in range(
            int(self.config["mongo_start_date"]), int(self.config["mongo_end_date"]) + 1
        ):
            file_path = f"data/dataset_15/transactions_{date}.json"
            collection_name = f"{self.config['mongodb_collection_prefix']}{date}"

            if collection_name in db.list_collection_names():
                db[collection_name].drop()
                print(f"🗑️ Cleared existing data from {collection_name}")

            if os.path.exists(file_path):
                with open(file_path, "r", encoding="utf-8") as file:
                    try:
                        data = json.load(file)
                        if (
                            isinstance(data, list) and data
                        ):  # Ensure it's a list of documents
                            db[collection_name].insert_many(data)
                            print(
                                f"Successfully inserted {len(data)} records into {collection_name}"
                            )
                        else:
                            print(
                                f"JSON file {file_path} is empty or incorrectly formatted!"
                            )

                    except json.JSONDecodeError as e:
                        print(f"Failed to parse {file_path}: {e}")
            else:
                print(f"File {file_path} not found!")

    def load_mysql_data_to_spark(self, table_name: str) -> DataFrame:
        """
        Load MySQL table into a Spark DataFrame.
        """
        df = (
            self.spark.read.format("jdbc")
            .option("url", self.config["mysql_url"])
            .option("driver", "com.mysql.cj.jdbc.Driver")
            .option("dbtable", table_name)
            .option("user", self.config["mysql_user"])
            .option("password", self.config["mysql_password"])
            .load()
        )
        print(f"Loaded MySQL table {table_name} into Spark DataFrame.")
        return df

    def load_mongo_data_to_spark(self) -> DataFrame:
        """
        Load all MongoDB transactions into a single Spark DataFrame.
        """
        transactions_df = {}  # Dict to store all transaction DataFrames

        for date in range(
            int(self.config["mongo_start_date"]), int(self.config["mongo_end_date"]) + 1
        ):
            collection_name = f"{self.config['mongodb_collection_prefix']}{date}"

            df = (
                self.spark.read.format("mongo")
                .option("uri", self.config["mongodb_uri"])
                .option("database", self.config["mongodb_db"])
                .option("collection", collection_name)
                .load()
            )

            if df.count() > 0:
                transactions_df[collection_name] = df
                print(
                    f"Loaded MongoDB collection {collection_name} into Spark DataFrame with {df.count()} records."
                )
            else:
                print("No data found in collection {collection_name}, skipping.")

        return transactions_df

    def transform_mongo_data(self, transactions_df: DataFrame) -> DataFrame:
        """
        Transform raw transactions DataFrame by exploding 'items' array into individual rows.S
        """

        # Explode items array into multiple rows (one row per product per transaction)
        exploded_df = transactions_df.withColumn("item", explode("items"))

        transformed_df = exploded_df.select(
            col("transaction_id"),
            col("customer_id"),
            col("timestamp"),
            col("item.product_id"),
            col("item.product_name"),
            col("item.qty"),
        )

        return transformed_df

    def generate_csv(
        self,
        daily_summary_df: DataFrame,
        orders_df: DataFrame,
        orders_line_df: DataFrame,
        products_df: DataFrame,
    ):
        """
        Combines and generates all the output files in csv format
        """
        self.save_to_csv(
            daily_summary_df, self.config["output_path"], "daily_summary.csv"
        )
        self.save_to_csv(orders_df, self.config["output_path"], "orders.csv")
        self.save_to_csv(
            orders_line_df, self.config["output_path"], "order_line_items.csv"
        )
        self.save_to_csv(
            products_df, self.config["output_path"], "products_updated.csv"
        )

    def process_daily_transactions(
        self, day_df: DataFrame, products_df: DataFrame
    ) -> Tuple[DataFrame, DataFrame, DataFrame]:
        """
        Process a single day's transactions with proper inventory depletion.
        This handles:
        - Filtering orders with null quantity
        - Checking stock levels before filling orders
        - Cancelling order lines if stock is insufficient (quantity = 0, line_total = 0)
        - Decreasing stock for products that have sufficient inventory
        - Creating per-day order and order line DataFrames
        - Returning updated products DataFrame with reduced stock
        """

        day_df = day_df.filter(col("qty").isNotNull())

        day_df = day_df.orderBy("timestamp")

        combined_df = day_df.join(
            products_df.select("product_id", "sales_price", "stock"),
            on="product_id",
            how="inner",
        )

        product_stock = {
            row["product_id"]: row["stock"] for row in products_df.collect()
        }

        processed_rows = []
        for row in combined_df.collect():
            product_id = row["product_id"]
            qty = row["qty"]
            sales_price = row["sales_price"]

            if product_id in product_stock:
                available_stock = product_stock[product_id]
                if available_stock >= qty:
                    final_qty = qty
                    product_stock[product_id] -= qty
                else:
                    final_qty = 0
            else:
                final_qty = 0

            line_total = round(final_qty * sales_price, 2)

            processed_rows.append(
                (
                    row["transaction_id"],
                    row["customer_id"],
                    row["timestamp"],
                    product_id,
                    final_qty,
                    sales_price,
                    line_total,
                )
            )

        schema = [
            "order_id",
            "customer_id",
            "order_datetime",
            "product_id",
            "quantity",
            "unit_price",
            "line_total",
        ]
        processed_df = self.spark.createDataFrame(processed_rows, schema)

        order_line_df = processed_df.select(
            "order_id", "product_id", "quantity", "unit_price", "line_total"
        ).orderBy("order_id", "product_id")

        orders_df = (
            processed_df.groupBy("order_id", "order_datetime", "customer_id")
            .agg(
                spark_round(spark_sum("line_total"), 2).alias("total_amount"),
                count("product_id").alias("num_items"),
            )
            .orderBy("order_id")
        )

        updated_stock_rows = [
            {"product_id": pid, "stock": stock} for pid, stock in product_stock.items()
        ]
        updated_products_df = self.spark.createDataFrame(updated_stock_rows)

        updated_products_df = products_df.drop("stock").join(
            updated_products_df, on="product_id", how="inner"
        )

        return orders_df, order_line_df, updated_products_df

    def batch_processing(self, transactions_df: Dict[str, DataFrame]):
        """
        Processes batch transactions per day
        Combines process daily transactions and calculate_daily_summary functions
        then combines the respective dataframes in the end
        """
        for collection_name, df in self.transactions_df.items():
            orders_temp, line_temp, self.products_df = self.process_daily_transactions(
                df, self.products_df
            )

            if self.orders_df is None:
                self.orders_df = orders_temp
            else:
                self.orders_df = self.orders_df.union(orders_temp)

            if self.order_line_items_df is None:
                self.order_line_items_df = line_temp
            else:
                self.order_line_items_df = self.order_line_items_df.union(line_temp)

            # Calculate daily summary for the current batch
            daily_summary_temp = self.calculate_daily_summary(
                orders_temp, line_temp, self.products_df
            )

            if self.daily_summary_df is None:
                self.daily_summary_df = daily_summary_temp
            else:
                self.daily_summary_df = self.daily_summary_df.union(daily_summary_temp)

            print(f"Daily Batch Processing of {collection_name} completed")

    def calculate_daily_summary(
        self, orders_df: DataFrame, order_line_df: DataFrame, products_df: DataFrame
    ) -> DataFrame:
        """
        Calculates daily summary including total orders, total sales, and total profit per day.
        No need to join orders_df with order_line_df.
        We calculate cost_to_make directly for the day from order_line_df.
        """

        orders_with_date = orders_df.withColumn(
            "order_date", to_date(col("order_datetime"))
        )

        daily_sales_summary = orders_with_date.groupBy("order_date").agg(
            spark_sum("total_amount").alias("total_sales"),
            countDistinct("order_id").alias("num_orders"),
        )

        # Calculate total cost to make for the day using order_line_df and products_df
        order_line_with_cost = order_line_df.join(
            products_df.select("product_id", "cost_to_make"),
            on="product_id",
            how="left",
        ).withColumn(
            "total_cost_to_make", (col("quantity") * col("cost_to_make")).cast("double")
        )

        # Calculate the total cost for that day (single value for the day)
        total_cost_to_make_for_day = order_line_with_cost.agg(
            spark_sum("total_cost_to_make").alias("total_cost_to_make")
        ).collect()[0]["total_cost_to_make"]

        daily_summary = daily_sales_summary.withColumn(
            "total_cost_to_make", lit(total_cost_to_make_for_day)
        )

        daily_summary = daily_summary.withColumn(
            "total_profit",
            spark_round(col("total_sales") - col("total_cost_to_make"), 2),
        )

        daily_summary = daily_summary.select(
            col("order_date").alias("date"), "num_orders", "total_sales", "total_profit"
        )

        return daily_summary

    # ------------------------------------------------------------------------------------------------
    # Try not to change the logic of the time series forecasting model
    # DO NOT change functions with prefix _
    # ------------------------------------------------------------------------------------------------
    def forecast_sales_and_profits(
        self, daily_summary_df: DataFrame, forecast_days: int = 1
    ) -> DataFrame:
        """
        Main forecasting function that coordinates the forecasting process
        """
        try:
            # Build model
            model_data = self.build_time_series_model(daily_summary_df)

            # Calculate accuracy metrics
            metrics = self.calculate_forecast_metrics(model_data)

            # Generate forecasts
            forecast_df = self.make_forecasts(model_data, forecast_days)

            return forecast_df

        except Exception as e:
            print(
                f"Error in forecast_sales_and_profits: {str(e)}, please check the data"
            )
            return None

    def print_inventory_levels(self) -> None:
        """Print current inventory levels for all products"""
        print("\nCURRENT INVENTORY LEVELS")
        print("-" * 40)

        inventory_data = self.current_inventory.orderBy("product_id").collect()
        for row in inventory_data:
            print(
                f"• {row['product_name']:<30} (ID: {row['product_id']:>3}): {row['current_stock']:>4} units"
            )
        print("-" * 40)

    def build_time_series_model(self, daily_summary_df: DataFrame) -> dict:
        """Build Prophet models for sales and profits"""
        print("\n" + "=" * 80)
        print("TIME SERIES MODEL CONSTRUCTION")
        print("-" * 80)

        model_data = self._prepare_time_series_data(daily_summary_df)
        return self._fit_forecasting_models(model_data)

    def calculate_forecast_metrics(self, model_data: dict) -> dict:
        """Calculate forecast accuracy metrics for both models"""
        print("\nCalculating forecast accuracy metrics...")

        # Get metrics from each model
        sales_metrics = model_data["sales_model"].get_metrics()
        profit_metrics = model_data["profit_model"].get_metrics()

        metrics = {
            "sales_mae": sales_metrics["mae"],
            "sales_mse": sales_metrics["mse"],
            "profit_mae": profit_metrics["mae"],
            "profit_mse": profit_metrics["mse"],
        }

        # Print metrics and model types
        print("\nForecast Error Metrics:")
        print(f"Sales Model Type: {sales_metrics['model_type']}")
        print(f"Sales MAE: ${metrics['sales_mae']:.2f}")
        print(f"Sales MSE: ${metrics['sales_mse']:.2f}")
        print(f"Profit Model Type: {profit_metrics['model_type']}")
        print(f"Profit MAE: ${metrics['profit_mae']:.2f}")
        print(f"Profit MSE: ${metrics['profit_mse']:.2f}")

        return metrics

    def make_forecasts(self, model_data: dict, forecast_days: int = 7) -> DataFrame:
        """Generate forecasts using Prophet models"""
        print(f"\nGenerating {forecast_days}-day forecast...")

        forecasts = self._generate_model_forecasts(model_data, forecast_days)
        forecast_dates = self._generate_forecast_dates(
            model_data["training_data"]["dates"][-1], forecast_days
        )

        return self._create_forecast_dataframe(forecast_dates, forecasts)

    def _prepare_time_series_data(self, daily_summary_df: DataFrame) -> dict:
        """Prepare data for time series modeling"""
        data = (
            daily_summary_df.select("date", "total_sales", "total_profit")
            .orderBy("date")
            .collect()
        )

        dates = np.array([row["date"] for row in data])
        sales_series = np.array([float(row["total_sales"]) for row in data])
        profit_series = np.array([float(row["total_profit"]) for row in data])

        self._print_dataset_info(dates, sales_series, profit_series)

        return {"dates": dates, "sales": sales_series, "profits": profit_series}

    def _print_dataset_info(
        self, dates: np.ndarray, sales: np.ndarray, profits: np.ndarray
    ) -> None:
        """Print time series dataset information"""
        print("Dataset Information:")
        print(f"• Time Period:          {dates[0]} to {dates[-1]}")
        print(f"• Number of Data Points: {len(dates)}")
        print(f"• Average Daily Sales:   ${np.mean(sales):.2f}")
        print(f"• Average Daily Profit:  ${np.mean(profits):.2f}")

    def _fit_forecasting_models(self, data: dict) -> dict:
        """Fit Prophet models to the prepared data"""
        print("\nFitting Models...")
        sales_forecaster = ProphetForecaster()
        profit_forecaster = ProphetForecaster()

        sales_forecaster.fit(data["sales"])
        profit_forecaster.fit(data["profits"])
        print("Model fitting completed successfully")
        print("=" * 80)

        return {
            "sales_model": sales_forecaster,
            "profit_model": profit_forecaster,
            "training_data": data,
        }

    def _generate_model_forecasts(self, model_data: dict, forecast_days: int) -> dict:
        """Generate forecasts from both models"""
        return {
            "sales": model_data["sales_model"].predict(forecast_days),
            "profits": model_data["profit_model"].predict(forecast_days),
        }

    def _generate_forecast_dates(self, last_date: datetime, forecast_days: int) -> list:
        """Generate dates for the forecast period"""
        return [last_date + timedelta(days=i + 1) for i in range(forecast_days)]

    def _create_forecast_dataframe(self, dates: list, forecasts: dict) -> DataFrame:
        """Create Spark DataFrame from forecast data"""
        forecast_rows = [
            (date, float(sales), float(profits))
            for date, sales, profits in zip(
                dates, forecasts["sales"], forecasts["profits"]
            )
        ]

        return self.spark.createDataFrame(
            forecast_rows, ["date", "forecasted_sales", "forecasted_profit"]
        )
