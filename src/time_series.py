import numpy as np
from sklearn.metrics import mean_squared_error, mean_absolute_error
from prophet import Prophet
import pandas as pd


class ProphetForecaster:
    """Prophet-based forecaster for univariate time series"""

    def __init__(self):
        self.model = Prophet()
        self.training_data = None
        self.dates = None
        self.last_date = None

    def fit(self, data: np.ndarray) -> None:
        """Fit Prophet model to the training data"""
        self.training_data = data

        # Create dates for the training data (daily frequency)
        self.dates = pd.date_range(end=pd.Timestamp.today(), periods=len(data))
        self.last_date = self.dates[-1]

        # Create DataFrame in Prophet format
        df = pd.DataFrame({"ds": self.dates, "y": data})

        # Fit the model
        self.model.fit(df)

    def predict(self, forecast_days: int) -> np.ndarray:
        """Generate forecasts using Prophet"""
        if self.model is None:
            raise ValueError("Model must be fit before making predictions")

        # Create future dates for prediction
        future_dates = pd.date_range(
            start=self.last_date + pd.Timedelta(days=1), periods=forecast_days
        )
        future = pd.DataFrame({"ds": future_dates})

        # Make predictions
        forecast = self.model.predict(future)
        return forecast["yhat"].values

    def get_metrics(self) -> dict:
        """Calculate forecast accuracy metrics without rounding"""
        if self.model is None:
            raise ValueError("Model must be fit before calculating metrics")

        # Create DataFrame for the training period
        df = pd.DataFrame({"ds": self.dates, "y": self.training_data})

        # Get predictions for the training period
        predictions = self.model.predict(df)
        predicted_values = predictions["yhat"].values

        mae = mean_absolute_error(self.training_data, predicted_values)
        mse = mean_squared_error(self.training_data, predicted_values)

        # Only round for display
        print("\nModel Performance Metrics:")
        print("-" * 40)
        print(f"• Mean Absolute Error: ${mae:.2f}")
        print(f"• Mean Squared Error:  ${mse:.2f}")
        print("-" * 40)

        return {"mae": mae, "mse": mse, "model_type": "prophet"}
