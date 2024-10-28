import logging
import os

import joblib
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

# Set up logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class ColdChainDataSimulator:
    def __init__(self,
                 ideal_temp=5.0,
                 ideal_humidity=60.0,
                 temp_std=0.5,
                 humidity_std=2.0):
        self.ideal_temp = ideal_temp
        self.ideal_humidity = ideal_humidity
        self.temp_std = temp_std
        self.humidity_std = humidity_std

    def generate_normal_window(self):
        """Generate a window of normal operation data"""
        # Generate mean values for this window
        temp_mean = np.random.normal(self.ideal_temp, self.temp_std)
        humidity_mean = np.random.normal(self.ideal_humidity, self.humidity_std)

        # Generate 10 readings per window
        temps = np.random.normal(temp_mean, self.temp_std / 2, 10)
        humidities = np.random.normal(humidity_mean, self.humidity_std / 2, 10)

        return {
            'temp_deviation': abs(np.mean(temps) - self.ideal_temp),
            'humidity_deviation': abs(np.mean(humidities) - self.ideal_humidity),
            'temp_mean': np.mean(temps),
            'humidity_mean': np.mean(humidities),
            'temp_max': np.max(temps),
            'humidity_max': np.max(humidities)
        }

    def generate_anomaly_window(self):
        """Generate a window of anomalous operation data"""
        anomaly_type = np.random.choice([
            'temp_spike',
            'humidity_spike',
            'both_spike',
            'gradual_drift'
        ])

        if anomaly_type == 'temp_spike':
            temp_mean = self.ideal_temp + np.random.uniform(3, 5)
            humidity_mean = self.ideal_humidity + np.random.normal(0, self.humidity_std)
        elif anomaly_type == 'humidity_spike':
            temp_mean = self.ideal_temp + np.random.normal(0, self.temp_std)
            humidity_mean = self.ideal_humidity + np.random.uniform(10, 15)
        elif anomaly_type == 'both_spike':
            temp_mean = self.ideal_temp + np.random.uniform(3, 5)
            humidity_mean = self.ideal_humidity + np.random.uniform(10, 15)
        else:  # gradual_drift
            temp_mean = self.ideal_temp + np.random.uniform(2, 3)
            humidity_mean = self.ideal_humidity + np.random.uniform(7, 10)

        temps = np.random.normal(temp_mean, self.temp_std, 10)
        humidities = np.random.normal(humidity_mean, self.humidity_std, 10)

        return {
            'temp_deviation': abs(np.mean(temps) - self.ideal_temp),
            'humidity_deviation': abs(np.mean(humidities) - self.ideal_humidity),
            'temp_mean': np.mean(temps),
            'humidity_mean': np.mean(humidities),
            'temp_max': np.max(temps),
            'humidity_max': np.max(humidities)
        }


def generate_training_data(n_normal=1000, n_anomaly=100):
    """Generate training dataset with both normal and anomalous windows"""
    simulator = ColdChainDataSimulator()

    # Generate normal windows
    normal_data = [simulator.generate_normal_window() for _ in range(n_normal)]

    # Generate anomalous windows
    anomaly_data = [simulator.generate_anomaly_window() for _ in range(n_anomaly)]

    # Convert to feature matrix
    features = []
    for window in normal_data + anomaly_data:
        features.append([
            window['temp_deviation'],
            window['humidity_deviation'],
            window['temp_mean'],
            window['humidity_mean'],
            window['temp_max'],
            window['humidity_max']
        ])

    return np.array(features)


def train_model(X, contamination=0.1):
    """Train the anomaly detection model"""
    # Scale the features
    scaler = StandardScaler()
    X_scaled = scaler.fit_transform(X)

    # Train Isolation Forest
    model = IsolationForest(
        contamination=contamination,
        random_state=42,
        n_estimators=100,
        max_samples='auto'
    )

    model.fit(X_scaled)
    return model, scaler


def evaluate_model(model, scaler, n_test=200):
    """Evaluate model performance on test data"""
    # Generate test data
    simulator = ColdChainDataSimulator()

    # Generate normal test windows
    normal_test = [simulator.generate_normal_window() for _ in range(n_test)]
    features_normal = np.array([[
        w['temp_deviation'],
        w['humidity_deviation'],
        w['temp_mean'],
        w['humidity_mean'],
        w['temp_max'],
        w['humidity_max']
    ] for w in normal_test])

    # Generate anomalous test windows
    anomaly_test = [simulator.generate_anomaly_window() for _ in range(n_test // 4)]
    features_anomaly = np.array([[
        w['temp_deviation'],
        w['humidity_deviation'],
        w['temp_mean'],
        w['humidity_mean'],
        w['temp_max'],
        w['humidity_max']
    ] for w in anomaly_test])

    # Scale features
    normal_scaled = scaler.transform(features_normal)
    anomaly_scaled = scaler.transform(features_anomaly)

    # Get predictions
    normal_preds = model.predict(normal_scaled)
    anomaly_preds = model.predict(anomaly_scaled)

    # Calculate metrics
    normal_accuracy = np.mean(normal_preds == 1)
    anomaly_accuracy = np.mean(anomaly_preds == -1)

    return {
        'normal_detection_rate': normal_accuracy,
        'anomaly_detection_rate': anomaly_accuracy
    }


def main():
    logger.info("Starting model training process...")

    # Create models directory if it doesn't exist
    os.makedirs('models', exist_ok=True)

    # Generate training data
    logger.info("Generating training data...")
    X = generate_training_data(n_normal=5000, n_anomaly=500)

    # Train model
    logger.info("Training model...")
    model, scaler = train_model(X, contamination=0.1)

    # Evaluate model
    logger.info("Evaluating model...")
    metrics = evaluate_model(model, scaler)
    logger.info(f"Model metrics: {metrics}")

    # Save model and scaler
    logger.info("Saving model and scaler...")
    joblib.dump(model, 'models/anomaly_detector.joblib')
    joblib.dump(scaler, 'models/scaler.joblib')

    logger.info("Training complete!")


if __name__ == "__main__":
    main()
