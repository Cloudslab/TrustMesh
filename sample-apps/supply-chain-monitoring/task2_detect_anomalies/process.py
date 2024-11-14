import asyncio
import json
import logging
import time
import traceback
from datetime import datetime
from typing import Dict, List
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureExtractor:
    @staticmethod
    def extract_essential_features(processed_data: Dict) -> List[float]:
        """Extract only the most important features while conserving memory"""
        features = []

        # Add stability index
        features.append(processed_data['environmental_stability_index'])

        # Extract key statistics from temperature analysis
        temp_analysis = processed_data['temperature_analysis']
        for key in ['segment_0', 'segment_1', 'segment_2']:  # Use only first few segments
            if key in temp_analysis:
                segment = temp_analysis[key]
                features.extend([
                    segment['mean'],
                    segment['std'],
                    segment['range']
                ])

        # Extract key statistics from humidity analysis
        humid_analysis = processed_data['humidity_analysis']
        for key in ['segment_0', 'segment_1', 'segment_2']:  # Use only first few segments
            if key in humid_analysis:
                segment = humid_analysis[key]
                features.extend([
                    segment['mean'],
                    segment['std'],
                    segment['range']
                ])

        # Extract essential correlation features
        corr_analysis = processed_data['correlation_analysis']
        if 'baseline' in corr_analysis:
            baseline = corr_analysis['baseline']
            features.extend([
                baseline['max_correlation'],
                baseline['mean_correlation']
            ])

        return features


class AnomalyDetector:
    def __init__(self, n_estimators=100, contamination=0.1):
        """Initialize a lightweight anomaly detector"""
        self.feature_extractor = FeatureExtractor()
        self.scaler = StandardScaler()
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.model = None

    def generate_synthetic_data(self, feature_dim: int, n_samples: int = 500000) -> np.ndarray:
        """Generate a small synthetic dataset for model fitting"""
        np.random.seed(42)  # For reproducibility

        # Generate normal distribution for core behavior
        synthetic_data = np.random.normal(0, 1, (n_samples, feature_dim))

        # Add a few outliers
        n_outliers = int(n_samples * self.contamination)
        outlier_indices = np.random.choice(n_samples, n_outliers, replace=False)
        synthetic_data[outlier_indices] *= 5

        return synthetic_data

    def fit_model(self, features: np.ndarray):
        """Initialize and fit model with minimal data"""
        feature_dim = features.shape[1]

        # Generate small synthetic dataset
        synthetic_data = self.generate_synthetic_data(feature_dim)

        # Combine real and synthetic data
        training_data = np.vstack([features, synthetic_data])

        # Scale the data
        training_data_scaled = self.scaler.fit_transform(training_data)

        # Initialize and fit a lightweight model
        self.model = IsolationForest(
            n_estimators=self.n_estimators,  # Reduced number of trees
            max_samples='auto',
            contamination=self.contamination,
            random_state=42,
            n_jobs=1  # Single CPU core
        )

        logger.info(f"Fitting model with {len(training_data_scaled)} samples...")
        self.model.fit(training_data_scaled)

    def predict(self, features: np.ndarray) -> tuple:
        """Make predictions using the fitted model"""
        features_scaled = self.scaler.transform(features)

        try:
            score = -self.model.score_samples(features_scaled)[0]
            pred = self.model.predict(features_scaled)[0]

            return float(score), int(pred)
        except Exception as e:
            logger.error(f"Error in prediction: {str(e)}")
            return None, None


def detect_anomaly(processed_data: Dict) -> Dict:
    """Detect anomalies in processed sensor data"""
    try:
        # Initialize detector with lightweight configuration
        detector = AnomalyDetector(n_estimators=100)  # Reduced number of trees

        # Extract minimal feature set
        features = np.array(detector.feature_extractor.extract_essential_features(processed_data))
        features = features.reshape(1, -1)

        # Fit model and get predictions
        detector.fit_model(features)
        anomaly_score, prediction = detector.predict(features)

        # Determine anomaly status
        is_anomaly = bool(prediction == -1) if prediction is not None else None

        return {
            "is_anomaly": is_anomaly,
            "anomaly_score": anomaly_score if anomaly_score is not None else 1.0,
            "detection_timestamp": datetime.now().isoformat(),
            "device_id": processed_data['device_id'],
            "window_start": processed_data['start_time'],
            "window_end": processed_data['end_time'],
            "environmental_stability": processed_data['environmental_stability_index'],
            "feature_analysis": {
                "feature_count": len(features[0]),
                "non_zero_features": int(np.sum(features != 0)),
                "feature_mean": float(np.mean(features)),
                "feature_std": float(np.std(features))
            }
        }

    except Exception as e:
        logger.error(f"Error in anomaly detection: {str(e)}")
        raise


async def read_json(reader):
    """Read JSON data from the stream"""
    buffer = b""
    while True:
        chunk = await reader.read(4096)
        if not chunk:
            raise ValueError("Connection closed before receiving a valid JSON object.")
        buffer += chunk
        try:
            return json.loads(buffer.decode())
        except json.JSONDecodeError:
            continue


async def handle_client(reader, writer):
    """Handle incoming client connections"""
    try:
        start_time = time.perf_counter()
        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        # Process each window
        results = []
        for processed_window in input_data['data']:
            logger.debug(f"Processing window for device {processed_window['device_id']}")
            anomaly_result = detect_anomaly(processed_window)
            results.append(anomaly_result)

        output = {
            "data": results,
            "total_task_time": input_data['total_task_time'] + time.perf_counter() - start_time
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info(f"Processed {len(results)} windows")

    except Exception as e:
        logger.error(f"An error occurred: {str(e)}")
        logger.error(traceback.format_exc())
        error_msg = json.dumps({"error": f"An internal error occurred: {str(e)}"})
        writer.write(error_msg.encode())
    finally:
        await writer.drain()
        writer.close()
        logger.info("Connection closed")


async def main():
    """Main server function"""
    server = await asyncio.start_server(handle_client, '0.0.0.0', 12345)

    addr = server.sockets[0].getsockname()
    logger.info(f'Serving on {addr}')

    async with server:
        await server.serve_forever()


if __name__ == "__main__":
    asyncio.run(main())
