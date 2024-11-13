import asyncio
import json
import logging
import time
import traceback
from datetime import datetime
from typing import Dict, List
import numpy as np
from sklearn.ensemble import IsolationForest
from sklearn.svm import OneClassSVM
from sklearn.preprocessing import StandardScaler
import warnings

warnings.filterwarnings('ignore')

logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class FeatureExtractor:
    def __init__(self):
        self.n_iterations = 1000  # Match task 1's iteration count

    def extract_wavelet_features(self, analysis_data: Dict) -> List[float]:
        """Extract features from wavelet analysis"""
        features = []

        # Process each wavelet family's features
        for key, value in analysis_data.items():
            if isinstance(value, dict) and all(k in value for k in ['mean', 'std', 'energy', 'entropy']):
                features.extend([
                    value['mean'],
                    value['std'],
                    value['energy'],
                    value['entropy']
                ])

        return features

    def extract_frequency_features(self, analysis_data: Dict) -> List[float]:
        """Extract features from frequency band analysis"""
        features = []

        # Process frequency band features
        for i in range(self.n_iterations):
            band_key = f'freq_band_{i}'
            if band_key in analysis_data:
                band_data = analysis_data[band_key]
                features.extend([
                    band_data['power'],
                    band_data['mean'],
                    band_data['std']
                ])

        return features

    def extract_segment_features(self, analysis_data: Dict) -> List[float]:
        """Extract features from time-domain segment analysis"""
        features = []

        # Process segment features
        for i in range(self.n_iterations):
            segment_key = f'segment_{i}'
            if segment_key in analysis_data:
                segment_data = analysis_data[segment_key]
                features.extend([
                    segment_data['mean'],
                    segment_data['std'],
                    segment_data.get('skew', 0),
                    segment_data.get('kurtosis', 0),
                    segment_data['range']
                ])

        return features

    def extract_correlation_features(self, correlation_data: Dict) -> List[float]:
        """Extract features from correlation analysis"""
        features = []

        # Extract baseline correlation features
        if 'baseline' in correlation_data:
            baseline = correlation_data['baseline']
            features.extend([
                baseline['max_correlation'],
                baseline['lag_at_max'],
                baseline['mean_correlation']
            ])

        # Extract iteration-based correlation features
        for i in range(self.n_iterations):
            # Find matching iteration keys (they contain 'iteration_{i}')
            for key, value in correlation_data.items():
                if f'iteration_{i}' in key:
                    features.extend([
                        value['max_correlation'],
                        value['lag_at_max'],
                        value['mean_correlation']
                    ])
                    break

        return features


class AnomalyDetector:
    def __init__(self, n_estimators=1000, contamination=0.1):
        self.feature_extractor = FeatureExtractor()
        self.scaler = StandardScaler()
        self.n_estimators = n_estimators
        self.contamination = contamination
        self.models = {}  # Will be initialized during fit

    def generate_synthetic_data(self, feature_dim: int, n_samples: int = 10000) -> np.ndarray:
        """Generate synthetic data for model fitting"""
        # Generate normal distribution for each feature
        synthetic_data = np.random.normal(0, 1, (n_samples, feature_dim))

        # Add some structured patterns
        for i in range(0, feature_dim, 10):  # Add correlations every 10 features
            if i + 1 < feature_dim:
                synthetic_data[:, i + 1] = synthetic_data[:, i] * 0.7 + np.random.normal(0, 0.3, n_samples)

        # Add some outliers
        n_outliers = int(n_samples * self.contamination)
        outlier_indices = np.random.choice(n_samples, n_outliers, replace=False)
        synthetic_data[outlier_indices] *= np.random.uniform(5, 10, (n_outliers, feature_dim))

        return synthetic_data

    def fit_models(self, features: np.ndarray):
        """Initialize and fit models with either real or synthetic data"""
        feature_dim = features.shape[1]

        # Generate synthetic training data
        synthetic_data = self.generate_synthetic_data(feature_dim)

        # Combine real and synthetic data
        training_data = np.vstack([features, synthetic_data])

        # Scale the combined data
        training_data_scaled = self.scaler.fit_transform(training_data)

        # Initialize and fit models
        self.models = {
            'isolation_forest': IsolationForest(
                n_estimators=self.n_estimators,
                contamination=self.contamination,
                random_state=42,
                n_jobs=-1
            ),
            'one_class_svm': OneClassSVM(
                kernel='rbf',
                nu=self.contamination,
                gamma='scale'
            )
        }

        # Fit each model
        for name, model in self.models.items():
            logger.info(f"Fitting {name} with {len(training_data_scaled)} samples...")
            model.fit(training_data_scaled)

    def extract_features(self, processed_data: Dict) -> np.ndarray:
        """Extract comprehensive feature set from processed data"""
        features = []

        # Extract stability index
        features.append(processed_data['environmental_stability_index'])

        # Extract temperature analysis features
        temp_analysis = processed_data['temperature_analysis']
        features.extend(self.feature_extractor.extract_wavelet_features(temp_analysis))
        features.extend(self.feature_extractor.extract_frequency_features(temp_analysis))
        features.extend(self.feature_extractor.extract_segment_features(temp_analysis))

        # Extract humidity analysis features
        humid_analysis = processed_data['humidity_analysis']
        features.extend(self.feature_extractor.extract_wavelet_features(humid_analysis))
        features.extend(self.feature_extractor.extract_frequency_features(humid_analysis))
        features.extend(self.feature_extractor.extract_segment_features(humid_analysis))

        # Extract correlation analysis features
        correlation_analysis = processed_data['correlation_analysis']
        features.extend(self.feature_extractor.extract_correlation_features(correlation_analysis))

        # Replace any potential NaN or infinite values
        features = np.array(features)
        features[~np.isfinite(features)] = 0

        return features

    def compute_ensemble_scores(self, features: np.ndarray) -> Dict:
        """Compute anomaly scores using multiple models"""
        scores = {}
        predictions = {}

        features_reshaped = features.reshape(1, -1)
        features_scaled = self.scaler.transform(features_reshaped)

        for name, model in self.models.items():
            try:
                if hasattr(model, 'score_samples'):
                    score = -model.score_samples(features_scaled)[0]
                else:
                    score = -model.decision_function(features_scaled)[0]

                pred = model.predict(features_scaled)[0]
                scores[name] = float(score)
                predictions[name] = int(pred)
            except Exception as e:
                logger.error(f"Error getting prediction from {name}: {str(e)}")
                scores[name] = None
                predictions[name] = None

        return scores, predictions


def detect_anomaly(processed_data: Dict) -> Dict:
    """Detect anomalies in processed sensor data"""
    try:
        # Initialize detector
        detector = AnomalyDetector()

        # Extract features
        features = detector.extract_features(processed_data)

        # Fit models with synthetic data
        detector.fit_models(features.reshape(1, -1))

        # Get ensemble predictions
        anomaly_scores, model_predictions = detector.compute_ensemble_scores(features)

        # Calculate aggregate anomaly score
        valid_scores = [score for score in anomaly_scores.values() if score is not None]
        aggregate_score = np.mean(valid_scores) if valid_scores else 1.0

        # Determine if anomaly based on majority voting
        valid_predictions = [pred for pred in model_predictions.values() if pred is not None]
        is_anomaly = sum(1 for p in valid_predictions if p == -1) > len(valid_predictions) / 2

        return {
            "is_anomaly": bool(is_anomaly),
            "anomaly_score": float(aggregate_score),
            "model_scores": anomaly_scores,
            "model_predictions": model_predictions,
            "detection_timestamp": datetime.now().isoformat(),
            "device_id": processed_data['device_id'],
            "window_start": processed_data['start_time'],
            "window_end": processed_data['end_time'],
            "environmental_stability": processed_data['environmental_stability_index'],
            "feature_analysis": {
                "feature_count": len(features),
                "non_zero_features": int(np.sum(features != 0)),
                "feature_statistics": {
                    "mean": float(np.mean(features)),
                    "std": float(np.std(features)),
                    "min": float(np.min(features)),
                    "max": float(np.max(features))
                }
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
