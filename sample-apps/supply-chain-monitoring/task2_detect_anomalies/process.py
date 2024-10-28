import asyncio
import json
import logging
import traceback
from datetime import datetime
from typing import Dict

import joblib
import numpy as np

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

# Load the pre-trained model and scaler
try:
    model = joblib.load('models/anomaly_detector.joblib')
    scaler = joblib.load('models/scaler.joblib')
    logger.info("Successfully loaded pre-trained anomaly detection model")
except Exception as e:
    logger.error(f"Failed to load model: {str(e)}")
    raise


def detect_anomaly(window_data: Dict) -> Dict:
    """Detect anomalies in a window of sensor readings"""
    try:
        # Extract features
        features = np.array([
            window_data['temp_deviation'],
            window_data['humidity_deviation'],
            window_data['window_stats']['temperature']['mean'],
            window_data['window_stats']['humidity']['mean'],
            window_data['window_stats']['temperature']['max'],
            window_data['window_stats']['humidity']['max']
        ]).reshape(1, -1)

        # Scale features
        features_scaled = scaler.transform(features)

        # Get anomaly score (-1 for anomaly, 1 for normal)
        prediction = model.predict(features_scaled)[0]

        # Get anomaly score (higher means more anomalous)
        anomaly_score = -model.score_samples(features_scaled)[0]

        # Convert numpy types to Python native types
        return {
            "is_anomaly": bool(prediction == -1),  # Convert np.bool_ to Python bool
            "anomaly_score": float(anomaly_score),  # Already converting to float
            "detection_timestamp": datetime.now().isoformat(),
            "device_id": window_data['device_id'],
            "window_start": window_data['start_time'],
            "window_end": window_data['end_time'],
            "deviations": {
                "temperature": float(window_data['temp_deviation']),  # Convert any numpy types
                "humidity": float(window_data['humidity_deviation'])
            },
            "stats": {
                key: {
                    stat: float(value) if isinstance(value, (np.floating, np.integer)) else value
                    for stat, value in stats.items()
                }
                for key, stats in window_data['window_stats'].items()
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
        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        results = []
        for i, window in enumerate(input_data['data']):
            logger.debug(f"Processing window {i + 1}/{len(input_data['data'])}")
            anomaly_result = detect_anomaly(window)
            if anomaly_result is not None:
                results.append(anomaly_result)

        output = {
            "data": results
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info(f"Processed request successfully, detected {sum(1 for r in results if r['is_anomaly'])} anomalies")

    except ValueError as ve:
        logger.error(str(ve))
        error_msg = json.dumps({"error": str(ve)})
        writer.write(error_msg.encode())
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
