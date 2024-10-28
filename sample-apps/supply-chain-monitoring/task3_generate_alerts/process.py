import asyncio
import json
import logging
import traceback
from datetime import datetime
from enum import Enum
from typing import Dict

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AlertSeverity(Enum):
    CRITICAL = "CRITICAL"
    WARNING = "WARNING"
    INFO = "INFO"


def determine_severity(anomaly_data: Dict) -> AlertSeverity:
    """Determine alert severity based on anomaly data"""

    # Get the deviations
    temp_dev = anomaly_data['deviations']['temperature']
    humid_dev = anomaly_data['deviations']['humidity']
    anomaly_score = anomaly_data['anomaly_score']

    # Critical conditions
    if (temp_dev > 4.0 or
            humid_dev > 12.0 or
            anomaly_score > 0.9):
        return AlertSeverity.CRITICAL

    # Warning conditions
    elif (temp_dev > 2.5 or
          humid_dev > 8.0 or
          anomaly_score > 0.7):
        return AlertSeverity.WARNING

    # Information only
    else:
        return AlertSeverity.INFO


def generate_alert_message(anomaly_data: Dict, severity: AlertSeverity) -> str:
    """Generate human-readable alert message"""
    device = anomaly_data['device_id']
    temp_stats = anomaly_data['stats']['temperature']
    humid_stats = anomaly_data['stats']['humidity']

    message = f"Cold Chain Alert - {severity.value}\n"
    message += f"Device: {device}\n"
    message += f"Time Window: {anomaly_data['window_start']} to {anomaly_data['window_end']}\n\n"

    message += "Conditions:\n"
    message += f"- Temperature: mean={temp_stats['mean']}°C (min={temp_stats['min']}, max={temp_stats['max']})\n"
    message += f"- Humidity: mean={humid_stats['mean']}% (min={humid_stats['min']}, max={humid_stats['max']})\n"
    message += f"- Anomaly Score: {anomaly_data['anomaly_score']:.3f}\n"

    if severity == AlertSeverity.CRITICAL:
        message += "\nIMMEDIATE ACTION REQUIRED: Critical cold chain violation detected!"
    elif severity == AlertSeverity.WARNING:
        message += "\nATTENTION: Cold chain parameters approaching critical levels."

    return message


def generate_alert(anomaly_data: Dict) -> Dict:
    """Generate alert from anomaly detection results"""
    try:
        # Only generate alerts for actual anomalies
        if not anomaly_data['is_anomaly']:
            return None

        # Determine severity
        severity = determine_severity(anomaly_data)

        return {
            "alert_id": f"alert_{datetime.now().strftime('%Y%m%d_%H%M%S')}_{anomaly_data['device_id']}",
            "timestamp": datetime.now().isoformat(),
            "severity": severity.value,
            "device_id": anomaly_data['device_id'],
            "window_info": {
                "start": anomaly_data['window_start'],
                "end": anomaly_data['window_end']
            },
            "conditions": {
                "temperature": anomaly_data['stats']['temperature'],
                "humidity": anomaly_data['stats']['humidity'],
                "deviations": anomaly_data['deviations'],
                "anomaly_score": anomaly_data['anomaly_score']
            },
            "message": generate_alert_message(anomaly_data, severity)
        }
    except Exception as e:
        logger.error(f"Error generating alert: {str(e)}")
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
        for i, anomaly in enumerate(input_data['data']):
            logger.debug(f"Processing anomaly {i + 1}/{len(input_data['data'])}")
            alert = generate_alert(anomaly)
            if alert is not None:
                results.append(alert)
                if alert['severity'] == 'CRITICAL':
                    logger.warning(f"Critical alert generated for device {alert['device_id']}")

        output = {
            "data": results
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info(f"Generated {len(results)} alerts")

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
