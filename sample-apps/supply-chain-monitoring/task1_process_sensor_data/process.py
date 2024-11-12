import json
import numpy as np
import asyncio
import logging
import time
import traceback
from datetime import datetime
from typing import List, Dict

# Set up logging
logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


def process_readings_batch(readings: List[Dict]) -> Dict:
    """Process a batch of 10 readings representing a 10-second window"""
    try:
        # Extract values
        temperatures = [float(r['temperature']) for r in readings]
        humidities = [float(r['humidity']) for r in readings]
        timestamps = [r['timestamp'] for r in readings]
        device_id = readings[0]['device_id']  # Assume same device for batch

        # Calculate averages
        avg_temp = np.mean(temperatures)
        avg_humidity = np.mean(humidities)

        # Calculate deviations from ideal conditions
        ideal_temp = 5.0  # Celsius
        ideal_humidity = 60.0  # Percentage

        temp_deviation = abs(avg_temp - ideal_temp)
        humidity_deviation = abs(avg_humidity - ideal_humidity)

        return {
            "temp_deviation": round(temp_deviation, 2),
            "humidity_deviation": round(humidity_deviation, 2),
            "processed_timestamp": datetime.now().isoformat(),
            "device_id": device_id,
            "start_time": min(timestamps),  # First reading in batch
            "end_time": max(timestamps),  # Last reading in batch
            "readings_count": len(readings),
            "window_stats": {
                "temperature": {
                    "mean": round(avg_temp, 2),
                    "min": round(min(temperatures), 2),
                    "max": round(max(temperatures), 2)
                },
                "humidity": {
                    "mean": round(avg_humidity, 2),
                    "min": round(min(humidities), 2),
                    "max": round(max(humidities), 2)
                }
            }
        }
    except Exception as e:
        logger.error(f"Error processing batch: {str(e)}")
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
        start = time.perf_counter()

        logger.info("New client connected")
        input_data = await read_json(reader)

        logger.debug(f"Received JSON data of length: {len(json.dumps(input_data))} bytes")

        if 'data' not in input_data or not isinstance(input_data['data'], list):
            raise ValueError("'data' field (as a list) is required in the JSON input")

        # Validate batch size
        if len(input_data['data']) != 10:
            logger.warning(f"Expected batch size of 10, got {len(input_data['data'])} readings")

        # Process the batch
        processed_data = process_readings_batch(input_data['data'])

        output = {
            "data": [processed_data],  # Single window output
            "task1_time": time.perf_counter() - start
        }
        output_json = json.dumps(output)
        writer.write(output_json.encode())
        await writer.drain()

        logger.info("Processed batch successfully")

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
